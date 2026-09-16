/**
 * MySQL 连接池与自动建库建表。
 * 首次连接不指定 database，确保数据库不存在时也能自动创建。
 */
import mysql from 'mysql2/promise';
import { config } from './config.js';

/** 管理连接（可跨 database），用于自动 CREATE DATABASE */
async function ensureDatabase() {
  const admin = await mysql.createConnection({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
  });
  try {
    await admin.query(
      `CREATE DATABASE IF NOT EXISTS \`${config.db.database}\`
       CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`,
    );
  } finally {
    await admin.end();
  }
}

/** 简易幂等 schema 初始化（与 sql/schema.sql 内容对应，CAS v3） */
async function ensureSchema(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS files (
      id                 VARCHAR(64)  NOT NULL,
      file_name          VARCHAR(512) NOT NULL,
      file_size          BIGINT UNSIGNED NOT NULL,
      chunk_size         INT UNSIGNED NOT NULL,
      total_chunks       INT UNSIGNED NOT NULL,
      file_hash          CHAR(64)     NULL,
      merged_hash        CHAR(64)     NULL,
      status             ENUM('uploading','merging','completed','failed') NOT NULL DEFAULT 'uploading',
      merged_path        VARCHAR(1024) NULL,
      merge_owner        VARCHAR(128) NULL COMMENT '当前执行合并的实例标识（崩溃恢复用）',
      merge_lease_until  DATETIME(3)  NULL COMMENT '合并租约到期时间；过期后其它实例可接管',
      created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
      PRIMARY KEY (id),
      KEY idx_status (status),
      KEY idx_file_hash_status (file_hash, status),
      KEY idx_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS cas_chunks (
      chunk_hash     CHAR(64) NOT NULL,
      chunk_size     BIGINT UNSIGNED NOT NULL,
      storage_path   VARCHAR(1024) NOT NULL,
      ref_count      INT UNSIGNED NOT NULL DEFAULT 0,
      created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
      PRIMARY KEY (chunk_hash),
      KEY idx_ref_count (ref_count)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS file_chunks (
      id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      file_id      VARCHAR(64) NOT NULL,
      chunk_index  INT UNSIGNED NOT NULL,
      chunk_hash   CHAR(64) NOT NULL,
      status       ENUM('uploaded','verified') NOT NULL DEFAULT 'uploaded',
      created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
      PRIMARY KEY (id),
      UNIQUE KEY uk_file_chunk (file_id, chunk_index),
      KEY idx_chunk_hash (chunk_hash),
      KEY idx_file_status (file_id, status),
      CONSTRAINT fk_fc_file FOREIGN KEY (file_id) REFERENCES files (id)
        ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS merged_blobs (
      merged_hash   CHAR(64) NOT NULL,
      file_size     BIGINT UNSIGNED NOT NULL,
      storage_path  VARCHAR(1024) NOT NULL,
      ref_count     INT UNSIGNED NOT NULL DEFAULT 0,
      created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      updated_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
      PRIMARY KEY (merged_hash),
      KEY idx_ref_count (ref_count)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  // ---- 增量迁移（旧库升级，CREATE TABLE IF NOT EXISTS 不会改既有列） ----
  // 迭代二起 file_hash 允许为 NULL（分阶段任务：init 时无聚合哈希，/hash 后补报）。
  // 旧 schema 为 NOT NULL，真 MySQL 下分阶段 init 会直接 ER_BAD_NULL_ERROR(500)。
  await migrateNullableColumn(pool, 'files', 'file_hash', 'CHAR(64) NULL');
  // 合并租约列（崩溃后其它实例可接管卡死的 merging）。旧库没有则 ADD COLUMN。
  await ensureColumn(pool, 'files', 'merge_owner', 'VARCHAR(128) NULL');
  await ensureColumn(pool, 'files', 'merge_lease_until', 'DATETIME(3) NULL');
}

/** 幂等新增列：列不存在时 ADD COLUMN */
async function ensureColumn(pool, table, column, ddl) {
  const [cols] = await pool.query(
    `SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [config.db.database, table, column],
  );
  if (cols.length === 0) {
    await pool.query(`ALTER TABLE \`${table}\` ADD COLUMN \`${column}\` ${ddl}`);
  }
}

/**
 * 幂等列迁移：仅当列的 IS_NULLABLE=NO 时才 ALTER。
 * information_schema 查询在内存 mock 中同样实现，保证测试与真库语义一致。
 */
async function migrateNullableColumn(pool, table, column, targetDef) {
  const [cols] = await pool.query(
    `SELECT IS_NULLABLE AS isNullable
       FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [config.db.database, table, column],
  );
  if (cols.length > 0 && cols[0].isNullable === 'NO') {
    await pool.query(`ALTER TABLE \`${table}\` MODIFY \`${column}\` ${targetDef}`);
  }
}

let pool;

export async function initDb() {
  await ensureDatabase();
  pool = mysql.createPool({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    // BIGINT 以字符串返回，避免精度问题；业务里统一按字符串处理 file_size
    bigNumberStrings: true,
    dateStrings: true,
  });
  await ensureSchema(pool);
  return pool;
}

export function getPool() {
  if (!pool) throw new Error('DB not initialized. Call initDb() first.');
  return pool;
}
