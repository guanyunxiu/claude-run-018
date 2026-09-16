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
