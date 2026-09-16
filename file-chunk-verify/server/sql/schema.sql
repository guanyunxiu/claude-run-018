-- ============================================================
-- 分片上传与校验系统元数据表
-- 后端启动时会自动执行等效的 CREATE DATABASE / CREATE TABLE，
-- 本文件用于手动初始化或 docker compose 首次导入。
-- ============================================================

CREATE TABLE IF NOT EXISTS files (
  id                 VARCHAR(64)  NOT NULL COMMENT '文件ID: sha256(name:size:lastModified:chunkSize) 的 hex',
  file_name          VARCHAR(512) NOT NULL COMMENT '原始文件名',
  file_size          BIGINT UNSIGNED NOT NULL COMMENT '文件总字节数',
  chunk_size         INT UNSIGNED NOT NULL COMMENT '固定分片大小（最后一片可更小）',
  total_chunks       INT UNSIGNED NOT NULL COMMENT '分片总数',
  file_hash          CHAR(64)     NOT NULL COMMENT '聚合哈希: sha256(concat(各分片sha256的hex))',
  merged_hash        CHAR(64)     NULL COMMENT '合并后完整文件的 sha256（complete 后回填）',
  status             ENUM('uploading','merging','completed','failed') NOT NULL DEFAULT 'uploading'
                     COMMENT 'uploading=上传中, merging=校验/合并中, completed=校验通过, failed=失败',
  merged_path        VARCHAR(1024) NULL COMMENT '合并文件存储相对路径',
  created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_status (status),
  KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='文件级上传任务元数据';

CREATE TABLE IF NOT EXISTS chunks (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  file_id         VARCHAR(64) NOT NULL COMMENT '所属文件ID',
  chunk_index     INT UNSIGNED NOT NULL COMMENT '分片序号，从 0 开始连续',
  chunk_hash      CHAR(64) NOT NULL COMMENT '分片内容 sha256 hex',
  chunk_size      BIGINT UNSIGNED NOT NULL COMMENT '分片实际字节数（最后一片可能更小）',
  storage_path    VARCHAR(1024) NOT NULL COMMENT '分片文件相对存储路径',
  status          ENUM('uploaded','verified') NOT NULL DEFAULT 'uploaded'
                  COMMENT 'uploaded=已上传落盘, verified=整文件聚合校验通过',
  created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_file_chunk (file_id, chunk_index),
  KEY idx_file_status (file_id, status),
  CONSTRAINT fk_chunks_file FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='分片元数据（序号/哈希/大小/落盘路径/状态）';
