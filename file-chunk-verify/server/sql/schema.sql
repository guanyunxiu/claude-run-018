-- ============================================================
-- 分片上传与校验系统 · 内容寻址（CAS）v3
--   - cas_chunks  : 全局物理分片，按 chunkHash 内容寻址，带引用计数
--   - file_chunks : files ↔ cas_chunks 的多对多关联（每文件每序号一条）
--   - merged_blobs: 合并产物同样内容寻址 + 引用计数（秒传可零拷贝共享）
-- 后端启动时会自动执行等效 CREATE TABLE，本文件用于手动初始化。
-- ============================================================

CREATE TABLE IF NOT EXISTS files (
  id                 VARCHAR(64)  NOT NULL COMMENT '文件ID: sha256(name:size:lastModified:chunkSize)',
  file_name          VARCHAR(512) NOT NULL COMMENT '原始文件名',
  file_size          BIGINT UNSIGNED NOT NULL COMMENT '文件总字节数',
  chunk_size         INT UNSIGNED NOT NULL COMMENT '固定分片大小（最后一片可更小）',
  total_chunks       INT UNSIGNED NOT NULL COMMENT '分片总数',
  file_hash          CHAR(64)     NULL COMMENT '聚合哈希（init 可空，经 /hash 或带清单 init 锁定）',
  merged_hash        CHAR(64)     NULL COMMENT '合并产物（完整文件）sha256，= merged_blobs 主键',
  status             ENUM('uploading','merging','completed','failed') NOT NULL DEFAULT 'uploading',
  merged_path        VARCHAR(1024) NULL COMMENT '合并产物相对路径（冗余，指向 merged_blobs.storage_path）',
  created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_status (status),
  KEY idx_file_hash_status (file_hash, status),
  KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='文件级上传任务元数据';

-- 全局内容寻址分片库：同一 chunkHash 物理只存一份
CREATE TABLE IF NOT EXISTS cas_chunks (
  chunk_hash     CHAR(64) NOT NULL COMMENT '分片内容 sha256（内容寻址主键）',
  chunk_size     BIGINT UNSIGNED NOT NULL COMMENT '分片字节数',
  storage_path   VARCHAR(1024) NOT NULL COMMENT '物理相对路径 cas/<xx>/<hash>.part',
  ref_count      INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '引用计数：被多少个 file_chunks 引用',
  created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (chunk_hash),
  KEY idx_ref_count (ref_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='内容寻址物理分片库（多文件共享，引用计数）';

-- 文件 ↔ 分片 多对多关联
CREATE TABLE IF NOT EXISTS file_chunks (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  file_id      VARCHAR(64) NOT NULL,
  chunk_index  INT UNSIGNED NOT NULL COMMENT '分片序号（文件内从 0 连续）',
  chunk_hash   CHAR(64) NOT NULL COMMENT '指向 cas_chunks.chunk_hash',
  status       ENUM('uploaded','verified') NOT NULL DEFAULT 'uploaded'
               COMMENT 'uploaded=已落盘, verified=整文件聚合校验通过',
  created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_file_chunk (file_id, chunk_index),
  KEY idx_chunk_hash (chunk_hash),
  KEY idx_file_status (file_id, status),
  CONSTRAINT fk_fc_file FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='文件分片关联（files↔cas_chunks 多对多）';

-- 合并产物内容寻址库（秒传命中时多个 files 共享同一合并文件）
CREATE TABLE IF NOT EXISTS merged_blobs (
  merged_hash   CHAR(64) NOT NULL COMMENT '完整文件 sha256（内容寻址主键）',
  file_size     BIGINT UNSIGNED NOT NULL,
  storage_path  VARCHAR(1024) NOT NULL COMMENT '物理相对路径 merged/<xx>/<hash>.bin',
  ref_count     INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '引用计数：多少个 files 指向它',
  created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (merged_hash),
  KEY idx_ref_count (ref_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='内容寻址合并产物库（秒传共享，引用计数）';
