/**
 * 文件分片业务路由：
 *  POST   /init                         注册/恢复文件任务（幂等）
 *  GET    /:fileId/chunks               已上传分片列表（断点续传依据）
 *  GET    /:fileId/status               任务状态
 *  POST   /:fileId/chunks/:index        上传单个分片二进制（幂等，落盘前校验哈希）
 *  POST   /:fileId/complete             分片齐全 → 重算分片哈希/聚合校验 → 合并
 */
import express from 'express';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { getPool } from '../db.js';
import { config } from '../config.js';
import { sha256Hex, aggregateHashHex } from '../hash.js';
import {
  writeChunk,
  mergeChunks,
  chunkRelPath,
  safeFileId,
} from '../storage.js';

const router = express.Router();

const HASH_RE = /^[a-f0-9]{64}$/i;
const ID_RE = /^[a-f0-9]{64}$/i;

function apiError(status, code, message, details) {
  const err = new Error(message);
  err.status = status;
  err.code = code;
  if (details !== undefined) err.details = details;
  return err;
}

const asyncHandler = (fn) => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch(next);

function isPosInt(v) {
  return typeof v === 'number' && Number.isInteger(v) && v > 0;
}
function isNonNegInt(v) {
  return typeof v === 'number' && Number.isInteger(v) && v >= 0;
}
/** 接收 number 或十进制字符串，返回 BigInt */
function toBigInt(v) {
  if (typeof v === 'number' && Number.isSafeInteger(v)) return BigInt(v);
  if (typeof v === 'string' && /^\d+$/.test(v)) return BigInt(v);
  return null;
}

function chunkRowMapper(r) {
  return {
    index: r.chunk_index,
    hash: r.chunk_hash,
    size: Number(r.chunk_size),
    status: r.status,
  };
}

/* ------------------------------------------------------------------ */
/* POST /init                                                          */
/* ------------------------------------------------------------------ */
router.post(
  '/init',
  asyncHandler(async (req, res) => {
    const b = req.body ?? {};
    const fileName = typeof b.fileName === 'string' ? b.fileName.trim() : '';
    const fileSize = toBigInt(b.fileSize);
    const chunkSize = b.chunkSize;
    const totalChunks = b.totalChunks;
    const fileHash = typeof b.fileHash === 'string' ? b.fileHash.toLowerCase() : '';
    const fileId = typeof b.fileId === 'string' ? b.fileId.toLowerCase() : '';

    if (!fileName || fileName.length > 512)
      throw apiError(400, 'VALIDATION_ERROR', 'fileName 非法（必填，最长 512）');
    if (fileSize === null)
      throw apiError(400, 'VALIDATION_ERROR', 'fileSize 必须为非负整数');
    if (!isPosInt(chunkSize))
      throw apiError(400, 'VALIDATION_ERROR', 'chunkSize 必须为正整数（字节）');
    if (!isNonNegInt(totalChunks))
      throw apiError(400, 'VALIDATION_ERROR', 'totalChunks 必须为非负整数');
    if (!HASH_RE.test(fileHash))
      throw apiError(400, 'VALIDATION_ERROR', 'fileHash 必须为 64 位 sha256 hex');
    if (!ID_RE.test(fileId))
      throw apiError(400, 'VALIDATION_ERROR', 'fileId 必须为 64 位 hex');

    const expectedChunks =
      fileSize === 0n ? 0 : Number((fileSize + BigInt(chunkSize) - 1n) / BigInt(chunkSize));
    if (expectedChunks !== totalChunks) {
      throw apiError(
        400,
        'VALIDATION_ERROR',
        `totalChunks 与 fileSize/chunkSize 不一致，应为 ${expectedChunks}`,
        { expectedChunks },
      );
    }

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [rows] = await conn.query('SELECT * FROM files WHERE id = ? FOR UPDATE', [
        fileId,
      ]);

      let fileRow;
      let resumed;
      if (rows.length > 0) {
        fileRow = rows[0];
        resumed = true;
        const mismatch =
          fileRow.file_name !== fileName ||
          BigInt(fileRow.file_size) !== fileSize ||
          fileRow.chunk_size !== chunkSize ||
          fileRow.total_chunks !== totalChunks ||
          fileRow.file_hash !== fileHash;
        if (mismatch) {
          throw apiError(
            409,
            'FILE_PARAM_MISMATCH',
            '相同 fileId 的文件参数与服务端记录不一致（文件名/大小/分片大小/聚合哈希）',
          );
        }
      } else {
        resumed = false;
        await conn.query(
          `INSERT INTO files
             (id, file_name, file_size, chunk_size, total_chunks, file_hash, status)
           VALUES (?, ?, ?, ?, ?, ?, 'uploading')`,
          [fileId, fileName, fileSize.toString(), chunkSize, totalChunks, fileHash],
        );
        fileRow = {
          id: fileId,
          file_name: fileName,
          file_size: fileSize.toString(),
          chunk_size: chunkSize,
          total_chunks: totalChunks,
          file_hash: fileHash,
          status: 'uploading',
        };
      }

      const [chunkRows] = await conn.query(
        'SELECT chunk_index, chunk_hash, chunk_size, status FROM chunks WHERE file_id = ? ORDER BY chunk_index',
        [fileId],
      );
      await conn.commit();

      res.status(resumed ? 200 : 201).json({
        resumed,
        file: {
          fileId: fileRow.id,
          fileName: fileRow.file_name,
          fileSize: Number(fileRow.file_size),
          chunkSize: fileRow.chunk_size,
          totalChunks: fileRow.total_chunks,
          fileHash: fileRow.file_hash,
          status: fileRow.status,
        },
        uploadedChunks: chunkRows.map(chunkRowMapper),
      });
    } catch (err) {
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
  }),
);

/* ------------------------------------------------------------------ */
/* GET /:fileId/chunks  —— 断点续传：查询已上传分片                     */
/* ------------------------------------------------------------------ */
router.get(
  '/:fileId/chunks',
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    const pool = getPool();
    const [files] = await pool.query('SELECT id FROM files WHERE id = ?', [fileId]);
    if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '文件任务不存在');

    const [chunkRows] = await pool.query(
      'SELECT chunk_index, chunk_hash, chunk_size, status FROM chunks WHERE file_id = ? ORDER BY chunk_index',
      [fileId],
    );
    res.json({
      fileId,
      uploadedCount: chunkRows.length,
      chunks: chunkRows.map(chunkRowMapper),
    });
  }),
);

/* ------------------------------------------------------------------ */
/* GET /:fileId/status                                                 */
/* ------------------------------------------------------------------ */
router.get(
  '/:fileId/status',
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    const pool = getPool();
    const [files] = await pool.query('SELECT * FROM files WHERE id = ?', [fileId]);
    if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '文件任务不存在');
    const f = files[0];
    const [cnt] = await pool.query(
      'SELECT COUNT(*) AS c FROM chunks WHERE file_id = ?',
      [fileId],
    );
    res.json({
      fileId: f.id,
      fileName: f.file_name,
      fileSize: Number(f.file_size),
      chunkSize: f.chunk_size,
      totalChunks: f.total_chunks,
      uploadedChunks: cnt[0].c,
      status: f.status,
      fileHash: f.file_hash,
      mergedHash: f.merged_hash,
      mergedPath: f.merged_path,
      createdAt: f.created_at,
      updatedAt: f.updated_at,
    });
  }),
);

/* ------------------------------------------------------------------ */
/* POST /:fileId/chunks/:index  —— 上传分片（raw 二进制）              */
/* ------------------------------------------------------------------ */
// 独立的 raw parser：只在本路由生效，limit 可配置
const rawParser = express.raw({ type: () => true, limit: config.chunkLimitBytes });

router.post(
  '/:fileId/chunks/:index',
  rawParser,
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);

    if (!/^\d+$/.test(req.params.index)) {
      throw apiError(400, 'VALIDATION_ERROR', '分片序号必须为非负整数');
    }
    const index = Number(req.params.index);

    const expectedHash = typeof req.query.hash === 'string'
      ? req.query.hash.toLowerCase()
      : '';
    if (!HASH_RE.test(expectedHash)) {
      throw apiError(400, 'VALIDATION_ERROR', '查询参数 hash 必须为 64 位 sha256 hex');
    }

    // 空 body 兜底为 0 字节 Buffer
    const body = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [files] = await conn.query(
        'SELECT * FROM files WHERE id = ? FOR UPDATE',
        [fileId],
      );
      if (files.length === 0) {
        throw apiError(404, 'FILE_NOT_FOUND', '请先调用 /init 注册文件任务');
      }
      const file = files[0];
      if (file.status === 'completed') {
        throw apiError(409, 'FILE_ALREADY_VERIFIED', '文件已完成校验，无需再上传分片');
      }
      if (index < 0 || index >= file.total_chunks) {
        throw apiError(400, 'CHUNK_INDEX_OUT_OF_RANGE', '分片序号超出范围', {
          index,
          totalChunks: file.total_chunks,
        });
      }

      // 期望大小：除最后一片外都等于 chunkSize
      const fileSize = BigInt(file.file_size);
      const chunkSize = BigInt(file.chunk_size);
      const expectedSize =
        index === file.total_chunks - 1
          ? fileSize - BigInt(index) * chunkSize
          : chunkSize;
      if (BigInt(body.length) !== expectedSize) {
        throw apiError(413, 'CHUNK_SIZE_MISMATCH', '分片大小与约定不符', {
          index,
          expectedSize: expectedSize.toString(),
          actualSize: body.length,
        });
      }

      const [existing] = await conn.query(
        'SELECT id, chunk_hash FROM chunks WHERE file_id = ? AND chunk_index = ?',
        [fileId, index],
      );

      // 幂等：同一序号同哈希，直接跳过（断点重传/重试场景）
      if (existing.length > 0 && existing[0].chunk_hash === expectedHash) {
        await conn.commit();
        return res.status(200).json({
          index,
          hash: expectedHash,
          size: body.length,
          skipped: true,
        });
      }

      // 落盘前强校验哈希：坏数据绝不写盘
      const actualHash = sha256Hex(body);
      if (actualHash !== expectedHash) {
        throw apiError(422, 'CHUNK_HASH_MISMATCH', '分片哈希校验失败，拒绝写入', {
          index,
          expectedHash,
          actualHash,
        });
      }

      const relPath = await writeChunk(fileId, index, body);

      await conn.query(
        `INSERT INTO chunks (file_id, chunk_index, chunk_hash, chunk_size, storage_path, status)
         VALUES (?, ?, ?, ?, ?, 'uploaded')
         ON DUPLICATE KEY UPDATE
           chunk_hash = VALUES(chunk_hash),
           chunk_size = VALUES(chunk_size),
           storage_path = VALUES(storage_path),
           status = 'uploaded',
           updated_at = CURRENT_TIMESTAMP(3)`,
        [fileId, index, actualHash, body.length, relPath],
      );
      await conn.commit();

      res.status(201).json({
        index,
        hash: actualHash,
        size: body.length,
        skipped: false,
      });
    } catch (err) {
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
  }),
);

/* ------------------------------------------------------------------ */
/* POST /:fileId/complete  —— 全量聚合校验 + 合并                      */
/* ------------------------------------------------------------------ */
router.post(
  '/:fileId/complete',
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    const pool = getPool();

    const [files] = await pool.query('SELECT * FROM files WHERE id = ?', [fileId]);
    if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '文件任务不存在');
    const file = files[0];

    if (file.status === 'completed') {
      throw apiError(409, 'FILE_ALREADY_VERIFIED', '文件已完成校验');
    }

    await pool.query("UPDATE files SET status = 'merging' WHERE id = ?", [fileId]);

    try {
      const [chunkRows] = await pool.query(
        'SELECT chunk_index, chunk_hash, chunk_size, storage_path FROM chunks WHERE file_id = ? ORDER BY chunk_index',
        [fileId],
      );

      const total = file.total_chunks;
      if (chunkRows.length !== total) {
        await pool.query("UPDATE files SET status = 'uploading' WHERE id = ?", [fileId]);
        throw apiError(409, 'CHUNKS_INCOMPLETE', '分片数量不足，无法完成校验', {
          expected: total,
          actual: chunkRows.length,
        });
      }

      const fileSize = BigInt(file.file_size);
      const chunkSize = BigInt(file.chunk_size);
      const recomputedHashes = [];
      let sumSize = 0n;

      // 逐片从磁盘重读、重算哈希，校验序号连续性与大小
      for (let i = 0; i < total; i += 1) {
        const row = chunkRows[i];
        if (!row || row.chunk_index !== i) {
          await pool.query("UPDATE files SET status = 'uploading' WHERE id = ?", [fileId]);
          throw apiError(409, 'CHUNKS_INCOMPLETE', `缺少分片或序号不连续：index=${i}`, {
            expectedIndex: i,
            actualIndex: row ? row.chunk_index : null,
          });
        }

        const expectedSize =
          i === total - 1 ? fileSize - BigInt(i) * chunkSize : chunkSize;
        if (BigInt(row.chunk_size) !== expectedSize) {
          await pool.query("UPDATE files SET status = 'uploading' WHERE id = ?", [fileId]);
          throw apiError(409, 'CHUNK_SIZE_MISMATCH', '服务端记录的分片大小异常', {
            index: i,
            expectedSize: expectedSize.toString(),
            actualSize: row.chunk_size.toString(),
          });
        }

        const abs = path.join(config.storageDir, row.storage_path);
        let buf;
        try {
          buf = await fsp.readFile(abs);
        } catch {
          await pool.query("UPDATE files SET status = 'uploading' WHERE id = ?", [fileId]);
          throw apiError(409, 'CHUNK_FILE_MISSING', `分片文件在磁盘上缺失：index=${i}`, {
            index: i,
          });
        }

        const h = sha256Hex(buf);
        if (h !== row.chunk_hash) {
          await pool.query("UPDATE files SET status = 'uploading' WHERE id = ?", [fileId]);
          throw apiError(422, 'CHUNK_HASH_MISMATCH', '服务端重算分片哈希与记录不符', {
            index: i,
            storedHash: row.chunk_hash,
            actualHash: h,
          });
        }
        recomputedHashes.push(h);
        sumSize += BigInt(buf.length);
      }

      if (sumSize !== fileSize) {
        await pool.query("UPDATE files SET status = 'uploading' WHERE id = ?", [fileId]);
        throw apiError(422, 'FILE_SIZE_MISMATCH', '分片累计大小与文件总大小不一致', {
          expectedSize: fileSize.toString(),
          actualSize: sumSize.toString(),
        });
      }

      // 聚合哈希：与前端算法一致
      const recomputedAggregate = aggregateHashHex(recomputedHashes);
      if (recomputedAggregate !== file.file_hash) {
        await pool.query("UPDATE files SET status = 'uploading' WHERE id = ?", [fileId]);
        throw apiError(
          422,
          'AGGREGATE_HASH_MISMATCH',
          '聚合哈希与前端上报值不一致，整体完整性校验失败',
          {
            expectedHash: file.file_hash,
            actualHash: recomputedAggregate,
          },
        );
      }

      // 全部通过 → 流式合并，同时产出完整文件 sha256
      const { mergedAbsolutePath, mergedHash, totalBytes } = await mergeChunks(
        fileId,
        total,
        file.file_name,
      );
      const mergedPath = path
        .relative(config.storageDir, mergedAbsolutePath)
        .split(path.sep)
        .join('/');

      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();
        await conn.query(
          "UPDATE chunks SET status = 'verified' WHERE file_id = ?",
          [fileId],
        );
        await conn.query(
          `UPDATE files
             SET status = 'completed', merged_hash = ?, merged_path = ?
           WHERE id = ?`,
          [mergedHash, mergedPath, fileId],
        );
        await conn.commit();
      } catch (err) {
        await conn.rollback();
        throw err;
      } finally {
        conn.release();
      }

      res.json({
        verified: true,
        fileId,
        totalChunks: total,
        fileSize: Number(totalBytes),
        aggregateHash: recomputedAggregate,
        mergedHash,
        mergedPath,
      });
    } catch (err) {
      // 非预期错误标记 failed；业务校验错误已回退为 uploading
      if (!err.status) {
        await pool
          .query("UPDATE files SET status = 'failed' WHERE id = ?", [fileId])
          .catch(() => {});
      }
      throw err;
    }
  }),
);

export default router;
