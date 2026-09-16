/**
 * 文件分片业务路由 —— 内容寻址（CAS）v3
 *
 *  POST   /init                    注册/恢复；可带 fileHash+chunkHashes 清单：
 *                                  命中已完成相同文件 → 秒传（零上传，建立引用共享合并产物）
 *  POST   /precheck                只读预检：返回 instant 与全局已存在分片命中（不产生副作用）
 *  POST   /:fileId/hash            补报/锁定聚合哈希
 *  GET    /:fileId/chunks          本文件已上传关联（断点续传）
 *  GET    /:fileId/status          任务状态
 *  POST   /:fileId/chunks/:index   上传分片 raw 二进制（按 chunkHash CAS 落盘 + 引用计数，幂等）
 *  POST   /:fileId/complete        强校验（重算 CAS 分片）→ 流式合并 → 引用合并产物
 *  DELETE /:fileId                 删除文件（仅减引用，不物理删除）
 *  GET    /:fileId/download        下载已完成文件（流式读共享合并产物）
 *
 * 孤儿对象 GC 见 /api/admin/gc（routes/admin.js）
 */
import express from 'express';
import path from 'node:path';
import fsp from 'node:fs/promises';
import { getPool } from '../db.js';
import { config } from '../config.js';
import { sha256Hex, aggregateHashHex } from '../hash.js';
import { getLocker } from '../store/locker.js';
import {
  writeCasChunk,
  writeCasChunkIfAbsent,
  readCasChunk,
  casChunkRelPath,
  casChunkPhysicalOk,
  mergeCasChunks,
  mergedBlobExists,
  mergedBlobPhysicalOk,
  createMergedReadStream,
  mergedBlobAbs,
  safeFileId,
  safeBaseName,
} from '../storage.js';
import { ensureCasChunk, releaseOneRef } from '../services/cas.js';

const router = express.Router();

const HASH_RE = /^[a-f0-9]{64}$/i;

function isLeaseExpired(leaseUntil) {
  if (!leaseUntil) return true; // 无租约信息的旧 merging 行可被接管
  const t = new Date(leaseUntil).getTime();
  return Number.isFinite(t) ? t <= Date.now() : true;
}

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
function toBigInt(v) {
  if (typeof v === 'number' && Number.isSafeInteger(v)) return BigInt(v);
  if (typeof v === 'string' && /^\d+$/.test(v)) return BigInt(v);
  return null;
}

function fileChunkRowMapper(r) {
  return {
    index: r.chunk_index,
    hash: r.chunk_hash,
    size: Number(r.chunk_size),
    status: r.status,
  };
}

/** 校验并归一化 init/precheck 的清单参数 */
function parseManifest(body, totalChunks) {
  let fileHash = null;
  if (body.fileHash !== null && body.fileHash !== undefined && body.fileHash !== '') {
    if (typeof body.fileHash !== 'string' || !HASH_RE.test(body.fileHash)) {
      throw apiError(400, 'VALIDATION_ERROR', 'fileHash 必须为 64 位 sha256 hex 或 null');
    }
    fileHash = body.fileHash.toLowerCase();
  }
  let chunkHashes = null;
  if (body.chunkHashes !== undefined && body.chunkHashes !== null) {
    if (!Array.isArray(body.chunkHashes)) {
      throw apiError(400, 'VALIDATION_ERROR', 'chunkHashes 必须为字符串数组');
    }
    if (body.chunkHashes.some((h) => typeof h !== 'string' || !HASH_RE.test(h))) {
      throw apiError(400, 'VALIDATION_ERROR', 'chunkHashes 中存在非法 sha256 hex');
    }
    if (body.chunkHashes.length !== totalChunks) {
      throw apiError(
        400,
        'VALIDATION_ERROR',
        'chunkHashes 长度必须等于 totalChunks',
        { expected: totalChunks, actual: body.chunkHashes.length },
      );
    }
    chunkHashes = body.chunkHashes.map((h) => h.toLowerCase());
  }
  return { fileHash, chunkHashes };
}

/**
 * 查找一个可用的秒捐文件：同聚合哈希、已完成、合并产物与全部分片物理可读。
 * 必须在事务内调用（FOR UPDATE 锁定候选行）。
 *
 * 不再 LIMIT 1：最新捐赠者可能是脏数据（合并产物/分片物理缺失、关联不齐），
 * 必须取全部候选按 updated_at 倒序逐个校验，跳过损坏者直到找到完好者。
 * @returns 捐赠文件行，或 null
 */
async function findUsableDonor(conn, fileHash, expectedChunkHashes) {
  const [donors] = await conn.query(
    `SELECT id, file_name, file_size, merged_hash, merged_path, total_chunks
       FROM files
      WHERE file_hash = ? AND status = 'completed' AND merged_hash IS NOT NULL
      ORDER BY updated_at DESC, created_at DESC
       FOR UPDATE`,
    [fileHash],
  );
  for (const donor of donors) {
    if (await isDonorUsable(conn, donor, expectedChunkHashes)) return donor;
  }
  return null;
}

/** 校验单个候选捐赠者：合并产物可读 + 分片关联齐全 + 逐片哈希一致 + 对象存储可读 */
async function isDonorUsable(conn, donor, expectedChunkHashes) {
  // 合并产物元数据在库，且对象存储上真实存在、大小一致（不能只看本机磁盘）
  const [mb] = await conn.query(
    'SELECT merged_hash FROM merged_blobs WHERE merged_hash = ? FOR UPDATE',
    [donor.merged_hash],
  );
  if (mb.length === 0) return false;
  if (!(await mergedBlobPhysicalOk(donor.merged_hash, donor.file_size))) return false;

  // 分片关联齐全且对象存储可读；若给了清单则逐片哈希必须一致
  const [links] = await conn.query(
    `SELECT fc.chunk_index, fc.chunk_hash, cc.chunk_size
       FROM file_chunks fc
       JOIN cas_chunks cc ON cc.chunk_hash = fc.chunk_hash
      WHERE fc.file_id = ?
      ORDER BY fc.chunk_index FOR UPDATE`,
    [donor.id],
  );
  if (links.length !== donor.total_chunks) return false;
  for (let i = 0; i < links.length; i += 1) {
    const link = links[i];
    if (link.chunk_index !== i) return false;
    if (expectedChunkHashes && expectedChunkHashes[i] !== link.chunk_hash) return false;
    // 逐片在对象存储上可读、大小匹配（S3/MinIO 或本地，统一走 ObjectStore）
    const ok = await casChunkPhysicalOk(link.chunk_hash, link.chunk_size);
    if (!ok) {
      if (link.chunk_size === 0 && donor.file_size === '0') continue;
      return false;
    }
  }
  return true;
}

/**
 * 在事务内建立秒传关联：把（可能已有部分关联的）文件置为 completed，
 * 复用捐赠文件的全部分片与合并产物。
 *
 * 幂等性（修复“边算边传秒传仲裁竞态”）：
 *   目标文件在仲裁前可能已有在途上传落了若干 file_chunks 关联，
 *   因此对每个序号必须按「已关联同哈希 / 已关联异哈希 / 未关联」分别处理，
 *   绝不对已存在的同哈希关联重复 ref_count+1，避免引用虚高导致 GC 永不回收。
 */
async function linkInstantFile(conn, fileId, fileRow, donor) {
  const [donorLinks] = await conn.query(
    'SELECT chunk_index, chunk_hash FROM file_chunks WHERE file_id = ? ORDER BY chunk_index',
    [donor.id],
  );

  // 目标文件已有关联（仲裁前在途上传所建）
  const [existingLinks] = await conn.query(
    'SELECT chunk_index, chunk_hash FROM file_chunks WHERE file_id = ? FOR UPDATE',
    [fileId],
  );
  const existingByIndex = new Map(existingLinks.map((l) => [l.chunk_index, l.chunk_hash]));

  // 合并产物引用：目标已指向同一 merged blob 时不重复计数
  if (fileRow.merged_hash !== donor.merged_hash) {
    const [mb] = await conn.query(
      'SELECT merged_hash FROM merged_blobs WHERE merged_hash = ? FOR UPDATE',
      [donor.merged_hash],
    );
    if (mb.length === 0) {
      throw apiError(500, 'INSTANT_LINK_FAILED', '秒传合并产物缺失');
    }
    // 目标若曾指向别的合并产物（极少见），先回收旧引用
    if (fileRow.merged_hash) {
      await conn.query(
        'UPDATE merged_blobs SET ref_count = GREATEST(ref_count - 1, 0) WHERE merged_hash = ?',
        [fileRow.merged_hash],
      );
    }
    await conn.query(
      'UPDATE merged_blobs SET ref_count = ref_count + 1 WHERE merged_hash = ?',
      [donor.merged_hash],
    );
  }

  // 捐赠分片按序号逐个对齐到目标文件
  for (const link of donorLinks) {
    const current = existingByIndex.get(link.chunk_index);
    if (current === link.chunk_hash) {
      // 已存在同序号同哈希关联（在途上传刚建好）：幂等，引用计数不动
      await conn.query(
        `UPDATE file_chunks SET status = 'verified'
          WHERE file_id = ? AND chunk_index = ?`,
        [fileId, link.chunk_index],
      );
    } else if (current !== undefined) {
      // 该序号已关联别的内容：替换，旧哈希引用 -1、新哈希引用 +1
      await conn.query(
        `UPDATE file_chunks SET chunk_hash = ?, status = 'verified'
          WHERE file_id = ? AND chunk_index = ?`,
        [link.chunk_hash, fileId, link.chunk_index],
      );
      await conn.query(
        'UPDATE cas_chunks SET ref_count = GREATEST(ref_count - 1, 0) WHERE chunk_hash = ?',
        [current],
      );
      await conn.query(
        'UPDATE cas_chunks SET ref_count = ref_count + 1 WHERE chunk_hash = ?',
        [link.chunk_hash],
      );
    } else {
      await conn.query(
        'UPDATE cas_chunks SET ref_count = ref_count + 1 WHERE chunk_hash = ?',
        [link.chunk_hash],
      );
      await conn.query(
        `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, status)
         VALUES (?, ?, ?, 'verified')`,
        [fileId, link.chunk_index, link.chunk_hash],
      );
    }
  }

  // 新文件指向同一合并产物
  await conn.query(
    `UPDATE files
        SET status = 'completed', file_hash = ?, merged_hash = ?, merged_path = ?
      WHERE id = ?`,
    [donor.file_hash, donor.merged_hash, donor.merged_path, fileId],
  );

  return donorLinks;
}

/** 计算全局 CAS 命中：清单中哪些 chunkHash 已存在物理分片（ref_count>0） */
async function globalCasHits(conn, chunkHashes) {
  if (!chunkHashes || chunkHashes.length === 0) return new Map();
  const placeholders = chunkHashes.map(() => '?').join(',');
  const [rows] = await conn.query(
    `SELECT chunk_hash FROM cas_chunks
      WHERE ref_count > 0 AND chunk_hash IN (${placeholders})`,
    chunkHashes,
  );
  const existing = new Set(rows.map((r) => r.chunk_hash));
  const hits = new Map();
  chunkHashes.forEach((h, i) => {
    if (existing.has(h)) hits.set(i, h);
  });
  return hits;
}

/* ------------------------------------------------------------------ */
/* POST /init                                                          */
/* ------------------------------------------------------------------ */
router.post(
  '/init',
  express.json({ limit: '1mb' }),
  asyncHandler(async (req, res) => {
    const b = req.body ?? {};
    const fileName = typeof b.fileName === 'string' ? b.fileName.trim() : '';
    const fileSize = toBigInt(b.fileSize);
    const chunkSize = b.chunkSize;
    const totalChunks = b.totalChunks;
    const fileId = typeof b.fileId === 'string' ? b.fileId.toLowerCase() : '';

    if (!fileName || fileName.length > 512)
      throw apiError(400, 'VALIDATION_ERROR', 'fileName 非法（必填，最长 512）');
    if (fileSize === null)
      throw apiError(400, 'VALIDATION_ERROR', 'fileSize 必须为非负整数');
    if (!isPosInt(chunkSize))
      throw apiError(400, 'VALIDATION_ERROR', 'chunkSize 必须为正整数（字节）');
    if (!isNonNegInt(totalChunks))
      throw apiError(400, 'VALIDATION_ERROR', 'totalChunks 必须为非负整数');
    if (!HASH_RE.test(fileId))
      throw apiError(400, 'VALIDATION_ERROR', 'fileId 必须为 64 位 hex');
    const expectedChunks =
      fileSize === 0n ? 0 : Number((fileSize + BigInt(chunkSize) - 1n) / BigInt(chunkSize));
    if (expectedChunks !== totalChunks) {
      throw apiError(400, 'VALIDATION_ERROR', `totalChunks 应为 ${expectedChunks}`, {
        expectedChunks,
      });
    }
    const { fileHash: manifestHash, chunkHashes } = parseManifest(b, totalChunks);

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [rows] = await conn.query(
        'SELECT * FROM files WHERE id = ? FOR UPDATE',
        [fileId],
      );
      let fileRow = rows[0];

      // 基础参数冲突检查（已存在任务时）
      if (fileRow) {
        const baseMismatch =
          fileRow.file_name !== fileName ||
          BigInt(fileRow.file_size) !== fileSize ||
          fileRow.chunk_size !== chunkSize ||
          fileRow.total_chunks !== totalChunks;
        const hashMismatch =
          manifestHash && fileRow.file_hash && fileRow.file_hash !== manifestHash;
        if (baseMismatch || hashMismatch) {
          throw apiError(
            409,
            'FILE_PARAM_MISMATCH',
            '相同 fileId 的文件参数与服务端记录不一致',
          );
        }
      } else {
        await conn.query(
          `INSERT INTO files
             (id, file_name, file_size, chunk_size, total_chunks, file_hash, status)
           VALUES (?, ?, ?, ?, ?, ?, 'uploading')`,
          [
            fileId,
            fileName,
            fileSize.toString(),
            chunkSize,
            totalChunks,
            manifestHash,
          ],
        );
        const [created] = await conn.query('SELECT * FROM files WHERE id = ?', [fileId]);
        fileRow = created[0];
      }

      // 秒传：清单完整 + 聚合哈希 + 可用捐赠文件（同一 fileId 已完成也算）
      const canInstant =
        manifestHash !== null && chunkHashes !== null && chunkHashes.length === totalChunks;

      if (canInstant && fileRow.status !== 'completed') {
        const donor = await findUsableDonor(conn, manifestHash, chunkHashes);
        if (donor) {
          await linkInstantFile(conn, fileId, fileRow, donor);
          await conn.commit();

          const [links] = await getPool().query(
            'SELECT fc.chunk_index, fc.chunk_hash, cc.chunk_size, fc.status FROM file_chunks fc JOIN cas_chunks cc ON cc.chunk_hash = fc.chunk_hash WHERE fc.file_id = ? ORDER BY fc.chunk_index',
            [fileId],
          );
          return res.status(200).json({
            resumed: true,
            instant: true,
            file: {
              fileId,
              fileName,
              fileSize: Number(fileSize),
              chunkSize,
              totalChunks,
              fileHash: manifestHash,
              hashLocked: true,
              status: 'completed',
              mergedHash: donor.merged_hash,
              mergedPath: donor.merged_path,
            },
            uploadedChunks: links.map(fileChunkRowMapper),
            hits: Object.fromEntries(chunkHashes.map((h, i) => [i, h])),
          });
        }
      }

      // 已完成的同 fileId 任务：幂等返回（等价秒传自身）
      if (fileRow.status === 'completed') {
        await conn.commit();
        const [links] = await getPool().query(
          'SELECT fc.chunk_index, fc.chunk_hash, cc.chunk_size, fc.status FROM file_chunks fc JOIN cas_chunks cc ON cc.chunk_hash = fc.chunk_hash WHERE fc.file_id = ? ORDER BY fc.chunk_index',
          [fileId],
        );
        return res.status(200).json({
          resumed: true,
          instant: true,
          file: {
            fileId,
            fileName: fileRow.file_name,
            fileSize: Number(fileRow.file_size),
            chunkSize: fileRow.chunk_size,
            totalChunks: fileRow.total_chunks,
            fileHash: fileRow.file_hash,
            hashLocked: true,
            status: 'completed',
            mergedHash: fileRow.merged_hash,
            mergedPath: fileRow.merged_path,
          },
          uploadedChunks: links.map(fileChunkRowMapper),
          hits: Object.fromEntries(links.map((l) => [l.chunk_index, l.chunk_hash])),
        });
      }

      // 非秒传：若带了聚合哈希则锁定
      if (manifestHash && !fileRow.file_hash) {
        await conn.query('UPDATE files SET file_hash = ? WHERE id = ?', [
          manifestHash,
          fileId,
        ]);
        fileRow.file_hash = manifestHash;
      }

      // 本文件已上传分片
      const [myLinks] = await conn.query(
        `SELECT fc.chunk_index, fc.chunk_hash, cc.chunk_size, fc.status
           FROM file_chunks fc
           JOIN cas_chunks cc ON cc.chunk_hash = fc.chunk_hash
          WHERE fc.file_id = ? ORDER BY fc.chunk_index`,
        [fileId],
      );
      // 全局 CAS 命中（其它文件共享的内容分片）
      const hits = await globalCasHits(conn, chunkHashes);

      await conn.commit();
      res.status(rows.length > 0 ? 200 : 201).json({
        resumed: rows.length > 0,
        instant: false,
        file: {
          fileId,
          fileName,
          fileSize: Number(fileSize),
          chunkSize,
          totalChunks,
          fileHash: fileRow.file_hash,
          hashLocked: fileRow.file_hash !== null,
          status: fileRow.status,
          mergedHash: null,
          mergedPath: null,
        },
        uploadedChunks: myLinks.map(fileChunkRowMapper),
        hits: Object.fromEntries(hits),
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
/* POST /precheck —— 只读秒传/分片命中预检                              */
/* ------------------------------------------------------------------ */
router.post(
  '/precheck',
  express.json({ limit: '1mb' }),
  asyncHandler(async (req, res) => {
    const b = req.body ?? {};
    const fileHash =
      typeof b.fileHash === 'string' && HASH_RE.test(b.fileHash)
        ? b.fileHash.toLowerCase()
        : null;
    const rawHashes = Array.isArray(b.chunkHashes) ? b.chunkHashes : null;
    if (!rawHashes || rawHashes.some((h) => typeof h !== 'string' || !HASH_RE.test(h))) {
      throw apiError(400, 'VALIDATION_ERROR', 'chunkHashes 必须为合法 sha256 hex 数组');
    }
    const chunkHashes = rawHashes.map((h) => h.toLowerCase());

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      let instant = false;
      let donorInfo = null;
      if (fileHash) {
        const donor = await findUsableDonor(conn, fileHash, chunkHashes);
        if (donor) {
          instant = true;
          donorInfo = {
            fileId: donor.id,
            fileName: donor.file_name,
            mergedHash: donor.merged_hash,
          };
        }
      }
      const hits = instant
        ? new Map(chunkHashes.map((h, i) => [i, h]))
        : await globalCasHits(conn, chunkHashes);
      await conn.commit();
      res.json({
        instant,
        donor: donorInfo,
        hits: Object.fromEntries(hits),
        hitCount: hits.size,
        totalChunks: chunkHashes.length,
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
/* POST /:fileId/hash                                                  */
/* ------------------------------------------------------------------ */
router.post(
  '/:fileId/hash',
  express.json({ limit: '64kb' }),
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    const fileHash =
      typeof req.body?.fileHash === 'string' ? req.body.fileHash.toLowerCase() : '';
    if (!HASH_RE.test(fileHash))
      throw apiError(400, 'VALIDATION_ERROR', 'fileHash 必须为 64 位 sha256 hex');

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [rows] = await conn.query(
        'SELECT id, file_hash, status FROM files WHERE id = ? FOR UPDATE',
        [fileId],
      );
      if (rows.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '请先调用 /init');
      const file = rows[0];
      if (file.status === 'completed')
        throw apiError(409, 'FILE_ALREADY_VERIFIED', '文件已完成校验');
      if (file.status === 'merging')
        throw apiError(409, 'FILE_VERIFYING', '文件正在聚合校验中');
      if (file.file_hash && file.file_hash !== fileHash) {
        throw apiError(409, 'FILE_HASH_LOCKED', '聚合哈希已提交为不同值，请使用新的 fileId', {
          existingHash: file.file_hash,
        });
      }
      const changed = file.file_hash === null;
      if (changed)
        await conn.query('UPDATE files SET file_hash = ? WHERE id = ?', [fileHash, fileId]);
      await conn.commit();
      res.status(changed ? 201 : 200).json({ fileId, fileHash, locked: true, changed });
    } catch (err) {
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
  }),
);

/* ------------------------------------------------------------------ */
/* GET /:fileId/chunks                                                 */
/* ------------------------------------------------------------------ */
router.get(
  '/:fileId/chunks',
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    const pool = getPool();
    const [files] = await pool.query('SELECT id FROM files WHERE id = ?', [fileId]);
    if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '文件任务不存在');
    const [rows] = await pool.query(
      `SELECT fc.chunk_index, fc.chunk_hash, cc.chunk_size, fc.status
         FROM file_chunks fc
         JOIN cas_chunks cc ON cc.chunk_hash = fc.chunk_hash
        WHERE fc.file_id = ? ORDER BY fc.chunk_index`,
      [fileId],
    );
    res.json({
      fileId,
      uploadedCount: rows.length,
      chunks: rows.map(fileChunkRowMapper),
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
      'SELECT COUNT(*) AS c FROM file_chunks WHERE file_id = ?',
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
      hashLocked: f.file_hash !== null,
      mergedHash: f.merged_hash,
      mergedPath: f.merged_path,
    });
  }),
);

/* ------------------------------------------------------------------ */
/* POST /:fileId/chunks/:index  —— CAS 分片上传（幂等 + 引用计数）      */
/* ------------------------------------------------------------------ */
const rawParser = express.raw({ type: () => true, limit: config.chunkLimitBytes });

router.post(
  '/:fileId/chunks/:index',
  rawParser,
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    if (!/^\d+$/.test(req.params.index))
      throw apiError(400, 'VALIDATION_ERROR', '分片序号必须为非负整数');
    const index = Number(req.params.index);
    const expectedHash =
      typeof req.query.hash === 'string' ? req.query.hash.toLowerCase() : '';
    if (!HASH_RE.test(expectedHash))
      throw apiError(400, 'VALIDATION_ERROR', '查询参数 hash 必须为 64 位 sha256 hex');

    const body = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
    const actualHash = sha256Hex(body);
    if (actualHash !== expectedHash) {
      // 哈希不符直接拒绝，绝不触碰 CAS 存储
      throw apiError(422, 'CHUNK_HASH_MISMATCH', '分片哈希校验失败，拒绝写入', {
        index,
        expectedHash,
        actualHash,
      });
    }

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [files] = await conn.query(
        'SELECT * FROM files WHERE id = ? FOR UPDATE',
        [fileId],
      );
      if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '请先调用 /init');
      const file = files[0];
      if (file.status === 'completed')
        throw apiError(409, 'FILE_ALREADY_VERIFIED', '文件已完成校验，无需再上传分片');
      if (file.status === 'merging')
        throw apiError(409, 'FILE_VERIFYING', '文件正在聚合校验中，禁止上传分片');
      if (index < 0 || index >= file.total_chunks)
        throw apiError(400, 'CHUNK_INDEX_OUT_OF_RANGE', '分片序号超出范围', {
          index,
          totalChunks: file.total_chunks,
        });

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

      // 该文件该序号是否已有旧关联（重传不同内容时需要转移引用）
      const [oldLinks] = await conn.query(
        'SELECT id, chunk_hash FROM file_chunks WHERE file_id = ? AND chunk_index = ? FOR UPDATE',
        [fileId, index],
      );

      // 跨机器保证对象+行+引用一致（锁 + 对象条件写 + DB 唯一键），并自愈幽灵片
      const locker = await getLocker();
      const casResult = await ensureCasChunk({ conn, hash: expectedHash, body, locker });
      let casExisted = casResult.existed;
      const healed = casResult.healed;

      // 建立/替换本文件关联
      let skipped = false;
      if (oldLinks.length > 0) {
        const old = oldLinks[0];
        if (old.chunk_hash === expectedHash) {
          // 幂等：同序号同哈希——撤销 ensureCasChunk 多 +1 的引用
          skipped = true;
          await releaseOneRef(conn, expectedHash);
        } else {
          // 内容变了：替换关联，旧分片引用 -1
          await conn.query(
            `UPDATE file_chunks SET chunk_hash = ?, status = 'uploaded' WHERE id = ?`,
            [expectedHash, old.id],
          );
          await releaseOneRef(conn, old.chunk_hash);
        }
      } else {
        await conn.query(
          `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, status)
           VALUES (?, ?, ?, 'uploaded')`,
          [fileId, index, expectedHash],
        );
      }

      await conn.commit();
      res.status(skipped ? 200 : 201).json({
        index,
        hash: expectedHash,
        size: body.length,
        skipped,
        dedup: casResult.dedup,
        healed,
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
/* POST /:fileId/chunks/:index/link  —— 只关联已有 CAS 分片，不传字节   */
/* ------------------------------------------------------------------ */
router.post(
  '/:fileId/chunks/:index/link',
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    if (!/^\d+$/.test(req.params.index))
      throw apiError(400, 'VALIDATION_ERROR', '分片序号必须为非负整数');
    const index = Number(req.params.index);
    const hash =
      typeof req.query.hash === 'string' ? req.query.hash.toLowerCase() : '';
    if (!HASH_RE.test(hash))
      throw apiError(400, 'VALIDATION_ERROR', '查询参数 hash 必须为 64 位 sha256 hex');

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [files] = await conn.query(
        'SELECT * FROM files WHERE id = ? FOR UPDATE',
        [fileId],
      );
      if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '请先调用 /init');
      const file = files[0];
      if (file.status === 'completed')
        throw apiError(409, 'FILE_ALREADY_VERIFIED', '文件已完成校验，无需再关联分片');
      if (file.status === 'merging')
        throw apiError(409, 'FILE_VERIFYING', '文件正在聚合校验中，禁止关联分片');
      if (index < 0 || index >= file.total_chunks)
        throw apiError(400, 'CHUNK_INDEX_OUT_OF_RANGE', '分片序号超出范围', {
          index,
          totalChunks: file.total_chunks,
        });

      // 只关联：对象与行必须已存在且可读。ensureCasChunk 不传字节，
      // 缺失/幽灵时抛 CAS_CHUNK_NOT_FOUND / CHUNK_FILE_MISSING，前端回退字节上传。
      const [oldLinks] = await conn.query(
        'SELECT id, chunk_hash FROM file_chunks WHERE file_id = ? AND chunk_index = ? FOR UPDATE',
        [fileId, index],
      );

      let skipped = false;
      let casSize;
      if (oldLinks.length > 0 && oldLinks[0].chunk_hash === hash) {
        // 幂等：本文件该序号已关联同一 CAS 分片，引用计数不动；仍要确认对象可读
        const [rows] = await conn.query(
          'SELECT chunk_size FROM cas_chunks WHERE chunk_hash = ? FOR UPDATE',
          [hash],
        );
        if (rows.length === 0 || !(await casChunkPhysicalOk(hash, rows[0].chunk_size))) {
          throw apiError(
            409,
            'CHUNK_FILE_MISSING',
            '已关联的 CAS 物理分片缺失，请改走字节上传自愈',
            { index, hash },
          );
        }
        casSize = rows[0].chunk_size;
        skipped = true;
      } else {
        const locker = await getLocker();
        try {
          const r = await ensureCasChunk({ conn, hash, body: null, locker });
          casSize = r;
        } catch (e) {
          if (e.status === 409) {
            throw apiError(e.status, e.code, e.message, { ...(e.details || {}), index });
          }
          throw e;
        }
        if (oldLinks.length > 0) {
          // 旧关联是不同内容：替换并回收旧引用（ensure 已给新哈希 +1）
          await conn.query(
            `UPDATE file_chunks SET chunk_hash = ?, status = 'uploaded' WHERE id = ?`,
            [hash, oldLinks[0].id],
          );
          await releaseOneRef(conn, oldLinks[0].chunk_hash);
        } else {
          await conn.query(
            `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, status)
             VALUES (?, ?, ?, 'uploaded')`,
            [fileId, index, hash],
          );
        }
        const [rows] = await conn.query(
          'SELECT chunk_size FROM cas_chunks WHERE chunk_hash = ?',
          [hash],
        );
        casSize = rows[0]?.chunk_size;
      }

      await conn.commit();
      res.status(skipped ? 200 : 201).json({
        index,
        hash,
        size: Number(casSize),
        skipped,
        linked: true,
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
/* POST /:fileId/complete                                              */
/* ------------------------------------------------------------------ */
router.post(
  '/:fileId/complete',
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    const pool = getPool();
    const locker = await getLocker();

    // 跨机器串行化同一文件的合并：双机同时点完成，只有一个能拿到锁进入。
    const mergeLock = locker
      ? await locker.acquire(`merge:${fileId}`, { waitMs: 0, ttlMs: Math.max(config.lock.ttlMs, 120_000) })
      : null;
    if (!mergeLock && locker) {
      throw apiError(409, 'FILE_VERIFYING', '另一台实例正在合并该文件，请勿重复提交 complete');
    }
    try {
      const [files] = await pool.query('SELECT * FROM files WHERE id = ?', [fileId]);
      if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '文件任务不存在');
      const file = files[0];
      if (file.status === 'completed')
        throw apiError(409, 'FILE_ALREADY_VERIFIED', '文件已完成校验');
      if (file.status === 'merging' && !isLeaseExpired(file.merge_lease_until)) {
        throw apiError(409, 'FILE_VERIFYING', '文件正在聚合校验中，请勿重复提交');
      }
      if (!file.file_hash)
        throw apiError(
          409,
          'FILE_HASH_REQUIRED',
          '聚合哈希尚未提交，请先 POST /hash 或在 init 带清单',
        );

      // 原子抢占：uploading→merging；或接管租约已过期的卡死 merging（崩溃恢复）
      const owner = `${process.env.HOSTNAME || 'node'}:${process.pid}`;
      const leaseMs = Math.max(config.lock.ttlMs, 120_000);
      const leaseUntil = new Date(Date.now() + leaseMs);
      const [claim] = await pool.query(
        `UPDATE files
            SET status='merging', merge_owner=?, merge_lease_until=?
          WHERE id=? AND (status='uploading'
                          OR (status='merging' AND (merge_lease_until IS NULL OR merge_lease_until < NOW(3))))`,
        [owner, leaseUntil, fileId],
      );
      if (claim.affectedRows === 0) {
        throw apiError(409, 'FILE_VERIFYING', '文件正在聚合校验中，请勿重复提交 complete');
      }

      const resetToUploading = async () => {
        await pool.query(
          `UPDATE files SET status='uploading', merge_owner=NULL, merge_lease_until=NULL
            WHERE id=? AND status='merging'`,
          [fileId],
        );
      };

      const [rows] = await pool.query(
        `SELECT fc.chunk_index, fc.chunk_hash, cc.chunk_size AS cas_size, cc.storage_path
           FROM file_chunks fc
           JOIN cas_chunks cc ON cc.chunk_hash = fc.chunk_hash
          WHERE fc.file_id = ? ORDER BY fc.chunk_index`,
        [fileId],
      );
      const total = file.total_chunks;
      if (rows.length !== total) {
        await resetToUploading();
        throw apiError(409, 'CHUNKS_INCOMPLETE', '分片数量不足，无法完成校验', {
          expected: total,
          actual: rows.length,
        });
      }

      const fileSize = BigInt(file.file_size);
      const chunkSize = BigInt(file.chunk_size);
      const orderedHashes = [];
      let sumSize = 0n;

      // 逐片重读物理 CAS 分片，重算哈希（命中/共享分片同样校验），防元数据指向空文件
      for (let i = 0; i < total; i += 1) {
        const row = rows[i];
        if (!row || row.chunk_index !== i) {
          await resetToUploading();
          throw apiError(409, 'CHUNKS_INCOMPLETE', `缺少分片或序号不连续：index=${i}`);
        }
        const expectedSize =
          i === total - 1 ? fileSize - BigInt(i) * chunkSize : chunkSize;
        if (BigInt(row.cas_size) !== expectedSize) {
          await resetToUploading();
          throw apiError(413, 'CHUNK_SIZE_MISMATCH', '记录的分片大小异常', { index: i });
        }
        const abs = path.join(config.storageDir, row.storage_path);
        let buf;
        try {
          // 统一走对象存储抽象（local 或 S3），不直接访问本机文件系统
          void abs;
          buf = await readCasChunk(row.chunk_hash);
        } catch {
          await resetToUploading();
          throw apiError(409, 'CHUNK_FILE_MISSING', `物理分片缺失：index=${i} hash=${row.chunk_hash}`);
        }
        const h = sha256Hex(buf);
        if (h !== row.chunk_hash) {
          await resetToUploading();
          throw apiError(422, 'CHUNK_HASH_MISMATCH', '重算分片哈希与记录不符', {
            index: i,
            storedHash: row.chunk_hash,
            actualHash: h,
          });
        }
        orderedHashes.push(h);
        sumSize += BigInt(buf.length);
      }

      if (sumSize !== fileSize) {
        await resetToUploading();
        throw apiError(422, 'FILE_SIZE_MISMATCH', '分片累计大小与文件总大小不一致');
      }
      const recomputedAggregate = aggregateHashHex(orderedHashes);
      if (recomputedAggregate !== file.file_hash) {
        await resetToUploading();
        throw apiError(422, 'AGGREGATE_HASH_MISMATCH', '聚合哈希校验失败', {
          expectedHash: file.file_hash,
          actualHash: recomputedAggregate,
        });
      }

      // 流式合并共享 CAS 分片 → 内容寻址合并产物
      const merged = await mergeCasChunks(orderedHashes);

      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();
        // 合并产物引用计数（可能已被同内容文件共享）
        const [mb] = await conn.query(
          'SELECT merged_hash FROM merged_blobs WHERE merged_hash = ? FOR UPDATE',
          [merged.mergedHash],
        );
        if (mb.length === 0) {
          await conn.query(
            `INSERT INTO merged_blobs (merged_hash, file_size, storage_path, ref_count)
             VALUES (?, ?, ?, 1)`,
            [merged.mergedHash, fileSize.toString(), merged.relPath],
          );
        } else {
          await conn.query(
            'UPDATE merged_blobs SET ref_count = ref_count + 1 WHERE merged_hash = ?',
            [merged.mergedHash],
          );
        }
        await conn.query("UPDATE file_chunks SET status = 'verified' WHERE file_id = ?", [
          fileId,
        ]);
        await conn.query(
          `UPDATE files
              SET status = 'completed', merged_hash = ?, merged_path = ?,
                  merge_owner = NULL, merge_lease_until = NULL
            WHERE id = ?`,
          [merged.mergedHash, merged.relPath, fileId],
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
        instant: false,
        fileId,
        totalChunks: total,
        fileSize: Number(merged.totalBytes),
        aggregateHash: recomputedAggregate,
        mergedHash: merged.mergedHash,
        mergedPath: merged.relPath,
      });
    } catch (err) {
      // 合并/对象存储异常必须可恢复：回退为 uploading 并清空租约，
      // 客户端/另一台实例可再次抢占 complete 重试（幂等，半成品只在 tmp/）。
      // 不再置终态 failed（旧逻辑会让任务永久卡死、租约接管也进不去）。
      if (!err.status || err.status >= 500) {
        await pool
          .query(
            `UPDATE files
                SET status='uploading', merge_owner=NULL, merge_lease_until=NULL
              WHERE id=? AND status='merging'`,
            [fileId],
          )
          .catch(() => {});
      }
      throw err;
    } finally {
      if (mergeLock) await mergeLock.release();
    }
  }),
);

/* ------------------------------------------------------------------ */
/* DELETE /:fileId  —— 只减引用，不物理删除（物理由 GC 回收）            */
/* ------------------------------------------------------------------ */
router.delete(
  '/:fileId',
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [files] = await conn.query(
        'SELECT * FROM files WHERE id = ? FOR UPDATE',
        [fileId],
      );
      if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '文件任务不存在');
      const file = files[0];

      const [links] = await conn.query(
        'SELECT chunk_index, chunk_hash FROM file_chunks WHERE file_id = ? ORDER BY chunk_index FOR UPDATE',
        [fileId],
      );
      // 每个被引用的唯一分片计数 -1（同文件内分片哈希互不相同，按行递减即可）
      for (const link of links) {
        await conn.query(
          'UPDATE cas_chunks SET ref_count = GREATEST(ref_count - 1, 0) WHERE chunk_hash = ?',
          [link.chunk_hash],
        );
      }
      await conn.query('DELETE FROM file_chunks WHERE file_id = ?', [fileId]);

      let freedMerged = false;
      if (file.merged_hash) {
        const [mb] = await conn.query(
          'SELECT merged_hash FROM merged_blobs WHERE merged_hash = ? FOR UPDATE',
          [file.merged_hash],
        );
        if (mb.length > 0) {
          await conn.query(
            'UPDATE merged_blobs SET ref_count = GREATEST(ref_count - 1, 0) WHERE merged_hash = ?',
            [file.merged_hash],
          );
          freedMerged = true;
        }
      }

      await conn.query('DELETE FROM files WHERE id = ?', [fileId]);
      await conn.commit();

      res.json({
        deleted: true,
        fileId,
        dereferencedChunks: links.length,
        dereferencedMerged: freedMerged,
        // 物理文件不立即删除；等待 /admin/gc 回收 ref_count=0 的对象
        physicalRemoved: false,
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
/* GET /:fileId/download                                               */
/* ------------------------------------------------------------------ */
router.get(
  '/:fileId/download',
  asyncHandler(async (req, res) => {
    const fileId = safeFileId(req.params.fileId);
    const pool = getPool();
    const [files] = await pool.query('SELECT * FROM files WHERE id = ?', [fileId]);
    if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '文件任务不存在');
    const file = files[0];
    if (file.status !== 'completed' || !file.merged_hash) {
      throw apiError(409, 'FILE_NOT_READY', '文件尚未完成校验，无法下载');
    }
    const abs = mergedBlobAbs(file.merged_hash);
    // 统一通过对象存储抽象校验可读（local 查文件系统，S3 查 HEAD）
    const ok = await mergedBlobExists(file.merged_hash);
    if (!ok) {
      throw apiError(410, 'MERGED_BLOB_MISSING', '合并产物在对象存储中缺失');
    }
    void abs;
    const downloadName = encodeURIComponent(safeBaseName(file.file_name));
    res.setHeader('Content-Type', 'application/octet-stream');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename*=UTF-8''${downloadName}`,
    );
    res.setHeader('Content-Length', Number(file.file_size));
    res.setHeader('X-Merged-Hash', file.merged_hash);
    createMergedReadStream(file.merged_hash).pipe(res);
  }),
);

export default router;
