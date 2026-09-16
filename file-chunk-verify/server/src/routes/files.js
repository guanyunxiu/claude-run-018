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
import {
  writeCasChunk,
  readCasChunk,
  casChunkRelPath,
  mergeCasChunks,
  createMergedReadStream,
  mergedBlobAbs,
  safeFileId,
  safeBaseName,
} from '../storage.js';

const router = express.Router();

const HASH_RE = /^[a-f0-9]{64}$/i;

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
 * @returns 捐赠文件行，或 null
 */
async function findUsableDonor(conn, fileHash, expectedChunkHashes) {
  const [donors] = await conn.query(
    `SELECT id, file_name, file_size, merged_hash, merged_path, total_chunks
       FROM files
      WHERE file_hash = ? AND status = 'completed' AND merged_hash IS NOT NULL
      ORDER BY updated_at DESC
      LIMIT 1
       FOR UPDATE`,
    [fileHash],
  );
  for (const donor of donors) {
    // 合并产物元数据与物理文件都在
    const [mb] = await conn.query(
      'SELECT merged_hash, storage_path FROM merged_blobs WHERE merged_hash = ? FOR UPDATE',
      [donor.merged_hash],
    );
    if (mb.length === 0) continue;
    try {
      await fsp.access(path.join(config.storageDir, mb[0].storage_path));
    } catch {
      continue;
    }

    // 分片关联齐全且物理可读；若给了清单则逐片哈希必须一致
    const [links] = await conn.query(
      `SELECT fc.chunk_index, fc.chunk_hash, cc.storage_path
         FROM file_chunks fc
         JOIN cas_chunks cc ON cc.chunk_hash = fc.chunk_hash
        WHERE fc.file_id = ?
        ORDER BY fc.chunk_index FOR UPDATE`,
      [donor.id],
    );
    if (links.length !== donor.total_chunks) continue;
    let usable = true;
    for (let i = 0; i < links.length; i += 1) {
      const link = links[i];
      if (link.chunk_index !== i) {
        usable = false;
        break;
      }
      if (expectedChunkHashes && expectedChunkHashes[i] !== link.chunk_hash) {
        usable = false;
        break;
      }
      // 抽检物理可读（秒传也要防止元数据指向空文件）
      try {
        const st = await fsp.stat(path.join(config.storageDir, link.storage_path));
        if (st.size === 0 && donor.file_size !== '0') {
          usable = false;
          break;
        }
      } catch {
        usable = false;
        break;
      }
    }
    if (usable) return donor;
  }
  return null;
}

/**
 * 在事务内建立秒传关联：把新文件置为 completed，复用捐赠文件的全部分片与合并产物，
 * 并对 cas_chunks / merged_blobs 做引用计数 +1。
 */
async function linkInstantFile(conn, fileId, fileRow, donor) {
  const [donorLinks] = await conn.query(
    'SELECT chunk_index, chunk_hash FROM file_chunks WHERE file_id = ? ORDER BY chunk_index',
    [donor.id],
  );

  // 合并产物引用 +1
  const [mb] = await conn.query(
    'SELECT merged_hash FROM merged_blobs WHERE merged_hash = ? FOR UPDATE',
    [donor.merged_hash],
  );
  if (mb.length === 0) {
    throw apiError(500, 'INSTANT_LINK_FAILED', '秒传合并产物缺失');
  }
  await conn.query(
    'UPDATE merged_blobs SET ref_count = ref_count + 1 WHERE merged_hash = ?',
    [donor.merged_hash],
  );

  // 分片引用 +1：按捐赠文件的每个序号关联各计一次引用
  // （同一哈希可能出现在多个序号，每个 file_chunks 行各占 1 个引用）
  for (const link of donorLinks) {
    await conn.query(
      'UPDATE cas_chunks SET ref_count = ref_count + 1 WHERE chunk_hash = ?',
      [link.chunk_hash],
    );
    await conn.query(
      `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, status)
       VALUES (?, ?, ?, 'verified')
       ON DUPLICATE KEY UPDATE chunk_hash = VALUES(chunk_hash), status = 'verified'`,
      [fileId, link.chunk_index, link.chunk_hash],
    );
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

      // CAS 全局分片：锁行，存在则引用 +1（本次引用先加上，旧关联稍后回收）
      const [casRows] = await conn.query(
        'SELECT * FROM cas_chunks WHERE chunk_hash = ? FOR UPDATE',
        [expectedHash],
      );
      let casExisted = casRows.length > 0;
      if (casExisted) {
        await conn.query(
          'UPDATE cas_chunks SET ref_count = ref_count + 1 WHERE chunk_hash = ?',
          [expectedHash],
        );
      } else {
        // 全局首传：先原子落盘（哈希已在上方校验），再插入元数据。
        // 并发首传时唯一键可能冲突——交由唯一键兜底，catch 后走“已存在”分支。
        const relPath = casChunkRelPath(expectedHash);
        await writeCasChunk(expectedHash, body);
        try {
          await conn.query(
            `INSERT INTO cas_chunks (chunk_hash, chunk_size, storage_path, ref_count)
             VALUES (?, ?, ?, 1)`,
            [expectedHash, body.length, relPath],
          );
        } catch (insErr) {
          if (insErr.code === 'ER_DUP_ENTRY') {
            casExisted = true;
            // 另一个请求已插入（物理也已原子安装）；ref_count 已被对方置 1，
            // 本请求仍需占 1 个引用
            await conn.query(
              'UPDATE cas_chunks SET ref_count = ref_count + 1 WHERE chunk_hash = ?',
              [expectedHash],
            );
          } else {
            throw insErr;
          }
        }
      }

      // 建立/替换本文件关联
      let skipped = false;
      if (oldLinks.length > 0) {
        const old = oldLinks[0];
        if (old.chunk_hash === expectedHash) {
          // 幂等：同序号同哈希——撤销刚才多 +1 的引用
          skipped = true;
          await conn.query(
            'UPDATE cas_chunks SET ref_count = GREATEST(ref_count - 1, 0) WHERE chunk_hash = ?',
            [expectedHash],
          );
        } else {
          // 内容变了：替换关联，旧分片引用 -1
          await conn.query(
            `UPDATE file_chunks SET chunk_hash = ?, status = 'uploaded' WHERE id = ?`,
            [expectedHash, old.id],
          );
          await conn.query(
            'UPDATE cas_chunks SET ref_count = GREATEST(ref_count - 1, 0) WHERE chunk_hash = ?',
            [old.chunk_hash],
          );
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
        dedup: casExisted,
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

    const [files] = await pool.query('SELECT * FROM files WHERE id = ?', [fileId]);
    if (files.length === 0) throw apiError(404, 'FILE_NOT_FOUND', '文件任务不存在');
    const file = files[0];
    if (file.status === 'completed')
      throw apiError(409, 'FILE_ALREADY_VERIFIED', '文件已完成校验');
    if (file.status === 'merging')
      throw apiError(409, 'FILE_VERIFYING', '文件正在聚合校验中，请勿重复提交');
    if (!file.file_hash)
      throw apiError(
        409,
        'FILE_HASH_REQUIRED',
        '聚合哈希尚未提交，请先 POST /hash 或在 init 带清单',
      );

    // 原子抢占 uploading → merging
    const [claim] = await pool.query(
      "UPDATE files SET status = 'merging' WHERE id = ? AND status = 'uploading'",
      [fileId],
    );
    if (claim.affectedRows === 0) {
      throw apiError(409, 'FILE_VERIFYING', '文件正在聚合校验中，请勿重复提交 complete');
    }

    const resetToUploading = async () => {
      await pool.query("UPDATE files SET status = 'uploading' WHERE id = ?", [fileId]);
    };

    try {
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
          await fsp.access(abs);
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
          `UPDATE files SET status = 'completed', merged_hash = ?, merged_path = ? WHERE id = ?`,
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
      if (!err.status) {
        await pool
          .query("UPDATE files SET status = 'failed' WHERE id = ?", [fileId])
          .catch(() => {});
      }
      throw err;
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
    try {
      await fsp.access(abs);
    } catch {
      throw apiError(410, 'MERGED_BLOB_MISSING', '合并产物物理文件缺失');
    }
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
