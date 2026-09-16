/**
 * 运维接口：孤儿 CAS 对象垃圾回收（跨机器安全、可恢复、幂等）。
 * 挂载于 /api/admin。
 *
 * 多机协调：
 *   - Redis 全局锁 `gc`：同一时刻全集群只有一台实例执行清理，其它请求直接跳过/快速返回。
 *   - 只处理 ref_count=0 且 created_at 早于宽限 cutoff 的行，跳过正在写入/刚写入的对象。
 *
 * 顺序（绝不“先删对象后改库”）：
 *   A. 事务内锁零引用行并 DELETE 库行，先提交；
 *   B. 提交后删对象；删前再 stat 确认（覆盖“首传对撞”：若同一 hash 在间隙被重新建为
 *      有引用对象，sweep 只删库里确实无行的 key，不会误删别人正在装的内容寻址对象）；
 *   C. 对账：删除“库中已无任何行”的对象孤儿（上次 GC 杀在 A 之后/B 之前的残留）。
 *   任何阶段被 kill，重跑都收敛：tmp/ 半成品永不暴露，内容寻址目标要么不存在要么完整。
 */
import express from 'express';
import { getPool } from '../db.js';
import { getLocker } from '../store/locker.js';
import { objectStore } from '../store/index.js';
import { casKey, mergedKey, safeHash } from '../store/base.js';

const router = express.Router();

router.post(
  '/gc',
  express.json({ limit: '8kb' }),
  async (req, res, next) => {
    try {
      const minAgeSec = Number.isInteger(req.body?.minAgeSec)
        ? Math.max(0, req.body.minAgeSec)
        : 300;
      const cutoff = new Date(Date.now() - minAgeSec * 1000);
      const pool = getPool();
      const locker = await getLocker();

      // 全集群同一时刻只跑一个 GC；拿不到锁则跳过（不视为错误）
      let gcLock = null;
      if (locker) {
        gcLock = await locker.acquire('gc', { waitMs: 0, ttlMs: 120_000 });
        if (!gcLock) {
          return res.json({
            skipped: true,
            reason: 'another_instance_is_gc',
            removedChunks: 0,
            removedMerged: 0,
            bytesFreed: '0',
          });
        }
      }

      try {
        /* ---------- 阶段 A：事务内只删零引用库行 ---------- */
        const conn = await pool.getConnection();
        let removedChunks = 0;
        let removedMerged = 0;
        let bytesFreed = 0n;
        const detachedChunks = []; // 已删行、待删对象
        const detachedMerged = [];
        try {
          await conn.beginTransaction();

          const [orphanChunks] = await conn.query(
            `SELECT chunk_hash, chunk_size, storage_path
               FROM cas_chunks
              WHERE ref_count = 0 AND created_at < ?
              FOR UPDATE SKIP LOCKED`,
            [cutoff],
          );
          for (const c of orphanChunks) {
            await conn.query(
              'DELETE FROM cas_chunks WHERE chunk_hash = ? AND ref_count = 0',
              [c.chunk_hash],
            );
            bytesFreed += BigInt(c.chunk_size);
            detachedChunks.push(c.chunk_hash);
            removedChunks += 1;
          }

          const [orphanMerged] = await conn.query(
            `SELECT merged_hash, file_size, storage_path
               FROM merged_blobs
              WHERE ref_count = 0 AND created_at < ?
              FOR UPDATE SKIP LOCKED`,
            [cutoff],
          );
          for (const mb of orphanMerged) {
            await conn.query(
              'DELETE FROM merged_blobs WHERE merged_hash = ? AND ref_count = 0',
              [mb.merged_hash],
            );
            detachedMerged.push(mb.merged_hash);
            removedMerged += 1;
          }

          await conn.commit();
        } catch (err) {
          await conn.rollback().catch(() => {});
          conn.release();
          throw err;
        }
        conn.release();

        /* ---------- 阶段 B：提交后删对象（先确认仍无库行，防首传对撞误删） ---------- */
        let physicalDeleteFailed = 0;
        for (const hash of detachedChunks) {
          try {
            // 对撞保护：间隙内若首传重建了同 hash 的行，说明对象又被引用，不删
            const [rows] = await pool.query(
              'SELECT 1 AS x FROM cas_chunks WHERE chunk_hash = ? LIMIT 1',
              [hash],
            );
            if (rows.length > 0) continue;
            await objectStore().delete(casKey(safeHash(hash)));
          } catch {
            physicalDeleteFailed += 1;
          }
        }
        for (const hash of detachedMerged) {
          try {
            const [rows] = await pool.query(
              'SELECT 1 AS x FROM merged_blobs WHERE merged_hash = ? LIMIT 1',
              [hash],
            );
            if (rows.length > 0) continue;
            await objectStore().delete(mergedKey(safeHash(hash)));
          } catch {
            physicalDeleteFailed += 1;
          }
        }

        /* ---------- 阶段 C：对账删除“库中无行”的对象孤儿（崩溃恢复/历史脏数据） ---------- */
        const [allChunkRows] = await pool.query('SELECT chunk_hash FROM cas_chunks');
        const [allMergedRows] = await pool.query('SELECT merged_hash FROM merged_blobs');
        const orphanChunkFiles = await sweepObjectOrphans(
          new Set(allChunkRows.map((r) => safeHash(r.chunk_hash))),
          'cas/',
          cutoff,
        );
        const orphanMergedFiles = await sweepObjectOrphans(
          new Set(allMergedRows.map((r) => safeHash(r.merged_hash))),
          'merged/',
          cutoff,
        );

        // 清理宽限期之前的 tmp/ 残留（合并/上传半成品）
        const tempRemoved = await sweepTmp(cutoff);

        res.json({
          skipped: false,
          removedChunks,
          removedMerged,
          bytesFreed: bytesFreed.toString(),
          physicalDeleteFailed,
          orphanChunkFilesRemoved: orphanChunkFiles,
          orphanMergedFilesRemoved: orphanMergedFiles,
          tempObjectsRemoved: tempRemoved,
          minAgeSec,
        });
      } finally {
        if (gcLock) await gcLock.release();
      }
    } catch (err) {
      next(err);
    }
  },
);

/** 删除“库里无行”的内容寻址对象；按对象 mtime/LastModified 宽限，保护刚写入对象 */
async function sweepObjectOrphans(knownHashes, prefix, cutoff) {
  const items = await objectStore().listPrefixMeta(prefix);
  let removed = 0;
  for (const it of items) {
    const m = prefix === 'cas/'
      ? /\/([a-f0-9]{64})\.part$/.exec(it.key)
      : /\/([a-f0-9]{64})\.bin$/.exec(it.key);
    if (!m) continue;
    if (knownHashes.has(m[1])) continue;
    if (it.mtimeMs && it.mtimeMs >= cutoff.getTime()) continue;
    await objectStore().delete(it.key);
    removed += 1;
  }
  return removed;
}

/** 删除宽限期之前的 tmp 残留（合并/上传半成品） */
async function sweepTmp(cutoff) {
  const items = await objectStore().listPrefixMeta('tmp/');
  let n = 0;
  for (const it of items) {
    if (it.mtimeMs && it.mtimeMs >= cutoff.getTime()) continue;
    await objectStore().delete(it.key);
    n += 1;
  }
  return n;
}

export default router;
