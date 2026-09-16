/**
 * 运维接口：孤儿 CAS 对象垃圾回收。
 * 挂载于 /api/admin（与 /api/files/:fileId 分离，避免路径参数冲突）。
 */
import express from 'express';
import { getPool } from '../db.js';
import { removeCasChunk, removeMergedBlob } from '../storage.js';

const router = express.Router();

/**
 * POST /api/admin/gc
 * body: { minAgeSec?: number }  默认仅回收创建超过 5 分钟的零引用对象，
 * 避免与“先建元数据后建关联”的正常首传流程竞争。
 */
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
      const conn = await pool.getConnection();
      let removedChunks = 0;
      let removedMerged = 0;
      let bytesFreed = 0n;
      try {
        await conn.beginTransaction();

        const [orphanChunks] = await conn.query(
          `SELECT chunk_hash, chunk_size, storage_path
             FROM cas_chunks
            WHERE ref_count = 0 AND created_at < ?
            FOR UPDATE`,
          [cutoff],
        );
        for (const c of orphanChunks) {
          try {
            await removeCasChunk(c.chunk_hash);
          } catch {
            continue;
          }
          bytesFreed += BigInt(c.chunk_size);
          await conn.query(
            'DELETE FROM cas_chunks WHERE chunk_hash = ? AND ref_count = 0',
            [c.chunk_hash],
          );
          removedChunks += 1;
        }

        const [orphanMerged] = await conn.query(
          `SELECT merged_hash, file_size, storage_path
             FROM merged_blobs
            WHERE ref_count = 0 AND created_at < ?
            FOR UPDATE`,
          [cutoff],
        );
        for (const mb of orphanMerged) {
          try {
            await removeMergedBlob(mb.merged_hash);
          } catch {
            continue;
          }
          bytesFreed += BigInt(mb.file_size);
          await conn.query(
            'DELETE FROM merged_blobs WHERE merged_hash = ? AND ref_count = 0',
            [mb.merged_hash],
          );
          removedMerged += 1;
        }

        await conn.commit();
        res.json({ removedChunks, removedMerged, bytesFreed: bytesFreed.toString(), minAgeSec });
      } catch (err) {
        await conn.rollback();
        throw err;
      } finally {
        conn.release();
      }
    } catch (err) {
      next(err);
    }
  },
);

export default router;
