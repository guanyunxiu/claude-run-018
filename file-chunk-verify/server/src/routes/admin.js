/**
 * 运维接口：孤儿 CAS 对象垃圾回收（可恢复、可重入）。
 * 挂载于 /api/admin（与 /api/files/:fileId 分离，避免路径参数冲突）。
 *
 * 关键正确性（修复“GC 幽灵分片”）：
 *   旧实现“事务内先删物理文件、再删库行”，若删文件后事务回滚/进程崩溃，
 *   会留下「cas_chunks 有行、磁盘无 .part」的幽灵行，此后去重上传只 +ref
 *   不验盘，complete/download 即 CHUNK_FILE_MISSING。
 *
 * 现实现：
 *   阶段 A（事务，先提交）：锁零引用行并 DELETE 库行；不碰物理文件。
 *   阶段 B（提交后，幂等）：删除这些行对应的物理文件；删盘失败不回滚已提交的库删除，
 *     下次 GC 由阶段 C 兜底。
 *   阶段 C（磁盘对账）：删除“库中已无任何行”的物理孤儿（覆盖 B 中途崩溃残留，
 *     以及历史脏数据：有行无文件的幽灵行会被引用方在上传时自愈重写）。
 */
import express from 'express';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { getPool } from '../db.js';
import { config } from '../config.js';
import {
  removeCasChunk,
  removeMergedBlob,
  sweepOrphanPhysical,
} from '../storage.js';

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

      /* ---------- 阶段 A：事务内只删库行（ref_count=0 且超过宽限期） ---------- */
      const conn = await pool.getConnection();
      let removedChunks = 0;
      let removedMerged = 0;
      let bytesFreed = 0n;
      const detachedChunks = []; // {hash,size}：已删行、待删文件
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
          await conn.query('DELETE FROM cas_chunks WHERE chunk_hash = ? AND ref_count = 0', [
            c.chunk_hash,
          ]);
          bytesFreed += BigInt(c.chunk_size);
          detachedChunks.push({ hash: c.chunk_hash, path: c.storage_path, size: c.chunk_size });
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
          bytesFreed += BigInt(mb.file_size);
          detachedMerged.push({ hash: mb.merged_hash, path: mb.storage_path });
          removedMerged += 1;
        }

        await conn.commit();
      } catch (err) {
        await conn.rollback();
        conn.release();
        throw err;
      }
      conn.release();

      /* ---------- 阶段 B：提交后删物理文件（失败不影响库一致性，下次兜底） ---------- */
      let physicalDeleteFailed = 0;
      for (const d of detachedChunks) {
        try {
          await removeCasChunk(d.hash);
        } catch {
          physicalDeleteFailed += 1;
        }
      }
      for (const d of detachedMerged) {
        try {
          await removeMergedBlob(d.hash);
        } catch {
          physicalDeleteFailed += 1;
        }
      }

      /* ---------- 阶段 C：磁盘对账，删“库中无行”的物理孤儿（任意年龄，幂等） ---------- */
      const [allChunkRows] = await pool.query(
        'SELECT chunk_hash FROM cas_chunks',
      );
      const [allMergedRows] = await pool.query(
        'SELECT merged_hash FROM merged_blobs',
      );
      const orphanChunkFiles = await sweepOrphanPhysical(
        allChunkRows.map((r) => r.chunk_hash),
        'cas',
        cutoff,
      );
      const orphanMergedFiles = await sweepOrphanPhysical(
        allMergedRows.map((r) => r.merged_hash),
        'merged',
        cutoff,
      );

      // 顺带清理 merge-* 临时目录残留
      let tempDirsRemoved = 0;
      try {
        const entries = await fsp.readdir(config.storageDir, { withFileTypes: true });
        for (const e of entries) {
          if (e.isDirectory() && e.name.startsWith('merge-')) {
            await fsp.rm(path.join(config.storageDir, e.name), {
              recursive: true,
              force: true,
            });
            tempDirsRemoved += 1;
          }
        }
      } catch {
        // 存储目录不可读时忽略
      }

      res.json({
        removedChunks, // 本次删除的库行数
        removedMerged,
        bytesFreed: bytesFreed.toString(),
        physicalDeleteFailed, // 阶段 B 删盘失败数（阶段 C/下次 GC 兜底）
        orphanChunkFilesRemoved: orphanChunkFiles,
        orphanMergedFilesRemoved: orphanMergedFiles,
        tempDirsRemoved,
        minAgeSec,
      });
    } catch (err) {
      next(err);
    }
  },
);

export default router;
