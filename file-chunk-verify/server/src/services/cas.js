/**
 * CAS 内容分片的跨机器一致操作（对象存储 + DB + 分布式锁）。
 *
 * ensureCasChunk 的语义：保证「对象存在 + cas_chunks 有一行」，并**恰好增加一个引用**。
 * 双机并发首传同一 hash 的正确性来自三层：
 *   1. 分布式锁 cas:<hash>（Redis；测试为进程内共享锁）串行化同一内容的首传/自愈；
 *   2. 对象存储条件写 putIfAbsent（S3 IfNoneMatch:'*' / local 硬链接），只保留一份正确内容；
 *   3. DB 唯一键 + INSERT ... ON DUPLICATE KEY ref_count=ref_count+1，行只有一条、计数精确。
 *
 * 调用方负责“本文件该序号是否已关联同哈希”的幂等：若已关联，则把这里多加的引用减回去
 * （见 routes/files.js 上传/link 路由）。这样“同文件重复传同片”不会虚增引用。
 *
 * 库有行、对象缺失（历史 GC 中途崩溃遗留的幽灵）时：必须有字节才能自愈重写；
 * 纯 /link（无字节）遇到幽灵会抛 CHUNK_FILE_MISSING，前端据此回退字节上传。
 */
import {
  writeCasChunkIfAbsent,
  casChunkRelPath,
  casChunkPhysicalOk,
} from '../storage.js';

export class CasServiceError extends Error {
  constructor(status, code, message, details) {
    super(message);
    this.status = status;
    this.code = code;
    if (details !== undefined) this.details = details;
  }
}

/**
 * @param {object} p
 * @param {import('mysql2/promise').Connection} p.conn 已 beginTransaction 的连接（行锁在其内）
 * @param {string} p.hash 内容哈希
 * @param {Buffer|null} p.body 分片字节；纯只关联且对象缺失时传 null
 * @param {object} p.locker 分布式锁（可为 null）
 * @returns {Promise<{existed:boolean, healed:boolean, dedup:boolean}>}
 *   existed 行是否此前已存在；healed 是否重写了缺失对象；dedup 是否为纯去重命中
 */
export async function ensureCasChunk({ conn, hash, body, locker }) {
  const lease = locker ? await locker.acquire(`cas:${hash}`, { waitMs: 30_000 }) : null;
  try {
    const relPath = casChunkRelPath(hash);

    // 锁内读取权威状态
    let [rows] = await conn.query(
      'SELECT * FROM cas_chunks WHERE chunk_hash = ? FOR UPDATE',
      [hash],
    );
    let row = rows[0];

    if (row) {
      const ok = await casChunkPhysicalOk(hash, row.chunk_size);
      if (ok) {
        // 纯去重命中：对象与行都健康，引用 +1
        await conn.query(
          'UPDATE cas_chunks SET ref_count = ref_count + 1 WHERE chunk_hash = ?',
          [hash],
        );
        return { existed: true, healed: false, dedup: true };
      }
      // 幽灵行：必须用字节自愈
      if (!body) {
        throw new CasServiceError(409, 'CHUNK_FILE_MISSING', 'CAS 物理分片缺失或损坏，需要字节上传', {
          index: undefined,
          hash,
        });
      }
      await writeCasChunkIfAbsent(hash, body);
      await conn.query(
        `UPDATE cas_chunks
            SET chunk_size = ?, storage_path = ?, ref_count = ref_count + 1
          WHERE chunk_hash = ?`,
        [body.length, relPath, hash],
      );
      return { existed: true, healed: true, dedup: false };
    }

    // 行不存在 → 首传（双机可能同时到此）。必须带字节。
    if (!body) {
      throw new CasServiceError(
        409,
        'CAS_CHUNK_NOT_FOUND',
        '该分片内容在全局 CAS 中不存在，不能只关联，请走分片上传',
        { hash },
      );
    }
    // 条件写对象：双机并发只保留一份（内容相同，谁写成都一样）
    await writeCasChunkIfAbsent(hash, body);

    // 建行竞争：INSERT IGNORE 只有一方真正插入（ref_count=0），另一方忽略。
    // 随后无论哪方都恰好 +1，保证“每个调用贡献一个引用”，双机首传不会翻倍。
    await conn.query(
      `INSERT IGNORE INTO cas_chunks (chunk_hash, chunk_size, storage_path, ref_count)
       VALUES (?, ?, ?, 0)`,
      [hash, body.length, relPath],
    );
    await conn.query(
      'UPDATE cas_chunks SET ref_count = ref_count + 1 WHERE chunk_hash = ?',
      [hash],
    );

    return { existed: false, healed: false, dedup: false };
  } finally {
    if (lease) await lease.release();
  }
}

/** 撤销一次“多加的引用”（同文件同序号同哈希的幂等上传/关联） */
export async function releaseOneRef(conn, hash) {
  await conn.query(
    'UPDATE cas_chunks SET ref_count = GREATEST(ref_count - 1, 0) WHERE chunk_hash = ?',
    [hash],
  );
}
