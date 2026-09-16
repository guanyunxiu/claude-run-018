/**
 * 存储外观层：历史代码与路由使用这里的函数；内部全部委托给可替换的 ObjectStore
 * （local 磁盘或 S3/MinIO）。对象 key 一律按内容哈希拼，绝不使用 fileId。
 *
 * 物理布局（对象 key / 本地相对路径一致）：
 *   cas/<hash 前2>/<hash>.part
 *   merged/<hash 前2>/<hash>.bin
 *   tmp/<uuid>.tmp
 */
import { createHash, randomUUID } from 'node:crypto';
import path from 'node:path';
import { config } from './config.js';
import { objectStore } from './store/index.js';
import { casKey, mergedKey, tmpKey, safeHash as baseSafeHash, CAS_PREFIX, MERGED_PREFIX } from './store/base.js';

/* 兼容路由层已有的 safeHash/safeBaseName 校验入口 */
export function safeHash(hash, label) {
  return baseSafeHash(hash, label);
}
export function safeFileId(id) {
  return baseSafeHash(id, '文件ID');
}
export function safeBaseName(name) {
  return path.basename(name).replace(/[\\/\0]+/g, '_');
}

/* ---------------- CAS 分片 ---------------- */

export function casChunkRelPath(chunkHash) {
  return casKey(chunkHash);
}

export async function writeCasChunk(chunkHash, buffer) {
  const h = safeHash(chunkHash);
  await objectStore().put(casKey(h), buffer);
  return casKey(h);
}

/** 仅当不存在时写入；并发首传时只有一个 written=true */
export async function writeCasChunkIfAbsent(chunkHash, buffer) {
  const h = safeHash(chunkHash);
  return objectStore().putIfAbsent(casKey(h), buffer);
}

export async function removeCasChunk(chunkHash) {
  await objectStore().delete(casKey(safeHash(chunkHash)));
}

export async function readCasChunk(chunkHash) {
  return objectStore().getBuffer(casKey(safeHash(chunkHash)));
}

export async function casChunkExists(chunkHash) {
  const st = await objectStore().stat(casKey(safeHash(chunkHash)));
  return st.exists;
}

/** 库行存在时的物理可读/大小校验（去重命中、/link、秒传都要用） */
export async function casChunkPhysicalOk(chunkHash, expectedSize) {
  try {
    const st = await objectStore().stat(casKey(safeHash(chunkHash)));
    if (!st.exists) return false;
    if (expectedSize !== undefined && BigInt(st.size) !== BigInt(expectedSize)) return false;
    return true;
  } catch {
    return false;
  }
}

/* ---------------- 内容寻址合并产物 ---------------- */

export function mergedBlobRelPath(mergedHash) {
  return mergedKey(safeHash(mergedHash));
}

export async function mergedBlobExists(mergedHash) {
  const st = await objectStore().stat(mergedKey(safeHash(mergedHash)));
  return st.exists;
}

/** 合并产物存在且大小匹配（秒传捐赠者校验/下载前校验，走对象存储而非本机磁盘） */
export async function mergedBlobPhysicalOk(mergedHash, expectedSize) {
  try {
    const st = await objectStore().stat(mergedKey(safeHash(mergedHash)));
    if (!st.exists) return false;
    if (expectedSize !== undefined && BigInt(st.size) !== BigInt(expectedSize)) return false;
    return true;
  } catch {
    return false;
  }
}

/**
 * 按序读取 CAS 分片，在对象存储侧拼接合并，并计算完整文件 sha256，
 * 最后“条件安装”到内容寻址 key：
 *   1) compose 到 tmp/<uuid>（半成品绝不会出现在 merged/）
 *   2) 流式读取 tmp 计算哈希与大小
 *   3) copy-if-absent 到 merged/<xx>/<hash>.bin；已存在（另一台机器刚合并完）则复用
 *   4) 删除 tmp
 * 任何阶段崩溃：tmp 残留由 GC 清理，merged 目标要么不存在、要么完整，重试可收敛。
 */
export async function mergeCasChunks(orderedChunkHashes) {
  const store = objectStore();

  if (orderedChunkHashes.length === 0) {
    const emptyHash = createHash('sha256').digest('hex');
    const key = mergedKey(emptyHash);
    await store.putIfAbsent(key, Buffer.alloc(0));
    return { relPath: key, mergedHash: emptyHash, totalBytes: 0 };
  }

  const sources = orderedChunkHashes.map((h) => casKey(safeHash(h)));
  const tmp = tmpKey(`merge-${randomUUID()}.tmp`);

  try {
    await store.compose(sources, tmp);

    // 流式计算合并内容哈希（不整文件进内存）
    const hash = createHash('sha256');
    let totalBytes = 0n;
    await new Promise((resolve, reject) => {
      const stream = store.getStream(tmp);
      stream.on('data', (d) => {
        totalBytes += BigInt(d.length);
        hash.update(d);
      });
      stream.on('error', reject);
      stream.on('end', resolve);
    });
    const mergedHash = hash.digest('hex');
    const finalKey = mergedKey(mergedHash);

    // 条件安装：并发 complete 时只有一方真正写入 merged 目标
    await store.copy(tmp, finalKey, { ifAbsent: true });

    return { relPath: finalKey, mergedHash, totalBytes: Number(totalBytes) };
  } finally {
    await store.delete(tmp).catch(() => {});
  }
}

export async function removeMergedBlob(mergedHash) {
  await objectStore().delete(mergedKey(safeHash(mergedHash)));
}

export function createMergedReadStream(mergedHash) {
  return objectStore().getStream(mergedKey(safeHash(mergedHash)));
}

export function mergedBlobAbs(mergedHash) {
  // local 模式返回绝对路径（下载路由 Content-Length/access 用）；
  // s3 模式返回合成 key（下载路由不依赖文件系统）。
  const key = mergedKey(safeHash(mergedHash));
  return objectStore().kind === 'local' ? path.join(config.storageDir, key) : key;
}

/* ---------------- 磁盘/对象对账（GC） ---------------- */

export async function listPhysicalCasHashes() {
  const all = await objectStore().listByPrefix(`${CAS_PREFIX}/`);
  return Object.keys(all)
    .map((k) => /\/([a-f0-9]{64})\.part$/.exec(k)?.[1])
    .filter(Boolean);
}

export async function listPhysicalMergedHashes() {
  const all = await objectStore().listByPrefix(`${MERGED_PREFIX}/`);
  return Object.keys(all)
    .map((k) => /\/([a-f0-9]{64})\.bin$/.exec(k)?.[1])
    .filter(Boolean);
}

async function listPhysicalWithMeta(prefix) {
  const store = objectStore();
  if (store.kind === 's3') {
    // S3 ListObjects 无 mtime：返回 size，GC 宽限由 DB 行 created_at + 调用方控制
    const sizes = await store.listByPrefix(prefix);
    return Object.entries(sizes).map(([key, size]) => ({ key, size, mtimeMs: 0 }));
  }
  // local：带 mtime
  return Object.values(await store.listByPrefixWithMeta(prefix));
}

/**
 * 删除“库里没有对应行”的物理孤儿（GC 阶段 C）。
 * S3 下无法从 ListObjects 拿 mtime，采用保守策略：只删不在 knownHashes 中的对象；
 * 调用方保证这些 key 不属于任何进行中的首传（内容寻址 + 唯一键 + DB 行先于可见）。
 */
export async function sweepOrphanPhysical(existingHashes, kind, cutoff = null) {
  const store = objectStore();
  const known = new Set(existingHashes.map((h) => safeHash(h)));
  const items = await listPhysicalWithMeta(kind === 'cas' ? `${CAS_PREFIX}/` : `${MERGED_PREFIX}/`);
  let removed = 0;
  for (const it of items) {
    const m = kind === 'cas'
      ? /\/([a-f0-9]{64})\.part$/.exec(it.key)
      : /\/([a-f0-9]{64})\.bin$/.exec(it.key);
    if (!m) continue;
    const hash = m[1];
    if (known.has(hash)) continue;
    if (store.kind === 'local' && cutoff && it.mtimeMs && it.mtimeMs >= cutoff.getTime()) continue;
    await store.delete(it.key);
    removed += 1;
  }
  return removed;
}

export async function ensureStorageDirs() {
  await objectStore().init();
}
