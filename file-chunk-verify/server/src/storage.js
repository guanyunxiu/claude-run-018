/**
 * 内容寻址（CAS）本地磁盘存储：
 *   分片：storage/cas/<hash 前2位>/<chunkHash>.part        —— 同内容全局只存一份
 *   合并：storage/merged/<hash 前2位>/<mergedHash>.bin     —— 秒传可共享
 *
 * 物理文件均为 tmp + rename 原子写入；路径由哈希推导，天然防目录穿越。
 */
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { config } from './config.js';

const CAS_DIR = path.join(config.storageDir, 'cas');
const MERGED_DIR = path.join(config.storageDir, 'merged');

const HASH_RE = /^[a-f0-9]{64}$/i;

/** 校验 hex 哈希，防止路径穿越；统一小写 */
export function safeHash(hash, label = 'hash') {
  if (typeof hash !== 'string' || !HASH_RE.test(hash)) {
    const err = new Error(`非法${label}`);
    err.code = 'VALIDATION_ERROR';
    err.status = 400;
    throw err;
  }
  return hash.toLowerCase();
}

/** fileId 也是 64 位 hex（用于兼容旧接口入参校验） */
export function safeFileId(fileId) {
  return safeHash(fileId, '文件ID');
}

/** 去除文件名中的路径分隔等危险字符 */
export function safeBaseName(name) {
  return path.basename(name).replace(/[\\/\0]+/g, '_');
}

/* ---------------- CAS 分片 ---------------- */

/** CAS 分片相对路径：cas/<前2>/<hash>.part */
export function casChunkRelPath(chunkHash) {
  const h = safeHash(chunkHash, '分片哈希');
  return path.join('cas', h.slice(0, 2), `${h}.part`).split(path.sep).join('/');
}

function casChunkAbsPath(chunkHash) {
  return path.join(config.storageDir, casChunkRelPath(chunkHash));
}

export function casChunkExists(chunkHash) {
  return fs.existsSync(casChunkAbsPath(safeHash(chunkHash)));
}

/** 原子写入一个 CAS 分片（tmp + rename；同名文件已存在则保留） */
export async function writeCasChunk(chunkHash, buffer) {
  const h = safeHash(chunkHash, '分片哈希');
  const abs = casChunkAbsPath(h);
  await fsp.mkdir(path.dirname(abs), { recursive: true });
  // 并发首传：临时名带 pid/随机串，互不覆盖；rename 到同目标由 FS 保证原子
  const tmp = `${abs}.${process.pid}.${Math.random().toString(36).slice(2, 8)}.tmp`;
  await fsp.writeFile(tmp, buffer);
  await fsp.rename(tmp, abs);
  return casChunkRelPath(h);
}

/** 删除一个 CAS 物理分片（GC / 引用归零时） */
export async function removeCasChunk(chunkHash) {
  const abs = casChunkAbsPath(safeHash(chunkHash));
  await fsp.rm(abs, { force: true });
}

/** 读取 CAS 分片字节（complete 重算/抽检） */
export function readCasChunk(chunkHash) {
  return fsp.readFile(casChunkAbsPath(safeHash(chunkHash)));
}

/**
 * 校验某个 CAS 物理分片存在且字节数与记录一致。
 * 用于「库行存在但磁盘可能缺失」的自愈判断（GC 崩溃后、去重命中时）。
 */
export async function casChunkPhysicalOk(chunkHash, expectedSize) {
  try {
    const st = await fsp.stat(casChunkAbsPath(safeHash(chunkHash)));
    if (expectedSize !== undefined && BigInt(st.size) !== BigInt(expectedSize)) {
      return false;
    }
    return st.size >= 0;
  } catch {
    return false;
  }
}

/* ---------------- 内容寻址合并产物 ---------------- */

export function mergedBlobRelPath(mergedHash) {
  const h = safeHash(mergedHash, '合并哈希');
  return path.join('merged', h.slice(0, 2), `${h}.bin`).split(path.sep).join('/');
}

function mergedBlobAbsPath(mergedHash) {
  return path.join(config.storageDir, mergedBlobRelPath(mergedHash));
}

export function mergedBlobExists(mergedHash) {
  return fs.existsSync(mergedBlobAbsPath(safeHash(mergedHash)));
}

/**
 * 按序流式读取 CAS 分片合并，同时计算完整文件 sha256。
 * @returns {Promise<{relPath:string, absPath:string, mergedHash:string, totalBytes:number}>}
 */
export async function mergeCasChunks(orderedChunkHashes, expectedMergedHash = null) {
  if (orderedChunkHashes.length === 0) {
    // 空文件：写一个 0 字节内容寻址文件
    const emptyHash = createHash('sha256').digest('hex');
    return writeMergedBlob(emptyHash, Buffer.alloc(0));
  }

  const hash = createHash('sha256');
  const tmpOut = path.join(
    await fsp.mkdtemp(path.join(config.storageDir, 'merge-')),
    'out.tmp',
  );
  const out = fs.createWriteStream(tmpOut);
  const writeAsync = (data) =>
    new Promise((resolve, reject) => {
      if (out.write(data)) return resolve();
      out.once('drain', resolve);
      out.once('error', reject);
    });

  let totalBytes = 0;
  try {
    for (const ch of orderedChunkHashes) {
      const input = fs.createReadStream(casChunkAbsPath(ch), {
        highWaterMark: 64 * 1024,
      });
      for await (const piece of input) {
        hash.update(piece);
        totalBytes += piece.length;
        await writeAsync(piece);
      }
    }
    await new Promise((resolve, reject) => {
      out.once('finish', resolve);
      out.once('error', reject);
      out.end();
    });
    const mergedHash = hash.digest('hex');
    if (expectedMergedHash && expectedMergedHash !== mergedHash) {
      throw Object.assign(new Error('合并产物哈希与预期不符'), {
        code: 'MERGED_HASH_MISMATCH',
      });
    }
    const result = await installMergedBlob(mergedHash, tmpOut);
    return { ...result, totalBytes };
  } finally {
    out.destroy();
    await fsp.rm(tmpOut, { force: true }).catch(() => {});
    await fsp.rm(path.dirname(tmpOut), { recursive: true, force: true }).catch(() => {});
  }
}

async function writeMergedBlob(mergedHash, buffer) {
  const h = safeHash(mergedHash, '合并哈希');
  const rel = mergedBlobRelPath(h);
  const abs = path.join(config.storageDir, rel);
  await fsp.mkdir(path.dirname(abs), { recursive: true });
  if (!fs.existsSync(abs)) {
    const tmp = `${abs}.${process.pid}.${Date.now()}.tmp`;
    await fsp.writeFile(tmp, buffer);
    await fsp.rename(tmp, abs);
  }
  return { relPath: rel, absPath: abs, mergedHash: h };
}

/** 把已写好的合并临时文件原子安装到内容寻址路径（已存在则复用） */
async function installMergedBlob(mergedHash, tmpAbs) {
  const rel = mergedBlobRelPath(mergedHash);
  const abs = path.join(config.storageDir, rel);
  await fsp.mkdir(path.dirname(abs), { recursive: true });
  if (fs.existsSync(abs)) {
    await fsp.rm(tmpAbs, { force: true });
  } else {
    await fsp.rename(tmpAbs, abs);
  }
  return { relPath: rel, absPath: abs, mergedHash: safeHash(mergedHash) };
}

export async function removeMergedBlob(mergedHash) {
  const abs = mergedBlobAbsPath(safeHash(mergedHash));
  await fsp.rm(abs, { force: true });
}

/** 供下载/校验流式读取合并产物 */
export function createMergedReadStream(mergedHash) {
  return fs.createReadStream(mergedBlobAbsPath(safeHash(mergedHash)));
}

export function mergedBlobAbs(mergedHash) {
  return mergedBlobAbsPath(safeHash(mergedHash));
}

/** 初始化存储目录 */
export async function ensureStorageDirs() {
  await fsp.mkdir(CAS_DIR, { recursive: true });
  await fsp.mkdir(MERGED_DIR, { recursive: true });
}

/* ---------------- 磁盘对账（GC 崩溃恢复用） ---------------- */

async function listFilesRecursive(root) {
  let entries;
  try {
    entries = await fsp.readdir(root, { withFileTypes: true });
  } catch {
    return [];
  }
  const out = [];
  for (const e of entries) {
    const abs = path.join(root, e.name);
    if (e.isDirectory()) {
      out.push(...(await listFilesRecursive(abs)));
    } else if (e.isFile()) {
      out.push(abs);
    }
  }
  return out;
}

/** 列出磁盘上全部 CAS 分片：{hash, abs, mtimeMs}，忽略 .tmp 等临时文件 */
async function listPhysicalCasFiles() {
  const out = [];
  for (const abs of await listFilesRecursive(CAS_DIR)) {
    const m = /^([a-f0-9]{64})\.part$/.exec(path.basename(abs));
    if (!m) continue;
    let mtimeMs = 0;
    try {
      mtimeMs = (await fsp.stat(abs)).mtimeMs;
    } catch {
      continue;
    }
    out.push({ hash: m[1], abs, mtimeMs });
  }
  return out;
}

/** 列出磁盘上全部合并产物：{hash, abs, mtimeMs} */
async function listPhysicalMergedFiles() {
  const out = [];
  for (const abs of await listFilesRecursive(MERGED_DIR)) {
    const m = /^([a-f0-9]{64})\.bin$/.exec(path.basename(abs));
    if (!m) continue;
    let mtimeMs = 0;
    try {
      mtimeMs = (await fsp.stat(abs)).mtimeMs;
    } catch {
      continue;
    }
    out.push({ hash: m[1], abs, mtimeMs });
  }
  return out;
}

/** 兼容旧调用 */
export async function listPhysicalCasHashes() {
  return (await listPhysicalCasFiles()).map((f) => f.hash);
}
export async function listPhysicalMergedHashes() {
  return (await listPhysicalMergedFiles()).map((f) => f.hash);
}

/**
 * 删除“库里没有对应行”的物理孤儿文件（GC 提交后、删盘前崩溃的残留）。
 * 仅删除 mtime 早于 cutoff 的文件，避免误删“物理已原子安装、DB 行尚未提交”
 * 的首传分片；tmp 文件不在内容寻址命名内，天然不会被扫到。
 * @returns 实际删除的文件数
 */
export async function sweepOrphanPhysical(existingHashes, kind, cutoff = null) {
  const known = new Set(existingHashes.map((h) => safeHash(h)));
  const files =
    kind === 'cas' ? await listPhysicalCasFiles() : await listPhysicalMergedFiles();
  let removed = 0;
  for (const f of files) {
    if (known.has(f.hash)) continue;
    if (cutoff && f.mtimeMs >= cutoff.getTime()) continue; // 太新，可能正在首传
    if (kind === 'cas') await removeCasChunk(f.hash);
    else await removeMergedBlob(f.hash);
    removed += 1;
  }
  return removed;
}
