/**
 * 本地磁盘存储：
 *  - 分片：storage/chunks/<fileId>/<index 补零 8 位>.part（tmp + rename 原子写入）
 *  - 合并文件：storage/merged/<fileId>__<原文件名>（流式顺序追加 + 流式哈希）
 */
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { config } from './config.js';

const CHUNK_DIR = path.join(config.storageDir, 'chunks');
const MERGED_DIR = path.join(config.storageDir, 'merged');

/** 防止路径穿越：fileId 只允许 hex */
export function safeFileId(fileId) {
  if (!/^[a-f0-9]{64}$/i.test(fileId)) {
    const err = new Error('非法文件ID');
    err.code = 'VALIDATION_ERROR';
    err.status = 400;
    throw err;
  }
  return fileId.toLowerCase();
}

/** 去除文件名中的路径分隔等危险字符，仅保留基础名 */
export function safeBaseName(name) {
  return path.basename(name).replace(/[\\/\0]+/g, '_');
}

function chunkDir(fileId) {
  return path.join(CHUNK_DIR, safeFileId(fileId));
}

export function chunkRelPath(fileId, index) {
  return path
    .join('chunks', safeFileId(fileId), `${String(index).padStart(8, '0')}.part`)
    .split(path.sep)
    .join('/');
}

function chunkAbsPath(fileId, index) {
  return path.join(config.storageDir, chunkRelPath(fileId, index));
}

export function mergedRelPath(fileId, originalName) {
  return path
    .join('merged', `${safeFileId(fileId)}__${safeBaseName(originalName)}`)
    .split(path.sep)
    .join('/');
}

/**
 * 原子写入分片：先写 .tmp，fsync 后 rename，避免半截分片被读取。
 */
export async function writeChunk(fileId, index, buffer) {
  const dir = chunkDir(fileId);
  await fsp.mkdir(dir, { recursive: true });
  const target = chunkAbsPath(fileId, index);
  const tmp = `${target}.${process.pid}.${Date.now()}.tmp`;
  await fsp.writeFile(tmp, buffer);
  await fsp.rename(tmp, target);
  return chunkRelPath(fileId, index);
}

export function chunkExists(fileId, index) {
  return fs.existsSync(chunkAbsPath(fileId, index));
}

/**
 * 按序号顺序流式合并分片，同时计算完整文件 sha256。
 * @returns {Promise<{mergedAbsolutePath:string, mergedHash:string}>}
 */
export async function mergeChunks(fileId, chunkCount, originalName) {
  const id = safeFileId(fileId);
  await fsp.mkdir(MERGED_DIR, { recursive: true });
  const rel = mergedRelPath(id, originalName);
  const abs = path.join(config.storageDir, rel);
  const tmp = `${abs}.${process.pid}.${Date.now()}.tmp`;

  const hash = createHash('sha256');
  const out = fs.createWriteStream(tmp);

  /** 等待可写流排空（背压），保证 GB 文件低内存 */
  const writeAsync = (chunk) =>
    new Promise((resolve, reject) => {
      if (out.write(chunk)) return resolve();
      out.once('drain', resolve);
      out.once('error', reject);
    });

  try {
    let totalBytes = 0;
    for (let i = 0; i < chunkCount; i += 1) {
      const part = chunkAbsPath(id, i);
      // 64KB 高水位读取，内存占用恒定
      const input = fs.createReadStream(part, { highWaterMark: 64 * 1024 });
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
    await fsp.rename(tmp, abs);
    return { mergedAbsolutePath: abs, mergedHash: hash.digest('hex'), totalBytes };
  } catch (err) {
    out.destroy();
    await fsp.rm(tmp, { force: true });
    throw err;
  }
}

/** 初始化存储目录 */
export async function ensureStorageDirs() {
  await fsp.mkdir(CHUNK_DIR, { recursive: true });
  await fsp.mkdir(MERGED_DIR, { recursive: true });
}
