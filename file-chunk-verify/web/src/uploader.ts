/**
 * 分片上传编排：
 *  1. 顺序从 File 切片（Blob.slice，不产生内存副本）；
 *  2. 每个切片 arrayBuffer() 后以 Transferable 交给【唯一】的 WebWorker 算 SHA-256；
 *  3. 由全分片哈希计算聚合哈希 fileHash；
 *  4. 调 /init 获取服务端已上传分片（断点续传），序号+哈希命中即跳过；
 *  5. 未上传分片用有界并发池（默认 3）上传，网络/5xx 自动重试 2 次；
 *  6. 全部完成后调 /complete，由服务端重算哈希并聚合校验、合并文件。
 *
 * 同一时刻 Worker 中只保留一个分片 buffer，上传分片随用随切，
 * 即便文件是 GB 级，浏览器内存占用也基本恒定。
 */
import HashWorker from './hash.worker?worker';
import {
  ApiException,
  completeFile,
  initFile,
  uploadChunk,
} from './api';
import type { CompleteResponse, InitResponse } from './types';

export type UploadPhase =
  | 'hashing'
  | 'init'
  | 'uploading'
  | 'completing'
  | 'done';

export interface UploadProgress {
  phase: UploadPhase;
  totalChunks: number;
  totalBytes: number;
  /** 已完成哈希的分片数 / 字节数 */
  hashedChunks: number;
  hashedBytes: number;
  /** 已确认落位的分片（服务端已有 + 本次新传） */
  settledChunks: number;
  /** 已确认落位字节，用于整体百分比 */
  settledBytes: number;
  /** 本次新上传字节 */
  newlyUploadedBytes: number;
  /** 本次跳过（断点续传命中）分片数 */
  skippedChunks: number;
  /** 上传阶段估算速度（字节/秒，含 0） */
  bytesPerSec: number;
}

export interface UploadResult {
  init: InitResponse;
  complete: CompleteResponse;
  skippedChunks: number;
}

export interface UploadFileOptions {
  file: File;
  chunkSize: number;
  concurrency?: number;
  signal?: AbortSignal;
  onLog?: (message: string) => void;
  onProgress?: (progress: UploadProgress) => void;
}

function toHex(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let hex = '';
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }
  return hex;
}

async function sha256OfText(text: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(text),
  );
  return toHex(digest);
}

/** 单个分片的期望大小（最后一片可能更小） */
function chunkLength(fileSize: number, chunkSize: number, index: number): number {
  const start = index * chunkSize;
  return Math.min(chunkSize, fileSize - start);
}

/** 聚合哈希：sha256(concat(各分片 sha256 的 hex))，与后端 aggregateHashHex 严格一致 */
async function aggregateHash(chunkHashes: string[]): Promise<string> {
  return sha256OfText(chunkHashes.join(''));
}

/**
 * 文件 ID：同一文件（文件名 + 大小 + 最后修改时间）在同一分片策略下稳定，
 * 作为断点续传的会话标识。
 */
async function computeFileId(
  fileName: string,
  fileSize: number,
  lastModified: number,
  chunkSize: number,
): Promise<string> {
  return sha256OfText(`${fileName}:${fileSize}:${lastModified}:${chunkSize}`);
}

/* ---------------- 单 Worker 的 Promise 化封装 ---------------- */

interface WorkerOk {
  id: number;
  index: number;
  hash: string;
  size: number;
}
interface WorkerErr {
  id: number;
  index: number;
  error: string;
}
type WorkerMessage = WorkerOk | WorkerErr;

interface Pending {
  resolve: (hash: string) => void;
  reject: (err: Error) => void;
}

class SingleHashWorker {
  private readonly worker: Worker;
  private readonly pending = new Map<number, Pending>();
  private nextId = 1;

  constructor() {
    this.worker = new HashWorker();
    this.worker.onmessage = (ev: MessageEvent<WorkerMessage>) => {
      const msg = ev.data;
      const p = this.pending.get(msg.id);
      if (!p) return;
      this.pending.delete(msg.id);
      if ('error' in msg) {
        p.reject(new Error(`Worker 计算分片 #${msg.index} 哈希失败：${msg.error}`));
      } else {
        p.resolve(msg.hash);
      }
    };
    this.worker.onerror = (ev) => {
      const err = new Error(`HashWorker 异常：${ev.message}`);
      for (const [, p] of this.pending) p.reject(err);
      this.pending.clear();
    };
  }

  /** 计算 buffer 的 SHA-256；buffer 以 Transferable 所有权转移给 Worker（零拷贝） */
  hash(index: number, buffer: ArrayBuffer): Promise<string> {
    const id = this.nextId++;
    return new Promise<string>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, index, buffer }, [buffer]);
    });
  }

  terminate(): void {
    const err = new Error('Worker 已终止');
    for (const [, p] of this.pending) p.reject(err);
    this.pending.clear();
    this.worker.terminate();
  }
}

/* ---------------- 有界并发任务池 ---------------- */

async function runPool<T>(
  items: T[],
  concurrency: number,
  signal: AbortSignal | undefined,
  workerFn: (item: T) => Promise<void>,
): Promise<void> {
  let cursor = 0;
  const runners = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (cursor < items.length) {
      const current = cursor++;
      if (signal?.aborted) {
        throw new DOMException('用户取消上传', 'AbortError');
      }
      await workerFn(items[current]);
    }
  });
  await Promise.all(runners);
}

/** 网络错误 / 5xx 自动重试；4xx（如哈希不符）直接抛出 */
async function withRetry<T>(
  fn: () => Promise<T>,
  signal: AbortSignal | undefined,
  attempts = 3,
): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    if (signal?.aborted) throw new DOMException('用户取消上传', 'AbortError');
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (err instanceof DOMException && err.name === 'AbortError') throw err;
      const retryable =
        !(err instanceof ApiException) || (err.status >= 500 && err.status < 600);
      if (!retryable || i === attempts - 1) throw err;
      await new Promise((r) => setTimeout(r, 400 * 2 ** i));
    }
  }
  throw lastErr;
}

/* ---------------- 主流程 ---------------- */

export async function uploadFileInChunks(
  options: UploadFileOptions,
): Promise<UploadResult> {
  const {
    file,
    chunkSize,
    concurrency = 3,
    signal,
    onLog,
    onProgress,
  } = options;

  if (!Number.isInteger(chunkSize) || chunkSize < 1) {
    throw new Error('分片大小非法');
  }

  const log = (msg: string) => onLog?.(msg);
  const totalChunks = file.size === 0 ? 0 : Math.ceil(file.size / chunkSize);

  const progress: UploadProgress = {
    phase: 'hashing',
    totalChunks,
    totalBytes: file.size,
    hashedChunks: 0,
    hashedBytes: 0,
    settledChunks: 0,
    settledBytes: 0,
    newlyUploadedBytes: 0,
    skippedChunks: 0,
    bytesPerSec: 0,
  };
  const emit = () => onProgress?.({ ...progress });
  emit();

  const worker = new SingleHashWorker();

  try {
    /* 阶段 1：单 Worker 顺序扫描全分片哈希（同时也完成了一次本地“读校验”） */
    log(`开始计算 ${totalChunks} 个分片的 SHA-256（Worker 线程）…`);
    const chunkHashes: string[] = new Array(totalChunks);
    for (let i = 0; i < totalChunks; i++) {
      if (signal?.aborted) throw new DOMException('用户取消上传', 'AbortError');
      const start = i * chunkSize;
      const blob = file.slice(start, start + chunkLength(file.size, chunkSize, i));
      const buffer = await blob.arrayBuffer();
      chunkHashes[i] = await worker.hash(i, buffer);
      progress.hashedChunks = i + 1;
      progress.hashedBytes = Math.min(start + blob.size, file.size);
      emit();
    }
    const fileHash = await aggregateHash(chunkHashes);
    log(`分片哈希完成，聚合哈希：${fileHash}`);

    /* 阶段 2：init 注册/恢复，拿到已上传分片清单 */
    progress.phase = 'init';
    emit();
    const fileId = await computeFileId(
      file.name,
      file.size,
      file.lastModified,
      chunkSize,
    );
    log(`文件 ID：${fileId}`);

    const init = await initFile({
      fileId,
      fileName: file.name,
      fileSize: file.size,
      chunkSize,
      totalChunks,
      fileHash,
    });
    log(
      init.resumed
        ? `检测到历史任务，服务端已存在 ${init.uploadedChunks.length} 个分片`
        : '新任务已在服务端注册',
    );

    const uploadedByIndex = new Map<number, string>();
    for (const c of init.uploadedChunks) uploadedByIndex.set(c.index, c.hash);

    /* 序号 + 哈希同时命中才算已上传（内容变化会被识别为需要重传） */
    const pendingIndexes: number[] = [];
    for (let i = 0; i < totalChunks; i++) {
      if (uploadedByIndex.get(i) === chunkHashes[i]) {
        progress.skippedChunks += 1;
        progress.settledChunks += 1;
        progress.settledBytes += chunkLength(file.size, chunkSize, i);
      } else {
        pendingIndexes.push(i);
      }
    }
    log(`断点续传跳过 ${progress.skippedChunks} 片，待上传 ${pendingIndexes.length} 片`);
    emit();

    /* 阶段 3：有界并发上传缺失分片（二进制 body） */
    progress.phase = 'uploading';
    emit();
    const uploadStart = performance.now();

    await runPool(pendingIndexes, concurrency, signal, async (index) => {
      const size = chunkLength(file.size, chunkSize, index);
      const blob = file.slice(index * chunkSize, index * chunkSize + size);
      await withRetry(
        () =>
          uploadChunk({
            fileId,
            index,
            hash: chunkHashes[index],
            blob,
            signal,
          }),
        signal,
      );
      progress.newlyUploadedBytes += size;
      progress.settledBytes += size;
      progress.settledChunks += 1;
      const elapsedSec = (performance.now() - uploadStart) / 1000;
      progress.bytesPerSec =
        elapsedSec > 0 ? progress.newlyUploadedBytes / elapsedSec : 0;
      emit();
    });

    /* 阶段 4：服务端全量重算 + 聚合校验 + 合并 */
    progress.phase = 'completing';
    emit();
    log('全部分片已就位，请求服务端聚合校验并合并…');
    const complete = await completeFile(fileId);
    log(`服务端校验通过，完整文件 SHA-256：${complete.mergedHash}`);

    progress.phase = 'done';
    emit();

    return { init, complete, skippedChunks: progress.skippedChunks };
  } finally {
    worker.terminate();
  }
}
