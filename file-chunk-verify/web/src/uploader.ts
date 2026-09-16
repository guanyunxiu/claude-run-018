/**
 * 上传编排（真实依赖装配，CAS v3）：
 *
 *  A. 全量分片哈希已在 IndexedDB（上次算完/刷新）→ init 直接带清单：
 *       命中已完成相同文件 → 秒传（零读取/零哈希/零上传）
 *       否则按返回 hits（本任务关联 + 全局 CAS）跳过，未命中分片只读字节上传
 *  B. 否则分阶段 init（无清单）→ 启动有界流水线边算边传；
 *       全部分片哈希算出的瞬间用「带清单 init」做秒传仲裁：
 *         命中 → 中止剩余上传；未命中 → 把全局 CAS hits 补进跳过集合继续。
 *
 * 物理分片由后端按 chunkHash 内容寻址去重；删除只减引用，GC 回收零引用对象。
 */
import HashWorker from './hash.worker?worker';
import {
  ApiException,
  completeFile,
  initFile,
  linkChunk,
  submitFileHash,
  uploadChunk,
} from './api';
import { IndexedDbHashCache, type HashCacheMeta } from './idb-cache';
import {
  runPipeline,
  type HashCache,
  type HashSink,
  type ChunkReader,
  type RemoteChunks,
} from './pipeline';
import type { CompleteResponse, InitResponse } from './types';

export type UploadPhase =
  | 'init'
  | 'instant-done'
  | 'pipeline'
  | 'locking-hash'
  | 'completing'
  | 'done';

export interface UploadProgress {
  phase: UploadPhase;
  totalChunks: number;
  totalBytes: number;
  hashedChunks: number;
  hashCacheReused: number;
  hashedBytes: number;
  hashing: boolean;
  uploading: boolean;
  queuedChunks: number;
  inflightChunks: number;
  settledChunks: number;
  settledBytes: number;
  serverSkippedChunks: number;
  globalDedupChunks: number;
  newlyUploadedChunks: number;
  newlyUploadedBytes: number;
  bytesPerSec: number;
}

export interface UploadResult {
  instant: boolean;
  init: InitResponse;
  complete: CompleteResponse | null;
  serverSkippedChunks: number;
  globalDedupChunks: number;
  hashCacheReused: number;
  newlyUploadedChunks: number;
}

export interface UploadFileOptions {
  file: File;
  chunkSize: number;
  maxInflight?: number;
  uploadConcurrency?: number;
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
  return toHex(
    await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text)),
  );
}

function chunkLength(fileSize: number, chunkSize: number, index: number): number {
  return Math.min(chunkSize, fileSize - index * chunkSize);
}

async function aggregateHash(chunkHashes: string[]): Promise<string> {
  return sha256OfText(chunkHashes.join(''));
}

async function computeFileId(
  fileName: string,
  fileSize: number,
  lastModified: number,
  chunkSize: number,
): Promise<string> {
  return sha256OfText(`${fileName}:${fileSize}:${lastModified}:${chunkSize}`);
}

/* ---------------- 单 Worker 串行哈希器 ---------------- */

interface WorkerOk {
  id: number;
  index: number;
  hash: string;
}
interface WorkerErr {
  id: number;
  index: number;
  error: string;
}
type WorkerMessage = WorkerOk | WorkerErr;

class SingleHashWorker implements HashSink {
  private readonly worker: Worker;
  private readonly pending = new Map<
    number,
    { resolve: (h: string) => void; reject: (e: Error) => void }
  >();
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

class IdbCacheAdapter implements HashCache {
  constructor(
    private readonly cache: IndexedDbHashCache,
    private readonly meta: Omit<HashCacheMeta, 'cursor'>,
  ) {}
  loadPrefix() {
    return this.cache.loadPrefix({
      fileId: this.meta.fileId,
      fileName: this.meta.fileName,
      fileSize: this.meta.fileSize,
      lastModified: this.meta.lastModified,
      chunkSize: this.meta.chunkSize,
    });
  }
  put(index: number, hash: string, size: number) {
    return this.cache.putChunk(this.meta, { index, hash, size });
  }
}

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
    maxInflight = 6,
    uploadConcurrency = 3,
    signal,
    onLog,
    onProgress,
  } = options;

  if (!Number.isInteger(chunkSize) || chunkSize < 1) throw new Error('分片大小非法');
  if (maxInflight < uploadConcurrency) throw new Error('maxInflight 必须 >= uploadConcurrency');

  const log = (msg: string) => onLog?.(msg);
  const totalChunks = file.size === 0 ? 0 : Math.ceil(file.size / chunkSize);
  const sizeOf = (index: number) => chunkLength(file.size, chunkSize, index);

  const progress: UploadProgress = {
    phase: 'init',
    totalChunks,
    totalBytes: file.size,
    hashedChunks: 0,
    hashCacheReused: 0,
    hashedBytes: 0,
    hashing: false,
    uploading: false,
    queuedChunks: 0,
    inflightChunks: 0,
    settledChunks: 0,
    settledBytes: 0,
    serverSkippedChunks: 0,
    globalDedupChunks: 0,
    newlyUploadedChunks: 0,
    newlyUploadedBytes: 0,
    bytesPerSec: 0,
  };
  const emit = () => onProgress?.({ ...progress });
  emit();

  const fileId = await computeFileId(
    file.name,
    file.size,
    file.lastModified,
    chunkSize,
  );
  log(`文件 ID：${fileId}`);

  const idb = new IndexedDbHashCache();
  const worker = new SingleHashWorker();
  const cacheMeta: Omit<HashCacheMeta, 'cursor'> = {
    fileId,
    fingerprint: IndexedDbHashCache.makeFingerprint(
      file.name,
      file.size,
      file.lastModified,
      chunkSize,
    ),
    fileName: file.name,
    fileSize: file.size,
    lastModified: file.lastModified,
    chunkSize,
    totalChunks,
  };
  const cache = new IdbCacheAdapter(idb, cacheMeta);
  const reader: ChunkReader = {
    size: sizeOf,
    read: (index) =>
      file.slice(index * chunkSize, index * chunkSize + sizeOf(index)).arrayBuffer(),
  };

  try {
    /* ---------- 0) 本地全量哈希缓存：直接带清单 init（可能零哈希秒传） ---------- */
    const prefix = await cache.loadPrefix();
    const fullyCached = totalChunks > 0 && prefix.cursor >= totalChunks;
    const cachedHashes = fullyCached ? prefix.hashes : null;

    const init0 = await initFile({
      fileId,
      fileName: file.name,
      fileSize: file.size,
      chunkSize,
      totalChunks,
      fileHash: cachedHashes ? await aggregateHash(cachedHashes) : null,
      chunkHashes: cachedHashes,
    });

    if (init0.instant) {
      log(`⚡ 秒传命中（完整清单），零上传，复用合并产物：${init0.file.mergedHash}`);
      progress.phase = 'instant-done';
      progress.hashedChunks = totalChunks;
      progress.hashCacheReused = totalChunks;
      progress.settledChunks = totalChunks;
      progress.settledBytes = file.size;
      progress.serverSkippedChunks = totalChunks;
      emit();
      await idb.clear(fileId).catch(() => {});
      return {
        instant: true,
        init: init0,
        complete: null,
        serverSkippedChunks: totalChunks,
        globalDedupChunks: 0,
        hashCacheReused: totalChunks,
        newlyUploadedChunks: 0,
      };
    }

    if (cachedHashes) {
      log(
        `本地已有全量哈希：本任务已有 ${init0.uploadedChunks.length} 片，` +
          `全局 CAS 命中 ${Object.keys(init0.hits).length} 片，仅上传缺失分片`,
      );
    } else if (init0.resumed) {
      log(`检测到历史任务，本任务已存在 ${init0.uploadedChunks.length} 片`);
    } else {
      log('已注册分阶段任务（边算边传，聚合哈希算完补报/仲裁秒传）');
    }

    // 本任务关联 + 初始全局 CAS 命中
    const known = new Map<number, string>();
    for (const c of init0.uploadedChunks) known.set(c.index, c.hash);
    const skipHashes = new Set<string>(Object.values(init0.hits ?? {}));

    const uploadStart = performance.now();
    const remote: RemoteChunks = {
      known,
      skipHashes,
      upload: (index, hash, buffer) =>
        withRetry(
          () =>
            uploadChunk({
              fileId,
              index,
              hash,
              blob: new Blob([buffer]),
              signal,
            }),
          signal,
        ).then((r) => {
          if (r.dedup) log(`分片 #${index} 命中全局 CAS 去重（物理只存一份）`);
          return undefined;
        }),
      // 全局 CAS 命中：只建立本文件 file_chunks 关联（不传字节），
      // 这是本文件能通过 complete 的必要条件
      link: (index, hash) =>
        withRetry(
          () => linkChunk(fileId, index, hash, signal),
          signal,
        ).then(() => {
          log(`分片 #${index} 全局 CAS 命中，只关联不传字节`);
          return undefined;
        }),
    };

    progress.phase = 'pipeline';
    emit();

    /* ---------- 1) 有界流水线；全哈希就绪时做秒传/去重仲裁 ---------- */
    const result = await runPipeline({
      totalChunks,
      hasher: worker,
      reader,
      cache,
      remote,
      maxInflight,
      uploadConcurrency,
      signal,
      events: {
        onAllHashed: async (allHashes) => {
          const fileHash = await aggregateHash(allHashes);
          const reInit = await initFile({
            fileId,
            fileName: file.name,
            fileSize: file.size,
            chunkSize,
            totalChunks,
            fileHash,
            chunkHashes: allHashes,
          });
          if (reInit.instant) {
            log('⚡ 全量哈希算出后秒传命中，中止剩余上传');
            progress.phase = 'instant-done';
            emit();
            return 'instant';
          }
          let added = 0;
          for (const h of Object.values(reInit.hits ?? {})) {
            if (!skipHashes.has(h)) {
              skipHashes.add(h);
              added += 1;
            }
          }
          if (added > 0) log(`哈希算齐复核：新增 ${added} 个全局 CAS 命中分片`);
        },
        onTick: (c) => {
          progress.hashing = c.hashing > 0;
          progress.uploading = c.uploading > 0;
          progress.queuedChunks = c.queued;
          progress.inflightChunks = c.inflight;
          progress.hashedChunks = c.hashedChunks;
          progress.hashCacheReused = c.hashCacheReused;
          progress.settledChunks = c.settledChunks;
          progress.serverSkippedChunks = c.serverSkipped;
          progress.globalDedupChunks = c.globalDedupSkipped;
          progress.newlyUploadedChunks = c.newlyUploaded;
          // 落位字节用计数器按片近似（精确字节在结束时统一结算）
          progress.settledBytes = 0;
          let settledBytes = 0;
          for (let i = 0; i < c.settledChunks && i < totalChunks; i++) {
            settledBytes += sizeOf(Math.min(i, totalChunks - 1));
          }
          progress.settledBytes = Math.min(settledBytes, file.size);
          emit();
        },
        onUploadDone: (info) => {
          if (!info.skippedOnServer) {
            progress.newlyUploadedBytes += info.size;
            const elapsedSec = (performance.now() - uploadStart) / 1000;
            progress.bytesPerSec =
              elapsedSec > 0 ? progress.newlyUploadedBytes / elapsedSec : 0;
          }
        },
      },
    });

    // 结算落位字节（按最终哈希与已知集合判定每片归属）
    let settledBytes = 0;
    for (let i = 0; i < totalChunks; i++) settledBytes += sizeOf(i);
    progress.settledBytes = settledBytes;
    emit();

    /* ---------- 2) 秒传仲裁命中：服务端已 completed ---------- */
    if (result.instantAborted) {
      const fileHash = await aggregateHash(result.chunkHashes);
      const initFinal = await initFile({
        fileId,
        fileName: file.name,
        fileSize: file.size,
        chunkSize,
        totalChunks,
        fileHash,
        chunkHashes: result.chunkHashes,
      });
      log(`秒传完成，复用合并产物：${initFinal.file.mergedHash}`);
      await idb.clear(fileId).catch(() => {});
      return {
        instant: true,
        init: initFinal,
        complete: null,
        serverSkippedChunks: result.counters.serverSkipped,
        globalDedupChunks: result.counters.globalDedupSkipped,
        hashCacheReused: result.counters.hashCacheReused,
        newlyUploadedChunks: result.counters.newlyUploaded,
      };
    }

    log(
      `流水线完成：复用本地哈希 ${result.counters.hashCacheReused} 片，` +
        `本任务跳过 ${result.counters.serverSkipped} 片，` +
        `全局 CAS 命中 ${result.counters.globalDedupSkipped} 片，` +
        `本次新传 ${result.counters.newlyUploaded} 片`,
    );

    /* ---------- 3) 补报锁定聚合哈希 → 服务端强校验 + 合并 ---------- */
    progress.phase = 'locking-hash';
    emit();
    const fileHash = await aggregateHash(result.chunkHashes);
    const hashResp = await submitFileHash(fileId, fileHash);
    log(
      hashResp.changed
        ? `聚合哈希已补报锁定：${fileHash}`
        : `聚合哈希一致（幂等）：${fileHash}`,
    );

    progress.phase = 'completing';
    emit();
    const complete = await completeFile(fileId);
    log(`服务端强校验通过，完整文件 SHA-256：${complete.mergedHash}`);
    progress.phase = 'done';
    emit();

    await idb.clear(fileId).catch(() => {});

    return {
      instant: false,
      init: init0,
      complete,
      serverSkippedChunks: result.counters.serverSkipped,
      globalDedupChunks: result.counters.globalDedupSkipped,
      hashCacheReused: result.counters.hashCacheReused,
      newlyUploadedChunks: result.counters.newlyUploaded,
    };
  } finally {
    worker.terminate();
    await idb.close().catch(() => {});
  }
}
