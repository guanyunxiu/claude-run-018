/**
 * 有界「哈希 → 上传」流水线核心（与 DOM/File/网络解耦，依赖注入，便于自动化测试）。
 *
 * 模型：
 *   - 一个哈希器（生产环境是单 WebWorker）串行产出 hash；
 *   - 哈希产出后该分片立即进入上传队列，不必等其它分片；
 *   - 有界槽位：同时 inflight（哈希中 + 排队待传 + 上传中）分片数 ≤ maxInflight。
 *     每个在途分片只保留一份字节，内存占用 ≈ maxInflight × chunkSize，与文件大小无关；
 *   - uploadConcurrency 个上传消费者，真正的 HTTP 在途请求数 ≤ uploadConcurrency；
 *   - 每算出一片就经 HashCache 持久化（IndexedDB），刷新/取消后从连续游标续算。
 */

export interface HashedChunkInfo {
  index: number;
  hash: string;
  size: number;
}

export interface HashSink {
  /** 串行哈希器：同一时刻只处理一个分片（对应单 WebWorker） */
  hash(index: number, buffer: ArrayBuffer): Promise<string>;
}

export interface ChunkReader {
  /** 读取一个分片字节（生产环境是 file.slice(...).arrayBuffer()） */
  read(index: number): Promise<ArrayBuffer>;
  /** 分片字节数（最后一片可能更小） */
  size(index: number): number;
}

export interface HashCache {
  /** 加载从 0 开始的连续已缓存哈希前缀 */
  loadPrefix(): Promise<{ cursor: number; hashes: string[] }>;
  /** 持久化第 index 片哈希（index 严格按序） */
  put(index: number, hash: string, size: number): Promise<void>;
}

export interface RemoteChunks {
  /** 本任务已确认落盘的分片：index → hash（来自 init.uploadedChunks） */
  known: Map<number, string>;
  /**
   * 全局 CAS 已存在的内容哈希集合（跨文件去重命中）。
   * 注意：这些物理分片存在 ≠ 本文件已建立关联，因此命中片必须调用 link()
   * 建立本文件的 file_chunks 关联并让服务端 ref_count+1，而不是裸跳过。
   */
  skipHashes: Set<string>;
  /** 上传一片（内部负责重试） */
  upload(index: number, hash: string, buffer: ArrayBuffer): Promise<void>;
  /**
   * 关联一片已存在的全局 CAS 分片（只建 file_chunks 关联，不传字节）。
   * 用于全局命中：本文件 complete 前必须有自己的关联，否则会 CHUNKS_INCOMPLETE。
   */
  link(index: number, hash: string): Promise<void>;
}

export interface PipelineEvents {
  onHashStart?(index: number, fromCache: boolean): void;
  onHashDone?(info: HashedChunkInfo, fromCache: boolean): void;
  onUploadStart?(index: number, skippedOnServer: boolean): void;
  onUploadDone?(info: { index: number; size: number; skippedOnServer: boolean }): void;
  /**
   * 全部分片哈希已知（含缓存恢复）时回调一次，参数为完整哈希数组。
   * 用于“算完后秒传仲裁”：返回 'instant' 则中止上传（文件已在服务端整体命中）；
   * 返回一组全局命中哈希时，会加入去重集合，剩余分片继续流水线。
   */
  onAllHashed?(hashes: string[]): Promise<'instant' | void> | 'instant' | void;
  /** 任意状态变化时回调，参数为计数器只读快照 */
  onTick?(counters: PipelineCounters): void;
}

export interface PipelineOptions {
  totalChunks: number;
  hasher: HashSink;
  reader: ChunkReader;
  cache: HashCache;
  remote: RemoteChunks;
  /** 同时 inflight（哈希中+排队+上传中）分片上限，内存闸门 */
  maxInflight: number;
  /** 同时在传的分片上限 */
  uploadConcurrency: number;
  events?: PipelineEvents;
  signal?: AbortSignal;
}

export interface PipelineCounters {
  totalChunks: number;
  hashedChunks: number;
  /** 本次从缓存复用、未重新哈希的片数 */
  hashCacheReused: number;
  settledChunks: number;
  /** 服务端已有（本任务关联）而跳过的片数 */
  serverSkipped: number;
  /** 全局 CAS 命中（其它文件已上传相同内容）而跳过上传的片数 */
  globalDedupSkipped: number;
  /** 本次真正发起上传的片数 */
  newlyUploaded: number;
  /** 当前实际 inflight 分片数（哈希中+排队+上传中） */
  inflight: number;
  hashing: number;
  uploading: number;
  queued: number;
}

export interface PipelineResult {
  chunkHashes: string[];
  counters: PipelineCounters;
  /** true=全量哈希算出后经 onAllHashed 仲裁为秒传，上传整体中止 */
  instantAborted: boolean;
}

export class PipelineAbortedError extends Error {
  constructor() {
    super('流水线已取消');
    this.name = 'AbortError';
  }
}

/** 内部信号：全量哈希算出后仲裁为秒传，中止后续上传（非失败） */
class PipelineInstantError extends Error {
  constructor() {
    super('instant');
    this.name = 'InstantAbort';
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw new PipelineAbortedError();
}

/** 可等待的槽位信号量 */
function semaphore(initial: number) {
  let permits = initial;
  const waiters: Array<() => void> = [];
  return {
    async acquire(signal?: AbortSignal): Promise<void> {
      while (permits <= 0) {
        await new Promise<void>((resolve) => waiters.push(resolve));
        throwIfAborted(signal);
      }
      permits -= 1;
    },
    release(): void {
      permits += 1;
      const next = waiters.shift();
      if (next) next();
    },
  };
}

/** 串行化原语：保证哈希器同一时刻只处理一个分片 */
function serialized<A extends unknown[], R>(fn: (...args: A) => Promise<R>) {
  let chain: Promise<unknown> = Promise.resolve();
  return (...args: A): Promise<R> => {
    const run = chain.then(() => fn(...args));
    // 失败不中断后续链，但调用方仍能拿到自己的 rejection
    chain = run.catch(() => {});
    return run;
  };
}

interface ReadyItem {
  index: number;
  hash: string;
  /** 仅上传项持有字节；全局 CAS「只关联」项为 null（不读字节、不占内存槽） */
  buffer: ArrayBuffer | null;
  /** true=只关联已存在的 CAS 分片（link），false=需要上传字节 */
  linkOnly: boolean;
}

export async function runPipeline(opts: PipelineOptions): Promise<PipelineResult> {
  const {
    totalChunks,
    hasher,
    reader,
    cache,
    remote,
    maxInflight,
    uploadConcurrency,
    events,
    signal,
  } = opts;

  if (maxInflight < 1) throw new Error('maxInflight 必须 >= 1');
  if (uploadConcurrency < 1) throw new Error('uploadConcurrency 必须 >= 1');

  const counters: PipelineCounters = {
    totalChunks,
    hashedChunks: 0,
    hashCacheReused: 0,
    settledChunks: 0,
    serverSkipped: 0,
    globalDedupSkipped: 0,
    newlyUploaded: 0,
    inflight: 0,
    hashing: 0,
    uploading: 0,
    queued: 0,
  };
  const tick = () => events?.onTick?.({ ...counters });

  const chunkHashes: string[] = new Array(totalChunks);
  if (totalChunks === 0) return { chunkHashes, counters, instantAborted: false };

  // 1) 恢复哈希断点：只接受从 0 开始的连续前缀（仅恢复哈希，不恢复字节）
  const prefix = await cache.loadPrefix();
  const resumeCursor = Math.min(prefix.cursor, totalChunks);
  for (let i = 0; i < resumeCursor; i++) chunkHashes[i] = prefix.hashes[i];
  counters.hashCacheReused = resumeCursor; // 这些片在生产者中跳过 Worker 哈希
  tick();

  // 2) 有界槽位：一个在途分片（读/哈希/排队/上传全程）占一个名额
  const slots = semaphore(maxInflight);
  // ready 队列：index -> item
  const readyMap = new Map<number, ReadyItem>();
  const readyCondition = createCondition();
  let firstError: Error | null = null;

  const fail = (err: unknown): void => {
    if (!firstError) {
      firstError = err instanceof Error ? err : new Error(String(err));
    }
    readyCondition.broadcast();
  };

  const hashSerial = serialized((index: number, buffer: ArrayBuffer) =>
    hasher.hash(index, buffer),
  );

  let instantAborted = false;
  let producerDone = false;

  /** 本文件已有关联（无需任何网络动作） */
  const isOwnKnown = (index: number, hash: string): boolean =>
    remote.known.get(index) === hash;
  /** 全局 CAS 已有该内容（需要走 link 只关联，不传字节） */
  const isGlobalHit = (hash: string): boolean => remote.skipHashes.has(hash);

  /** 放入一个“只关联”项：不持有字节、不占内存槽，消费者调用 remote.link */
  const enqueueLink = (index: number, hash: string): void => {
    counters.queued += 1;
    readyMap.set(index, { index, hash, buffer: null, linkOnly: true });
    readyCondition.broadcast();
  };

  /* ---------- 生产者：顺序读片 → （缓存命中则跳过 Worker）哈希 → 上传/只关联/跳过 ---------- */
  async function produce(): Promise<void> {
    try {
      for (let index = 0; index < totalChunks; index++) {
        throwIfAborted(signal);

        const fromCache = index < resumeCursor;
        const knownHash = chunkHashes[index]; // 缓存前缀已有哈希

        // 路径 A：哈希已知（本地缓存前缀），无需再算
        if (knownHash !== undefined) {
          if (isOwnKnown(index, knownHash)) {
            // 本文件已有关联：彻底跳过，不读字节
            counters.settledChunks += 1;
            counters.serverSkipped += 1;
            counters.hashedChunks = Math.max(counters.hashedChunks, index + 1);
            events?.onHashStart?.(index, true);
            events?.onHashDone?.({ index, hash: knownHash, size: reader.size(index) }, true);
            events?.onUploadStart?.(index, true);
            events?.onUploadDone?.({ index, size: reader.size(index), skippedOnServer: true });
            tick();
            continue;
          }
          if (isGlobalHit(knownHash)) {
            // 全局 CAS 命中：只关联、不传字节、不读字节、不占内存槽
            counters.hashedChunks = Math.max(counters.hashedChunks, index + 1);
            events?.onHashStart?.(index, true);
            events?.onHashDone?.({ index, hash: knownHash, size: reader.size(index) }, true);
            enqueueLink(index, knownHash);
            tick();
            continue;
          }
        }

        // 路径 B：需要读字节（可能还要 Worker 哈希），占内存槽
        await slots.acquire(signal); // 内存闸门
        throwIfAborted(signal);

        counters.inflight += 1;
        if (!fromCache) counters.hashing += 1;
        events?.onHashStart?.(index, fromCache);
        tick();

        let buffer: ArrayBuffer;
        let hash: string;
        try {
          buffer = await reader.read(index);
          throwIfAborted(signal);
          if (fromCache) {
            hash = chunkHashes[index];
          } else {
            hash = await hashSerial(index, buffer);
          }
          throwIfAborted(signal);
        } catch (err) {
          counters.inflight -= 1;
          if (!fromCache) counters.hashing -= 1;
          slots.release();
          throw err;
        }

        chunkHashes[index] = hash;
        if (!fromCache) {
          // 新算出的分片持久化哈希，刷新后可续算
          await cache.put(index, hash, buffer.byteLength);
        }

        if (!fromCache) counters.hashing -= 1;
        counters.hashedChunks = index + 1;
        events?.onHashDone?.({ index, hash, size: buffer.byteLength }, fromCache);

        if (isOwnKnown(index, hash)) {
          // 本文件已有关联：释放槽位，直接跳过
          counters.settledChunks += 1;
          counters.serverSkipped += 1;
          counters.inflight -= 1;
          events?.onUploadStart?.(index, true);
          events?.onUploadDone?.({ index, size: buffer.byteLength, skippedOnServer: true });
          slots.release();
        } else if (isGlobalHit(hash)) {
          // 全局命中：释放字节与内存槽，改为只关联（本文件仍需建立 file_chunks）
          counters.inflight -= 1;
          slots.release();
          enqueueLink(index, hash);
        } else {
          counters.queued += 1;
          readyMap.set(index, { index, hash, buffer, linkOnly: false });
          readyCondition.broadcast();
        }
        tick();
      }

      // 全部分片哈希已知：秒传仲裁（此前可能已有部分分片上传/关联，秒传意味着剩余全部免传）
      if (chunkHashes.length === totalChunks && chunkHashes.every((h) => h !== undefined)) {
        const verdict = await events?.onAllHashed?.([...chunkHashes]);
        if (verdict === 'instant') {
          instantAborted = true;
          throw new PipelineInstantError();
        }
      }
    } catch (err) {
      if (err instanceof PipelineInstantError) {
        // 秒传：不视为失败；唤醒消费者快速退出
        readyCondition.broadcast();
        return;
      }
      fail(err);
    } finally {
      producerDone = true;
      // 唤醒可能在等待新分片的消费者（正常结束或失败都需要）
      readyCondition.broadcast();
    }
  }

  /* ---------- 消费者：先占 HTTP 槽，再认领分片（保证秒传中止可丢弃排队项） ---------- */
  let claimCursor = 0;
  const uploadSlots = semaphore(uploadConcurrency);

  async function consume(): Promise<void> {
    for (;;) {
      throwIfAborted(signal);
      if (firstError) throw firstError;

      // 先预留 HTTP 并发槽（此处可能等待）。在拿到槽之前不认领任何分片，
      // 这样秒传中止时未预留槽的分片仍安全留在 readyMap 中可被丢弃。
      await uploadSlots.acquire(signal);
      if (instantAborted) {
        uploadSlots.release();
        break;
      }

      // 同步段：推进可认领序号，跳过“本任务已有”的片（全局命中是 link 项，在队列里），
      // 从队列取一个就绪项。整段无 await，保证多个消费者不会重复认领同一项。
      let item: ReadyItem | null = null;
      for (;;) {
        if (firstError) {
          uploadSlots.release();
          throw firstError;
        }
        if (instantAborted || claimCursor >= totalChunks) {
          uploadSlots.release();
          return;
        }
        const index = claimCursor;
        if (
          chunkHashes[index] !== undefined &&
          !readyMap.has(index) &&
          isOwnKnown(index, chunkHashes[index])
        ) {
          claimCursor += 1;
          continue;
        }
        const found = readyMap.get(index);
        if (!found) break; // 等待生产者产出
        readyMap.delete(index);
        claimCursor += 1;
        counters.queued -= 1;
        item = found;
        break;
      }

      if (!item) {
        // 释放预留槽，等待新产出 / 结束 / 秒传中止广播后重试
        uploadSlots.release();
        if (instantAborted) break;
        if (
          producerDone &&
          counters.queued === 0 &&
          claimCursor < totalChunks &&
          chunkHashes[claimCursor] === undefined
        ) {
          break;
        }
        await readyCondition.wait(signal);
        continue;
      }

      const { index, hash, buffer, linkOnly } = item;
      const size = linkOnly ? reader.size(index) : (buffer as ArrayBuffer).byteLength;
      try {
        counters.uploading += 1;
        events?.onUploadStart?.(index, false);
        tick();
        try {
          if (linkOnly) {
            // 全局 CAS 命中：只建立本文件关联（不传字节）
            await remote.link(index, hash);
            counters.globalDedupSkipped += 1;
          } else {
            await remote.upload(index, hash, buffer as ArrayBuffer);
            counters.newlyUploaded += 1;
          }
        } catch (err) {
          // 秒传仲裁竞态：在途请求到达时任务已被带清单 init 置为 completed，
          // 服务端返回 FILE_ALREADY_VERIFIED —— 此时整文件已由秒传关联完成，
          // 在途的这一片“失败”不应让整次上传失败。
          if (
            instantAborted &&
            typeof err === 'object' &&
            err !== null &&
            'code' in err &&
            (err as { code?: string }).code === 'FILE_ALREADY_VERIFIED'
          ) {
            counters.globalDedupSkipped += linkOnly ? 1 : 0;
          } else {
            throw err;
          }
        }
        counters.uploading -= 1;
        counters.settledChunks += 1;
        events?.onUploadDone?.({
          index,
          size,
          skippedOnServer: linkOnly,
        });
      } finally {
        // 只有真正持有字节的上传项占过内存槽；link 项不占
        if (!linkOnly) {
          counters.inflight -= 1;
          slots.release();
        }
        uploadSlots.release();
      }
      tick();
    }
  }

  const producer = produce();
  const consumers = Array.from({ length: uploadConcurrency }, () => consume());
  await Promise.all([producer, ...consumers]);

  // 秒传中止：仍留在 readyMap 的项不再处理。
  // 上传项归还其占有的内存槽；link 项不持字节、不占槽，仅丢弃即可。
  if (instantAborted) {
    for (const [, item] of readyMap) {
      if (!item.linkOnly) {
        counters.inflight -= 1;
        slots.release();
      }
    }
    readyMap.clear();
  }

  if (signal?.aborted) throw new PipelineAbortedError();
  if (firstError) throw firstError;
  if (!instantAborted && chunkHashes.some((h) => h === undefined)) {
    throw new Error('流水线结束但存在未哈希分片');
  }
  tick();
  return { chunkHashes, counters, instantAborted };
}

/** 简易条件变量：等待新 ready 分片或结束信号 */
function createCondition() {
  let waiters: Array<() => void> = [];
  return {
    async wait(signal?: AbortSignal): Promise<void> {
      await new Promise<void>((resolve) => {
        const done = () => {
          signal?.removeEventListener?.('abort', done);
          resolve();
        };
        waiters.push(done);
        signal?.addEventListener?.('abort', done, { once: true });
      });
    },
    notify(): void {
      const w = waiters.shift();
      if (w) w();
    },
    broadcast(): void {
      const ws = waiters;
      waiters = [];
      for (const w of ws) w();
    },
  };
}
