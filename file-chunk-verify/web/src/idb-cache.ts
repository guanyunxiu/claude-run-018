/**
 * 分片哈希的 IndexedDB 持久化（哈希断点续算）。
 *
 * 键设计（库名 chunk-hash-cache，均以 fileId 为主键）：
 *   meta store： key=fileId
 *     { fileId, fingerprint, fileName, fileSize, lastModified, chunkSize,
 *       totalChunks, cursor }            —— cursor = 下一个待哈希序号（已缓存 0..cursor-1）
 *   chunks store：key=[fileId, index]（复合键）
 *     { fileId, index, hash, size, ts }
 *
 * 约束：只按顺序哈希，所以“可续算区间”必须是从 0 开始的连续前缀。
 * 加载时会校验指纹与连续性；指纹不符（换了同名文件/改了分片大小）直接废弃旧缓存。
 */

export interface HashCacheMeta {
  fileId: string;
  fingerprint: string;
  fileName: string;
  fileSize: number;
  lastModified: number;
  chunkSize: number;
  totalChunks: number;
  /** 下一个待哈希的序号；缓存保证 0..cursor-1 均存在且连续 */
  cursor: number;
}

export interface CachedChunk {
  fileId: string;
  index: number;
  hash: string;
  size: number;
  ts: number;
}

const DB_NAME = 'chunk-hash-cache';
const DB_VERSION = 1;
const META_STORE = 'meta';
const CHUNK_STORE = 'chunks';

function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(META_STORE)) {
        db.createObjectStore(META_STORE, { keyPath: 'fileId' });
      }
      if (!db.objectStoreNames.contains(CHUNK_STORE)) {
        // 复合键 [fileId, index]，可按 fileId 范围遍历/清除
        db.createObjectStore(CHUNK_STORE, { keyPath: ['fileId', 'index'] });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error ?? new Error('打开 IndexedDB 失败'));
  });
}

function tx<T>(
  db: IDBDatabase,
  stores: string[],
  mode: IDBTransactionMode,
  fn: (t: IDBTransaction) => IDBRequest<T>,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const t = db.transaction(stores, mode);
    const req = fn(t);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}export class IndexedDbHashCache {
  private dbPromise: Promise<IDBDatabase> | null = null;

  private db(): Promise<IDBDatabase> {
    if (!this.dbPromise) this.dbPromise = openDb();
    return this.dbPromise;
  }

  private static fingerprint(
    fileName: string,
    fileSize: number,
    lastModified: number,
    chunkSize: number,
  ): string {
    return `${fileName}:${fileSize}:${lastModified}:${chunkSize}`;
  }

  /**
   * 加载某文件的连续哈希前缀；指纹不符或为空返回 { cursor:0, hashes:[] }。
   * 同时防御性校验连续性与条目数量，遇到空洞截断到空洞前。
   */
  async loadPrefix(params: {
    fileId: string;
    fileName: string;
    fileSize: number;
    lastModified: number;
    chunkSize: number;
  }): Promise<{ cursor: number; hashes: string[] }> {
    const db = await this.db();
    const meta = await tx<HashCacheMeta | undefined>(
      db,
      [META_STORE],
      'readonly',
      (t) =>
        t.objectStore(META_STORE).get(params.fileId) as IDBRequest<
          HashCacheMeta | undefined
        >,
    );

    const fingerprint = IndexedDbHashCache.fingerprint(
      params.fileName,
      params.fileSize,
      params.lastModified,
      params.chunkSize,
    );
    if (!meta || meta.fingerprint !== fingerprint) {
      if (meta) await this.clear(params.fileId);
      return { cursor: 0, hashes: [] };
    }

    // 用游标按 [fileId, index] 复合键范围取该文件的全部条目
    const rows = await new Promise<CachedChunk[]>((resolve, reject) => {
      const t = db.transaction(CHUNK_STORE, 'readonly');
      const store = t.objectStore(CHUNK_STORE);
      const range = IDBKeyRange.bound(
        [params.fileId, 0],
        [params.fileId, Number.MAX_SAFE_INTEGER],
      );
      const out: CachedChunk[] = [];
      t.oncomplete = () => resolve(out);
      t.onerror = () => reject(t.error);
      const curReq = store.openCursor(range);
      curReq.onerror = () => reject(curReq.error);
      curReq.onsuccess = () => {
        const cursor = curReq.result;
        if (cursor) {
          out.push(cursor.value as CachedChunk);
          cursor.continue();
        }
      };
    });

    // 按序号排序，找到第一个空洞，只保留连续前缀
    rows.sort((a, b) => a.index - b.index);
    const hashes: string[] = [];
    let cursor = 0;
    for (const row of rows) {
      if (row.index !== cursor) break;
      hashes.push(row.hash);
      cursor += 1;
    }
    // 不能超过 meta 声明的游标（防止脏数据）
    if (meta.cursor < cursor) cursor = meta.cursor;
    return { cursor, hashes: hashes.slice(0, cursor) };
  }

  /** 持久化一个新算出的分片哈希（调用方保证 index 严格按序 = 当前 cursor） */
  async putChunk(
    meta: Omit<HashCacheMeta, 'cursor'>,
    entry: { index: number; hash: string; size: number },
  ): Promise<void> {
    const db = await this.db();
    await new Promise<void>((resolve, reject) => {
      const t = db.transaction([META_STORE, CHUNK_STORE], 'readwrite');
      t.oncomplete = () => resolve();
      t.onerror = () => reject(t.error);
      t.objectStore(CHUNK_STORE).put({
        fileId: meta.fileId,
        index: entry.index,
        hash: entry.hash,
        size: entry.size,
        ts: Date.now(),
      } satisfies CachedChunk);
      t.objectStore(META_STORE).put({
        ...meta,
        cursor: entry.index + 1,
      } satisfies HashCacheMeta);
    });
  }

  /** 清空某文件的缓存（文件内容口径变化或完成后清理） */
  async clear(fileId: string): Promise<void> {
    const db = await this.db();
    await new Promise<void>((resolve, reject) => {
      const t = db.transaction([META_STORE, CHUNK_STORE], 'readwrite');
      t.oncomplete = () => resolve();
      t.onerror = () => reject(t.error);
      t.objectStore(META_STORE).delete(fileId);
      const store = t.objectStore(CHUNK_STORE);
      const range = IDBKeyRange.bound(
        [fileId, 0],
        [fileId, Number.MAX_SAFE_INTEGER],
      );
      store.delete(range);
    });
  }

  async close(): Promise<void> {
    if (this.dbPromise) (await this.dbPromise).close();
    this.dbPromise = null;
  }

  static makeFingerprint = IndexedDbHashCache.fingerprint;
}
