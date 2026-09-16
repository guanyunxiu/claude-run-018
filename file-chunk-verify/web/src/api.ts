/**
 * Fetch 接口封装。开发环境经 Vite /api 代理到 :3000；
 * 生产环境可把 BASE 改为同源或网关地址。
 *
 * 分阶段任务时序：
 *   init(fileHash 可空) → 分片上传 与 POST /hash 可交错 → complete
 */
import type {
  ChunkListResponse,
  ChunkUploadResponse,
  CompleteResponse,
  DeleteResponse,
  GcResponse,
  InitResponse,
  PrecheckResponse,
  SubmitHashResponse,
} from './types';
import type { ApiErrorBody } from './types';

const BASE = '/api/files';

export class ApiException extends Error {
  status: number;
  code: string;
  details?: unknown;

  constructor(status: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'ApiException';
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

async function parseError(res: Response): Promise<ApiException> {
  let body: ApiErrorBody | null = null;
  try {
    body = (await res.json()) as ApiErrorBody;
  } catch {
    /* 非 JSON 错误响应 */
  }
  return new ApiException(
    res.status,
    body?.error.code || 'HTTP_ERROR',
    body?.error.message || `请求失败：HTTP ${res.status}`,
    body?.error.details,
  );
}

export interface InitParams {
  fileId: string;
  fileName: string;
  fileSize: number;
  chunkSize: number;
  totalChunks: number;
  /** 分阶段流水线：init 时尚未算完，允许为 null */
  fileHash: string | null;
  /** 全部已知时（全量缓存/算完）携带分片哈希清单，用于秒传与 CAS 命中预检 */
  chunkHashes?: string[] | null;
}

export async function initFile(params: InitParams): Promise<InitResponse> {
  const res = await fetch(`${BASE}/init`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as InitResponse;
}

/** 只读预检：返回是否可秒传 + 全局 CAS 分片命中（不产生副作用） */
export async function precheck(params: {
  fileHash: string | null;
  chunkHashes: string[];
}): Promise<PrecheckResponse> {
  const res = await fetch(`${BASE}/precheck`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as PrecheckResponse;
}

/** 删除文件任务（仅解除引用；物理对象由 GC 在引用归零时回收） */
export async function deleteFile(fileId: string): Promise<DeleteResponse> {
  const res = await fetch(`${BASE}/${fileId}`, { method: 'DELETE' });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as DeleteResponse;
}

/** 手动触发孤儿 CAS 对象垃圾回收（挂载于 /api/admin/gc） */
export async function runGc(minAgeSec = 0): Promise<GcResponse> {
  const adminBase = BASE.replace(/\/files$/, '/admin');
  const res = await fetch(`${adminBase}/gc`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ minAgeSec }),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as GcResponse;
}

/** 补报/锁定聚合哈希。相同值重复提交幂等；不同值服务端返回 FILE_HASH_LOCKED */
export async function submitFileHash(
  fileId: string,
  fileHash: string,
): Promise<SubmitHashResponse> {
  const res = await fetch(`${BASE}/${fileId}/hash`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ fileHash }),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as SubmitHashResponse;
}

export async function listChunks(fileId: string): Promise<ChunkListResponse> {
  const res = await fetch(`${BASE}/${fileId}/chunks`);
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as ChunkListResponse;
}

export interface UploadChunkParams {
  fileId: string;
  index: number;
  hash: string;
  blob: Blob;
  signal?: AbortSignal;
}

/**
 * 上传单个分片：原始二进制 body（application/octet-stream），
 * 分片哈希走 query 参数。
 */
export async function uploadChunk(
  params: UploadChunkParams,
): Promise<ChunkUploadResponse> {
  const { fileId, index, hash, blob, signal } = params;
  const res = await fetch(`${BASE}/${fileId}/chunks/${index}?hash=${hash}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream' },
    body: blob,
    signal,
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as ChunkUploadResponse;
}

export async function completeFile(fileId: string): Promise<CompleteResponse> {
  const res = await fetch(`${BASE}/${fileId}/complete`, { method: 'POST' });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as CompleteResponse;
}

export interface LinkChunkResponse {
  index: number;
  hash: string;
  size: number;
  skipped: boolean;
  linked: true;
}

/**
 * 只关联一片已存在的全局 CAS 分片（不发送分片字节）。
 * 用于 init.hits 命中：物理片已在服务端，但本文件需要自己的 file_chunks 关联，
 * 否则 complete 会 CHUNKS_INCOMPLETE。
 */
export async function linkChunk(
  fileId: string,
  index: number,
  hash: string,
  signal?: AbortSignal,
): Promise<LinkChunkResponse> {
  const res = await fetch(`${BASE}/${fileId}/chunks/${index}/link?hash=${hash}`, {
    method: 'POST',
    signal,
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as LinkChunkResponse;
}
