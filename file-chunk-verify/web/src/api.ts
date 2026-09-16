/**
 * Fetch 接口封装。开发环境经 Vite /api 代理到 :3000；
 * 生产环境可把 BASE 改为同源或网关地址。
 */
import type {
  ChunkListResponse,
  ChunkUploadResponse,
  CompleteResponse,
  InitResponse,
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
  fileHash: string;
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
