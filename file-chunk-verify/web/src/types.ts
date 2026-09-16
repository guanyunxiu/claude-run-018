/** 前后端共享的接口类型（与 server/src/routes/files.js CAS v3 响应对齐） */

export interface UploadedChunk {
  index: number;
  hash: string;
  size: number;
  status: 'uploaded' | 'verified';
}

export interface InitResponse {
  resumed: boolean;
  /** true=秒传命中：文件已标记 completed，客户端零上传 */
  instant: boolean;
  file: {
    fileId: string;
    fileName: string;
    fileSize: number;
    chunkSize: number;
    totalChunks: number;
    fileHash: string | null;
    hashLocked: boolean;
    status: 'uploading' | 'merging' | 'completed' | 'failed';
    mergedHash: string | null;
    mergedPath: string | null;
  };
  /** 该文件已存在的分片关联（断点续传） */
  uploadedChunks: UploadedChunk[];
  /** 清单中已在全局 CAS 存在的分片 index→hash（跨文件去重命中） */
  hits: Record<number, string>;
}

export interface PrecheckResponse {
  instant: boolean;
  donor: { fileId: string; fileName: string; mergedHash: string } | null;
  hits: Record<number, string>;
  hitCount: number;
  totalChunks: number;
}

export interface SubmitHashResponse {
  fileId: string;
  fileHash: string;
  locked: true;
  changed: boolean;
}

export interface ChunkUploadResponse {
  index: number;
  hash: string;
  size: number;
  skipped: boolean;
  /** true=命中全局 CAS（其它文件已上传过相同内容），物理去重 */
  dedup: boolean;
}

export interface ChunkListResponse {
  fileId: string;
  uploadedCount: number;
  chunks: UploadedChunk[];
}

export interface CompleteResponse {
  verified: true;
  instant: boolean;
  fileId: string;
  totalChunks: number;
  fileSize: number;
  aggregateHash: string;
  mergedHash: string;
  mergedPath: string;
}

export interface DeleteResponse {
  deleted: true;
  fileId: string;
  dereferencedChunks: number;
  dereferencedMerged: boolean;
  physicalRemoved: false;
}

export interface GcResponse {
  removedChunks: number;
  removedMerged: number;
  bytesFreed: string;
  minAgeSec: number;
}

export interface ApiErrorBody {
  error: {
    code: string;
    message: string;
    details?: unknown;
  };
}
