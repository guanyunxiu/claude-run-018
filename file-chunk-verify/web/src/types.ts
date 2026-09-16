/** 前后端共享的接口类型（与 server/src/routes/files.js 响应对齐） */

export interface UploadedChunk {
  index: number;
  hash: string;
  size: number;
  status: 'uploaded' | 'verified';
}

export interface InitResponse {
  resumed: boolean;
  file: {
    fileId: string;
    fileName: string;
    fileSize: number;
    chunkSize: number;
    totalChunks: number;
    fileHash: string;
    status: 'uploading' | 'merging' | 'completed' | 'failed';
  };
  uploadedChunks: UploadedChunk[];
}

export interface ChunkUploadResponse {
  index: number;
  hash: string;
  size: number;
  skipped: boolean;
}

export interface ChunkListResponse {
  fileId: string;
  uploadedCount: number;
  chunks: UploadedChunk[];
}

export interface CompleteResponse {
  verified: true;
  fileId: string;
  totalChunks: number;
  fileSize: number;
  aggregateHash: string;
  mergedHash: string;
  mergedPath: string;
}

export interface ApiErrorBody {
  error: {
    code: string;
    message: string;
    details?: unknown;
  };
}
