/**
 * 唯一的 WebWorker：所有 SHA-256 计算都在这里完成，避免阻塞主线程。
 *
 * 主线程把每个分片的 ArrayBuffer 以 Transferable 方式转移进来（零拷贝）；
 * Worker 用 Web Crypto（SubtleCrypto）计算哈希后回传 hex 字符串。
 */

export interface HashRequest {
  /** 与消息一一对应的请求编号 */
  id: number;
  buffer: ArrayBuffer;
  index: number;
}

export interface HashResponse {
  id: number;
  index: number;
  hash: string;
  size: number;
}

const ctx = self as unknown as DedicatedWorkerGlobalScope;

function bufferToHex(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let hex = '';
  // 每次处理 2 字节，减少字符串拼接次数
  for (let i = 0; i < bytes.length; i += 2) {
    hex += bytes[i].toString(16).padStart(2, '0');
    if (i + 1 < bytes.length) {
      hex += bytes[i + 1].toString(16).padStart(2, '0');
    }
  }
  return hex;
}

ctx.onmessage = async (ev: MessageEvent<HashRequest>) => {
  const { id, buffer, index } = ev.data;
  try {
    const digest = await crypto.subtle.digest('SHA-256', buffer);
    const response: HashResponse = {
      id,
      index,
      hash: bufferToHex(digest),
      size: buffer.byteLength,
    };
    ctx.postMessage(response);
  } catch (err) {
    ctx.postMessage({
      id,
      index,
      error: err instanceof Error ? err.message : String(err),
    });
  }
};
