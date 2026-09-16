import { createHash } from 'node:crypto';

/** 计算 Buffer 的 sha256 hex */
export function sha256Hex(buffer) {
  return createHash('sha256').update(buffer).digest('hex');
}

/**
 * 聚合哈希：sha256(concat(各分片 sha256 的 hex 字符串))。
 * 与前端 Web Crypto 的计算方式保持严格一致（hex 字符串按序号直接拼接）。
 * @param {string[]} chunkHashes 按序号排列的分片哈希 hex
 */
export function aggregateHashHex(chunkHashes) {
  const hash = createHash('sha256');
  for (const h of chunkHashes) hash.update(h, 'utf8');
  return hash.digest('hex');
}
