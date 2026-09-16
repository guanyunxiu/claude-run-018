/**
 * 对象存储抽象层（ObjectStore）
 * --------------------------------
 * 物理存放处对业务层只有“按内容哈希的 key”，绝不使用 fileId：
 *   CAS 分片：  cas/<hash 前2>/<hash>.part
 *   合并产物：  merged/<hash 前2>/<hash>.bin
 *   临时对象：  tmp/<uuid>.tmp
 *
 * 两套实现：
 *   - LocalObjectStore：本机磁盘（单测/单机），路径布局与历史一致
 *   - S3ObjectStore   ：MinIO / Amazon S3（默认，多台后端共享一套）
 *
 * 关键原语 putIfAbsent：
 *   跨机器首传同一 hash 时，最终只保留一份正确内容。S3 用条件写
 *   （IfNoneMatch: '*'）；local 用 rename 到内容寻址路径（原子，同内容覆盖无害）。
 *   调用方仍配合 DB 唯一键 + 分布式锁保证“库里只有一行、引用数正确”。
 */

export const CAS_PREFIX = 'cas';
export const MERGED_PREFIX = 'merged';
export const TMP_PREFIX = 'tmp';

const HASH_RE = /^[a-f0-9]{64}$/i;

export function safeHash(hash, label = 'hash') {
  if (typeof hash !== 'string' || !HASH_RE.test(hash)) {
    const err = new Error(`非法${label}`);
    err.code = 'VALIDATION_ERROR';
    err.status = 400;
    throw err;
  }
  return hash.toLowerCase();
}

export function casKey(chunkHash) {
  const h = safeHash(chunkHash, '分片哈希');
  return `${CAS_PREFIX}/${h.slice(0, 2)}/${h}.part`;
}

export function mergedKey(mergedHash) {
  const h = safeHash(mergedHash, '合并哈希');
  return `${MERGED_PREFIX}/${h.slice(0, 2)}/${h}.bin`;
}

export function tmpKey(name) {
  return `${TMP_PREFIX}/${name}`;
}

/**
 * @typedef {Object} ObjectStore
 * @property {(key:string, data:Buffer|Uint8Array, meta?:object)=>Promise<{written:boolean, size:number}>}
 *   putIfAbsent  仅当对象不存在时写入；返回 {written,size}。两台机器并发时只有一个 written=true。
 * @property {(key:string, data:Buffer|Uint8Array, meta?:object)=>Promise<{size:number}>}
 *   put          强制覆盖写（临时对象/自愈重写用）
 * @property {(key:string)=>Promise<Buffer>} getBuffer  读取整个对象
 * @property {(key:string)=>Promise<{exists:boolean, size:number}>} stat  存在性+大小
 * @property {(key:string)=>Promise<boolean>} delete  删除；不存在也不报错
 * @property {(key:string)=>import('node:stream').Readable} getStream  流式读取
 * @property {()=>Promise<Record<string,number>>} listByPrefix  列出前缀下 {key:size}
 * @property {(sources:string[], destKey:string)=>Promise<{size:number}>}
 *   compose      把若干对象按序服务端拼接为新对象（S3 multipart；local 流式读写）
 * @property {()=>Promise<void>} init  建桶/建目录
 */
