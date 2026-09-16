/**
 * 流水线自动化测试（Node 直跑：node test/pipeline.test.mjs）
 * 覆盖验收点：
 *   T1 边算边传：上传先于“全部哈希完成”启动；最终哈希/上传全对
 *   T2 有界内存：inflight 峰值 ≤ maxInflight，HTTP 在途峰值 ≤ uploadConcurrency
 *   T3 刷新后续哈希：持久缓存存在前缀时，仅对游标后分片调用 Worker
 *   T4 已算未传直接传：缓存前缀里的片只读字节、不重算哈希，且服务端无记录时正常上传
 *   T5 取消后再开：少算（缓存）+ 少传（服务端清单），恢复后跑通
 *   T6/T7 上传/哈希失败错误冒泡；T8 0 分片
 */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { importTs } from './import-ts.mjs';

const { runPipeline, PipelineAbortedError } = await importTs('./src/pipeline.ts');

/* ---------------- 测试夹具 ---------------- */

// 确定性“假哈希”：对内容 sha256（前端 Web Crypto 与后端算法一致，这里用 node crypto 代替）
const sha256 = (buf) => createHash('sha256').update(Buffer.from(buf)).digest('hex');

function makeReader(total, sizeFor) {
  // 每片确定性内容（定长填充字节，不同序号填充不同）
  const bufs = Array.from({ length: total }, (_, i) =>
    Buffer.alloc(sizeFor(i), 65 + (i % 26)),
  );
  const reads = [];
  return {
    bufs,
    reads,
    size: (i) => sizeFor(i),
    read: async (i) => {
      reads.push(i);
      // 返回精确切片对应的独立 ArrayBuffer
      return bufs[i].buffer.slice(
        bufs[i].byteOffset,
        bufs[i].byteOffset + bufs[i].byteLength,
      );
    },
  };
}

function makeHasher() {
  const calls = [];
  let active = 0;
  let peakActive = 0;
  return {
    calls,
    get peakActive() {
      return peakActive;
    },
    hash: async (index, buffer) => {
      calls.push(index);
      active += 1;
      peakActive = Math.max(peakActive, active);
      // 制造异步：让上传/生产交错
      await new Promise((r) => setTimeout(r, 5));
      active -= 1;
      return sha256(buffer);
    },
  };
}

/** 内存哈希缓存：模拟 IndexedDB 持久化，可在“刷新”间复用同一实例 */
function makeCache(prefix = { cursor: 0, hashes: [] }) {
  let stored = new Map();
  for (let i = 0; i < prefix.cursor; i++) stored.set(i, prefix.hashes[i]);
  let cursor = prefix.cursor;
  const puts = [];
  return {
    get puts() {
      return puts;
    },
    get stored() {
      return stored;
    },
    async loadPrefix() {
      const hashes = [];
      let i = 0;
      while (stored.has(i)) {
        hashes.push(stored.get(i));
        i++;
      }
      cursor = i;
      return { cursor, hashes };
    },
    async put(index, hash) {
      puts.push(index);
      stored.set(index, hash);
    },
  };
}

function makeRemote(knownEntries = [], opts = {}) {
  const known = new Map(knownEntries);
  const skipHashes = new Set(opts.skipHashes ?? []);
  const uploaded = new Map(); // index -> hash
  const uploadOrder = [];
  let active = 0;
  let peakActive = 0;
  const failIndexes = new Set(opts.failIndexes ?? []);
  const uploadDelay = opts.uploadDelay ?? 8;
  return {
    known,
    skipHashes,
    uploaded,
    uploadOrder,
    get peakActive() {
      return peakActive;
    },
    async upload(index, hash, buffer) {
      uploadOrder.push(index);
      active += 1;
      peakActive = Math.max(peakActive, active);
      await new Promise((r) => setTimeout(r, uploadDelay));
      if (failIndexes.has(index)) {
        active -= 1;
        throw new Error(`模拟上传失败 index=${index}`);
      }
      assert.equal(sha256(buffer), hash, `上传内容与哈希不符 index=${index}`);
      uploaded.set(index, hash);
      active -= 1;
    },
  };
}

function makeEvents(peak) {
  return {
    onTick(c) {
      peak.inflight = Math.max(peak.inflight, c.inflight);
      peak.hashing = Math.max(peak.hashing, c.hashing);
      peak.uploading = Math.max(peak.uploading, c.uploading);
      peak.queued = Math.max(peak.queued, c.queued);
    },
  };
}

let passed = 0;
function check(name, cond) {
  assert.ok(cond, `断言失败：${name}`);
  passed += 1;
  console.log(`  ✓ ${name}`);
}

const CHUNK = 100;

/* ---------------- T1 边算边传 ---------------- */
console.log('T1：边算边传——上传在全部哈希完成前启动');
{
  const total = 20;
  const reader = makeReader(total, () => CHUNK);
  const hasher = makeHasher();
  const cache = makeCache();
  const remote = makeRemote();
  const peak = { inflight: 0, hashing: 0, uploading: 0, queued: 0 };

  let hashDoneAtUploadStart = null;
  const events = {
    ...makeEvents(peak),
    onUploadStart(index) {
      if (hashDoneAtUploadStart === null) {
        hashDoneAtUploadStart = hasher.calls.length;
      }
    },
  };

  const res = await runPipeline({
    totalChunks: total,
    hasher,
    reader,
    cache,
    remote,
    maxInflight: 6,
    uploadConcurrency: 3,
    events,
  });

  check('全部 20 片完成哈希', res.counters.hashedChunks === 20);
  check('全部 20 片完成上传', res.counters.settledChunks === 20);
  check('首个分片上传启动时哈希远未算完（< 20）', hashDoneAtUploadStart < 20);
  check('首个分片上传在第 1~3 片算完时即启动', hashDoneAtUploadStart <= 3);
  check('上传顺序覆盖全部 0..19', JSON.stringify([...remote.uploaded.keys()].sort((a, b) => a - b)) === JSON.stringify([...Array(20).keys()]));
  check('返回哈希与内容一致', res.chunkHashes.every((h, i) => h === sha256(reader.bufs[i])));
  check('缓存写入了 20 个哈希', cache.stored.size === 20);
}

/* ---------------- T2 有界内存 ---------------- */
console.log('T2：有界队列——inflight ≤ maxInflight，HTTP 在途 ≤ uploadConcurrency');
{
  const total = 60;
  const reader = makeReader(total, () => CHUNK);
  const hasher = makeHasher(); // 5ms
  const cache = makeCache();
  // 上传更慢（20ms），队列会被填满，能真正检验闸门
  const remote = makeRemote([], { uploadDelay: 20 });
  const peak = { inflight: 0, hashing: 0, uploading: 0, queued: 0 };

  await runPipeline({
    totalChunks: total,
    hasher,
    reader,
    cache,
    remote,
    maxInflight: 6,
    uploadConcurrency: 3,
    events: makeEvents(peak),
  });

  check(`inflight 峰值 ≤ 6（实测 ${peak.inflight}）`, peak.inflight <= 6);
  check(`队列中等待上传峰值 ≤ 6（实测 ${peak.queued}）`, peak.queued <= 6);
  check(`HTTP 在途峰值 ≤ 3（实测 ${remote.peakActive}）`, remote.peakActive <= 3);
  check('单哈希器从未并发（峰值 1）', hasher.peakActive === 1);
  check('上传确实发生过排队（queued 峰值 > 0）', peak.queued > 0);
}

/* ---------------- T3 刷新后续哈希 ---------------- */
console.log('T3：刷新后续哈希——只哈希缓存游标之后的分片');
{
  const total = 10;
  const reader = makeReader(total, () => CHUNK);
  const allHashes = reader.bufs.map((b) => sha256(b));

  // 模拟“上次会话已算完前 7 片”
  const cache = makeCache({ cursor: 7, hashes: allHashes.slice(0, 7) });
  const hasher = makeHasher();
  const remote = makeRemote();
  const peak = { inflight: 0, hashing: 0, uploading: 0, queued: 0 };

  const res = await runPipeline({
    totalChunks: total,
    hasher,
    reader,
    cache,
    remote,
    maxInflight: 6,
    uploadConcurrency: 3,
    events: makeEvents(peak),
  });

  check('Worker 只被调用 3 次（index 7,8,9）', hasher.calls.length === 3);
  check('Worker 调用序号恰为 7,8,9', JSON.stringify(hasher.calls) === '[7,8,9]');
  check('复用哈希计数为 7', res.counters.hashCacheReused === 7);
  check('总哈希完成数仍为 10', res.counters.hashedChunks === 10);
  check('全部 10 片均上传（含缓存前缀）', remote.uploaded.size === 10);
  check('未重复写缓存前缀（puts 从 7 开始）', JSON.stringify(cache.puts) === '[7,8,9]');
}

/* ---------------- T4 已算未传直接传 ---------------- */
console.log('T4：已算未传——缓存前缀只读字节上传，不重算哈希');
{
  const total = 8;
  const reader = makeReader(total, () => CHUNK);
  const allHashes = reader.bufs.map((b) => sha256(b));
  // 前 4 片已算哈希，但服务端一片都没有
  const cache = makeCache({ cursor: 4, hashes: allHashes.slice(0, 4) });
  const hasher = makeHasher();
  const remote = makeRemote(); // known 为空

  const res = await runPipeline({
    totalChunks: total,
    hasher,
    reader,
    cache,
    remote,
    maxInflight: 6,
    uploadConcurrency: 2,
  });

  check('Worker 只算后 4 片', hasher.calls.length === 4);
  check('前 4 片（已算未传）直接上传', [0, 1, 2, 3].every((i) => remote.uploaded.has(i)));
  check('8 片全部上传成功', remote.uploaded.size === 8);
  check('没有服务端跳过', res.counters.serverSkipped === 0);
  check('本次新传计数为 8', res.counters.newlyUploaded === 8);
}

/* ---------------- T5 取消 → 再开：少算少传 ---------------- */
console.log('T5：取消后再开——缓存与服务端清单分别续算、续传');
{
  const total = 12;
  const reader = makeReader(total, () => CHUNK);
  const allHashes = reader.bufs.map((b) => sha256(b));
  const cache = makeCache();
  // 让上传变慢，确保取消发生在中途
  const remote = makeRemote([], { uploadDelay: 15 });

  const ac = new AbortController();
  let rejected = false;
  const run1 = runPipeline({
    totalChunks: total,
    hasher: makeHasher(),
    reader,
    cache,
    remote,
    maxInflight: 4,
    uploadConcurrency: 2,
    signal: ac.signal,
  });

  // 40ms 后取消：此时已持久化一部分哈希、上传完成一部分分片
  setTimeout(() => ac.abort(), 40);
  try {
    await run1;
  } catch (err) {
    rejected = true;
    assert.ok(err instanceof PipelineAbortedError, '取消应抛出 PipelineAbortedError');
  }
  check('第一次运行被取消并抛错', rejected);
  const hashedBeforeAbort = cache.stored.size;
  const uploadedBeforeAbort = remote.uploaded.size;
  check('取消时已持久化部分哈希（>0 且 <12）', hashedBeforeAbort > 0 && hashedBeforeAbort < 12);
  check('取消时部分分片已上传（>0 且 <12）', uploadedBeforeAbort > 0 && uploadedBeforeAbort < 12);
  console.log(`    （取消时刻：本地已缓存 ${hashedBeforeAbort} 片哈希，服务端已收 ${uploadedBeforeAbort} 片）`);

  // 第二次开始：同一缓存（刷新语义）+ 服务端 known 用已上传清单构造
  const persistedHashes = [...cache.stored.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([, h]) => h);
  const cursor = cache.stored.size;
  const cache2 = makeCache({ cursor, hashes: persistedHashes.slice(0, cursor) });
  const knownEntries = [...remote.uploaded.entries()];
  const hasher2 = makeHasher();
  const remote2 = makeRemote(knownEntries, { uploadDelay: 2 });

  const res2 = await runPipeline({
    totalChunks: total,
    hasher: hasher2,
    reader,
    cache: cache2,
    remote: remote2,
    maxInflight: 4,
    uploadConcurrency: 2,
  });

  check('第二次 Worker 只算缺失的哈希（< 12）', hasher2.calls.length === 12 - cursor);
  check(`第二次少算了 ${cursor} 片`, hasher2.calls.length + cursor === 12);
  check('第二次跳过全部已上传分片', res2.counters.serverSkipped === uploadedBeforeAbort);
  check('第二次新传数 = 12 - 已上传', res2.counters.newlyUploaded === 12 - uploadedBeforeAbort);
  check('最终 12 片全部 settled', res2.counters.settledChunks === 12);
  check('最终哈希与全部内容一致', res2.chunkHashes.every((h, i) => h === allHashes[i]));
}

/* ---------------- T6 上传失败冒泡 ---------------- */
console.log('T6：上传失败错误必须冒泡到 runPipeline');
{
  const total = 6;
  const reader = makeReader(total, () => CHUNK);
  await assert.rejects(
    () =>
      runPipeline({
        totalChunks: total,
        hasher: makeHasher(),
        reader,
        cache: makeCache(),
        remote: makeRemote([], { failIndexes: [2] }),
        maxInflight: 6,
        uploadConcurrency: 2,
      }),
    /模拟上传失败 index=2/,
  );
  passed += 1;
  console.log('  ✓ 上传失败被抛出');
}

/* ---------------- T7 哈希失败冒泡 ---------------- */
console.log('T7：哈希失败错误必须冒泡');
{
  const total = 4;
  const reader = makeReader(total, () => CHUNK);
  const brokenHasher = {
    hash: async (index) => {
      if (index === 1) throw new Error('模拟 Worker 哈希失败');
      return sha256(reader.bufs[index]);
    },
  };
  await assert.rejects(
    () =>
      runPipeline({
        totalChunks: total,
        hasher: brokenHasher,
        reader,
        cache: makeCache(),
        remote: makeRemote(),
        maxInflight: 4,
        uploadConcurrency: 2,
      }),
    /模拟 Worker 哈希失败/,
  );
  passed += 1;
  console.log('  ✓ 哈希失败被抛出');
}

/* ---------------- T8 0 分片 ---------------- */
console.log('T8：0 分片直接成功返回空哈希数组');
{
  const reader = makeReader(0, () => 0);
  const hasher = makeHasher();
  const res = await runPipeline({
    totalChunks: 0,
    hasher,
    reader,
    cache: makeCache(),
    remote: makeRemote(),
    maxInflight: 6,
    uploadConcurrency: 3,
  });
  check('返回空哈希数组', Array.isArray(res.chunkHashes) && res.chunkHashes.length === 0);
  check('Worker 未被调用', hasher.calls.length === 0);
  check('无上传', res.counters.newlyUploaded === 0);
}

/* ---------------- T9 全局 CAS 去重命中 ---------------- */
console.log('T9：部分分片哈希在全局 CAS 已存在 → 跳过上传（跨文件去重）');
{
  const total = 8;
  const reader = makeReader(total, () => CHUNK);
  const allHashes = reader.bufs.map((b) => sha256(b));
  // 模拟“其它文件已上传过 index 2、5 对应内容”：按内容哈希命中（与本文件序号无关）
  const remote = makeRemote([], { skipHashes: [allHashes[2], allHashes[5]] });
  const hasher = makeHasher();

  const res = await runPipeline({
    totalChunks: total,
    hasher,
    reader,
    cache: makeCache(),
    remote,
    maxInflight: 6,
    uploadConcurrency: 3,
  });

  check('全局命中 2 片计数正确', res.counters.globalDedupSkipped === 2);
  check('全局命中分片未发起上传', !remote.uploaded.has(2) && !remote.uploaded.has(5));
  check('其余 6 片正常上传', remote.uploaded.size === 6);
  check('8 片全部 settled', res.counters.settledChunks === 8);
  check('新传计数为 6', res.counters.newlyUploaded === 6);
}

/* ---------------- T10 全哈希就绪 → 秒传中止 ---------------- */
console.log('T10：onAllHashed 仲裁秒传后，剩余分片不再上传');
{
  const total = 20;
  const reader = makeReader(total, () => CHUNK);
  const allHashes = reader.bufs.map((b) => sha256(b));
  // 哈希极快(1ms)、上传较慢(30ms)、HTTP 并发仅 2；槽位放宽到 ≥总数，
  // 模拟“哈希远快于网络”：算完时大部分片还在排队，秒传仲裁应丢弃这些排队项
  const remote = makeRemote([], { uploadDelay: 30 });
  const fastHasher = {
    calls: 0,
    async hash(index, buffer) {
      this.calls += 1;
      await new Promise((r) => setTimeout(r, 1));
      return sha256(buffer);
    },
  };

  // 当全部哈希就绪时返回 instant（模拟带清单 init 命中已完成文件）
  let hookHashes = null;
  const res = await runPipeline({
    totalChunks: total,
    hasher: fastHasher,
    reader,
    cache: makeCache(),
    remote,
    maxInflight: 20,
    uploadConcurrency: 2,
    events: {
      onAllHashed(hashes) {
        hookHashes = hashes;
        return 'instant';
      },
    },
  });

  check('钩子收到完整且正确的 20 个哈希',
    hookHashes && hookHashes.length === 20 &&
    hookHashes.every((h, i) => h === allHashes[i]));
  check('返回 instantAborted=true', res.instantAborted === true);
  check('全部 20 片完成哈希', res.counters.hashedChunks === 20);
  // 仲裁时最多 2 个在途请求 + 极少数边界（信号切换瞬间），排队项必须被丢弃
  check('秒传中止后排队项被丢弃，新传 ≤ 在途并发+边界(4)',
    res.counters.newlyUploaded <= 4);
  check('绝大多数分片未上传（至少 15 片被秒传省去）', remote.uploaded.size <= 4);
}

/* ---------------- T11 缓存前缀 + 全局命中：不读字节直接跳过 ---------------- */
console.log('T11：缓存前缀中命中全局 CAS 的分片，不读字节、不哈希');
{
  const total = 6;
  const reader = makeReader(total, () => CHUNK);
  const allHashes = reader.bufs.map((b) => sha256(b));
  // 前 3 片已缓存哈希，其中 0、1 全局命中，2 需要上传
  const cache = makeCache({ cursor: 3, hashes: allHashes.slice(0, 3) });
  const remote = makeRemote([], { skipHashes: [allHashes[0], allHashes[1]] });
  const hasher = makeHasher();

  const res = await runPipeline({
    totalChunks: total,
    hasher,
    reader,
    cache,
    remote,
    maxInflight: 6,
    uploadConcurrency: 2,
  });

  check('Worker 只算后 3 片', hasher.calls.length === 3);
  check('全局命中 2 片', res.counters.globalDedupSkipped === 2);
  check('实际新传 4 片（缓存中的 #2 + 后 3 片）', res.counters.newlyUploaded === 4);
  check('6 片全 settled', res.counters.settledChunks === 6);
  check('#0/#1 未上传', !remote.uploaded.has(0) && !remote.uploaded.has(1));
}

console.log(`\n流水线全部 ${passed} 条断言通过 ✅`);
