/**
 * 大样例冒烟：模拟 4 GB 文件（512 × 8 MB）的「首跑取消 → 刷新 → 再跑完成」。
 * 不真正分配 4GB：reader/hasher/upload 都是轻量假实现，
 * 但完整经过真实的有界流水线（pipeline.ts），统计少算/少传的片数。
 *
 *   node test/smoke-resume.mjs
 */
import { createHash } from 'node:crypto';
import { importTs } from './import-ts.mjs';

const { runPipeline, PipelineAbortedError } = await importTs('./src/pipeline.ts');

/* ---- 大文件画像（不真实分配字节，用“序号令牌”代表 8MB 分片） ---- */
const GB = 1024 ** 3;
const FILE_SIZE = 4 * GB; // 4 GB
const CHUNK_SIZE = 8 * 1024 * 1024; // 8 MB
const TOTAL = Math.ceil(FILE_SIZE / CHUNK_SIZE); // 512

// 每片的确定内容指纹（线上是真实字节的 SHA-256，这里对“分片序号令牌”哈希）
const tokenHash = (i) =>
  createHash('sha256').update(`fake-8MB-chunk-${i}`).digest('hex');
const ALL_HASHES = Array.from({ length: TOTAL }, (_, i) => tokenHash(i));

// 假“字节”：只放序号，真实内存可忽略，但保留 ArrayBuffer 形态
const fakeRead = async (i) =>
  new TextEncoder().encode(`fake-8MB-chunk-${i}`).buffer;

function countingHasher() {
  let active = 0;
  return {
    count: 0,
    peak: 0,
    async hash(i) {
      this.count += 1;
      active += 1;
      this.peak = Math.max(this.peak, active);
      await new Promise((r) => setImmediate(r));
      active -= 1;
      return tokenHash(i);
    },
  };
}

/** 可选的慢上传器，确保取消发生在“部分已传”阶段 */
function slowRemote(knownEntries = [], delayMs = 2) {
  const base = remote(knownEntries);
  const origUpload = base.upload.bind(base);
  base.upload = async (i, h) => {
    await new Promise((r) => setTimeout(r, delayMs));
    return origUpload(i, h);
  };
  return base;
}

function reader() {
  return {
    size: () => CHUNK_SIZE,
    read: fakeRead,
  };
}

function cache() {
  const stored = new Map();
  return {
    stored,
    async loadPrefix() {
      const hashes = [];
      let i = 0;
      while (stored.has(i)) {
        hashes.push(stored.get(i));
        i++;
      }
      return { cursor: i, hashes };
    },
    async put(i, h) {
      stored.set(i, h);
    },
  };
}

function remote(knownEntries = []) {
  const known = new Map(knownEntries);
  const uploaded = new Map();
  return {
    known,
    skipHashes: new Set(),
    uploaded,
    uploads: 0,
    async upload(i, h) {
      this.uploads += 1;
      await new Promise((r) => setImmediate(r));
      uploaded.set(i, h);
    },
  };
}

const line = (s) => console.log(s);

line(`==============================================`);
line(`大样例：4 GB 文件 = ${TOTAL} × 8 MB 分片`);
line(`==============================================`);

/* ---------- 第 1 次：启动后在约 30% 进度处“刷新/取消” ---------- */
const c1 = cache();
const r1 = slowRemote([], 2);
const h1 = countingHasher();
const ac = new AbortController();
let peakInflight = 0;
const abortThreshold = Math.floor(TOTAL * 0.3);
const run1 = runPipeline({
  totalChunks: TOTAL,
  hasher: h1,
  reader: reader(),
  cache: c1,
  remote: r1,
  maxInflight: 6,
  uploadConcurrency: 3,
  signal: ac.signal,
  events: {
    onTick(c) {
      peakInflight = Math.max(peakInflight, c.inflight);
    },
    // 在哈希约 30% 时模拟用户刷新/取消（此时部分片已传、部分仅算了哈希）
    onHashDone() {
      if (!ac.signal.aborted && c1.stored.size >= abortThreshold) ac.abort();
    },
  },
});

let err1 = null;
try {
  await run1;
} catch (e) {
  err1 = e;
}
if (!(err1 instanceof PipelineAbortedError)) throw new Error('第一次应被取消');
const hashedAfterAbort = c1.stored.size;
const uploadedAfterAbort = r1.uploaded.size;

line(`① 首次运行（哈希约 30% 时刷新页面）`);
line(`   Worker 实算哈希：${h1.count} 片（峰值哈希并发 ${h1.peak}，应为 1）`);
line(`   IndexedDB 已持久化哈希：${hashedAfterAbort} 片（≈ ${((hashedAfterAbort / TOTAL) * 100).toFixed(1)}%）`);
line(`   服务端已落盘分片：${uploadedAfterAbort} 片（上传落后于哈希，符合流水线特征）`);
line(`   在途槽位峰值：${peakInflight}（上限 6，内存 ≈ ${peakInflight}×8MB = ${peakInflight * 8}MB，与 4GB 文件大小无关）`);

/* ---------- 第 2 次：“刷新页面后”重新开始 ---------- */
const c2 = cache(); // 同一持久缓存（IndexedDB 跨刷新）
// 把 c1 已存哈希搬入“新开页面”读到的缓存
for (const [k, v] of c1.stored) c2.stored.set(k, v);

const knownEntries = [...r1.uploaded.entries()];
const r2 = remote(knownEntries); // 新会话从 init 拿到服务端清单
const h2 = countingHasher();
let peak2 = 0;

const result = await runPipeline({
  totalChunks: TOTAL,
  hasher: h2,
  reader: reader(),
  cache: c2,
  remote: r2,
  maxInflight: 6,
  uploadConcurrency: 3,
  events: {
    onTick(c) {
      peak2 = Math.max(peak2, c.inflight);
    },
  },
});

line(``);
line(`② 刷新后再次开始（续算 + 续传）`);
line(`   复用本地哈希（不再算）：${result.counters.hashCacheReused} 片`);
line(`   Worker 本次实算：${h2.count} 片（应 = ${TOTAL} - ${hashedAfterAbort} = ${TOTAL - hashedAfterAbort}）`);
line(`   服务端已有跳过：${result.counters.serverSkipped} 片（应 = ${uploadedAfterAbort}）`);
line(`   本次实际新传：${result.counters.newlyUploaded} 片（应 = ${TOTAL} - ${uploadedAfterAbort} = ${TOTAL - uploadedAfterAbort}）`);
line(`   在途槽位峰值：${peak2}（内存仍 ≈ ${peak2 * 8}MB 恒定）`);

/* ---------- 汇总核对 ---------- */
const ok = [
  ['单 Worker 哈希从未并发（第一次）', h1.peak === 1],
  ['单 Worker 哈希从未并发（第二次）', h2.peak === 1],
  ['内存槽位有界（第一次 ≤ 6）', peakInflight <= 6],
  ['内存槽位有界（第二次 ≤ 6）', peak2 <= 6],
  ['第二次少算的片数 = 已缓存片数', h2.count === TOTAL - hashedAfterAbort],
  ['第二次少传的片数 = 服务端已有片数', result.counters.newlyUploaded === TOTAL - uploadedAfterAbort],
  ['服务端跳过片数 = 第一次已上传片数', result.counters.serverSkipped === uploadedAfterAbort],
  ['最终 512 片全部 settled', result.counters.settledChunks === TOTAL],
  [
    '最终哈希序列与逐片真值一致',
    result.chunkHashes.every((h, i) => h === ALL_HASHES[i]),
  ],
];

line(``);
line(`③ 复用收益汇总（相对全量重算重传）`);
line(`   节省哈希：${hashedAfterAbort}/${TOTAL} 片（${((hashedAfterAbort / TOTAL) * 100).toFixed(1)}%）`);
line(`   节省上传：${uploadedAfterAbort}/${TOTAL} 片（${((uploadedAfterAbort / TOTAL) * 100).toFixed(1)}%，约 ${((uploadedAfterAbort * CHUNK_SIZE) / GB).toFixed(2)} GB 流量）`);
line(`==============================================`);

let allOk = true;
for (const [name, cond] of ok) {
  console.log(`${cond ? '  ✓' : '  ✗'} ${name}`);
  if (!cond) allOk = false;
}
if (!allOk) process.exit(1);
console.log(`\n大样例冒烟通过 ✅（模拟 4 GB 文件 / ${TOTAL} 片，峰值在途内存仅 ${peak2 * 8} MB 量级）`);
