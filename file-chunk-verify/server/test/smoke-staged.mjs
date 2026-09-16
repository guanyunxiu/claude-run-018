/**
 * 冒烟：完全按前端分阶段时序打真实 HTTP（mock DB + 真实磁盘）。
 * 模拟“边算边传”：先 init(fileHash=null)，逐片上传（哈希边算边得），
 * 全部片上传与补报 /hash 交错，最后 complete。
 */
import { createHash } from 'node:crypto';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

process.env.STORAGE_DIR = await fsp.mkdtemp(path.join(os.tmpdir(), 'chunk-smoke-'));
const { createApp } = await import('../src/app.js');
const { initDb } = await import('../src/db.js');
await (await import('../src/storage.js')).ensureStorageDirs();
await initDb();

const sha256 = (b) => createHash('sha256').update(b).digest('hex');
const sha256Text = (s) => sha256(Buffer.from(s, 'utf8'));

const app = createApp();
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const base = `http://localhost:${server.address().port}/api/files`;
const json = (o) => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(o) });

// 40MB 文件 / 8MB = 5 片（最后一片 8MB? 40/8=5 整除）
const content = Buffer.alloc(40 * 1024 * 1024);
for (let i = 0; i < content.length; i++) content[i] = (i * 7 + 3) % 256;
const chunkSize = 8 * 1024 * 1024;
const total = 5;
const parts = Array.from({ length: total }, (_, i) =>
  content.subarray(i * chunkSize, Math.min((i + 1) * chunkSize, content.length)),
);
const hashes = parts.map(sha256);
const fileId = sha256Text(`big.bin:${content.length}:1700000000999:${chunkSize}`);

async function expect(cond, msg) {
  if (!cond) {
    console.error('✗ ' + msg);
    process.exit(1);
  }
  console.log('  ✓ ' + msg);
}

// 1) init 无聚合哈希（分阶段）
let r = await fetch(`${base}/init`, json({
  fileId, fileName: 'big.bin', fileSize: content.length, chunkSize, totalChunks: total, fileHash: null,
}));
let body = await r.json();
expect(r.status === 201 && body.file.hashLocked === false, '分阶段 init 成功，hashLocked=false');

// 2) 立刻 complete 必须被拒（哈希还没补）
r = await fetch(`${base}/${fileId}/complete`, { method: 'POST' });
expect(r.status === 409 && (await r.json()).error.code === 'FILE_HASH_REQUIRED', '未补哈希 complete → FILE_HASH_REQUIRED');

// 3) 边算边传：逐片上传（真实二进制），交错补报哈希放在中间
for (let i = 0; i < total; i++) {
  const up = await fetch(`${base}/${fileId}/chunks/${i}?hash=${hashes[i]}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream' },
    body: new Blob([parts[i]]),
  });
  expect(up.status === 201, `分片 ${i} 上传成功（此时聚合哈希可能尚未补报）`);
  // 在传到一半时补报聚合哈希（边传边补）
  if (i === 2) {
    const agg = sha256Text(hashes.join(''));
    const hr = await fetch(`${base}/${fileId}/hash`, json({ fileHash: agg }));
    const hb = await hr.json();
    expect(hr.status === 201 && hb.changed === true, '上传途中补报聚合哈希成功锁定');
  }
}

// 4) complete 强校验 + 合并
const agg = sha256Text(hashes.join(''));
r = await fetch(`${base}/${fileId}/complete`, { method: 'POST' });
body = await r.json();
expect(r.status === 200 && body.verified === true, 'complete 强校验通过');
expect(body.aggregateHash === agg, '聚合哈希与逐片哈希链一致');
expect(body.mergedHash === sha256(content), '合并文件 SHA-256 等于整文件真值');

const merged = await fsp.readFile(path.join(process.env.STORAGE_DIR, body.mergedPath));
expect(merged.length === content.length && Buffer.compare(merged, content) === 0, '合并产物字节与原文件完全一致');

server.close();
console.log('\n真实 HTTP 分阶段冒烟通过 ✅（40MB / 5 片，边传边补哈希，服务端强校验）');
