/**
 * CAS v3 冒烟（mock DB + 真实 HTTP + 真实磁盘）：
 *   文件 A 正常上传 → 文件 B(同内容) init 带清单秒传零上传 →
 *   文件 C(部分相同) 只传差异片 → 删除 A/B/C → GC 物理回收
 */
import { createHash } from 'node:crypto';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

process.env.STORAGE_DIR = await fsp.mkdtemp(path.join(os.tmpdir(), 'chunk-cas-'));
const { createApp } = await import('../src/app.js');
const { initDb } = await import('../src/db.js');
await (await import('../src/storage.js')).ensureStorageDirs();
await initDb();

const sha256 = (b) => createHash('sha256').update(b).digest('hex');
const sha256Text = (s) => sha256(Buffer.from(s, 'utf8'));

const app = createApp();
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const port = server.address().port;
const base = `http://localhost:${port}/api/files`;
const adminBase = `http://localhost:${port}/api/admin`;
const J = (o) => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(o) });
const gc = (minAgeSec = 0) =>
  fetch(`${adminBase}/gc`, J({ minAgeSec })).then((r) => r.json());

let n = 0;
function ok(cond, msg) {
  if (!cond) {
    console.error('✗ ' + msg);
    process.exit(1);
  }
  n += 1;
  console.log('  ✓ ' + msg);
}
const fid = (name, size, cs) => sha256Text(`${name}:${size}:1700000002${n++}:${cs}`);

// 24B / 8B = 3 片
const content = Buffer.alloc(24);
for (let i = 0; i < content.length; i++) content[i] = (i * 13 + 1) % 256;
const cs = 8;
const parts = [0, 1, 2].map((i) => content.subarray(i * 8, i * 8 + 8));
const hashes = parts.map(sha256);
const agg = sha256Text(hashes.join(''));

/* ---- A 正常分阶段上传后完成 ---- */
const idA = fid('a.bin', content.length, cs);
let r = await fetch(`${base}/init`, J({
  fileId: idA, fileName: 'a.bin', fileSize: content.length, chunkSize: cs,
  totalChunks: 3, fileHash: null,
}));
ok((await r.json()).instant === false, 'A 首次 init 非秒传');
for (let i = 0; i < 3; i++) {
  await fetch(`${base}/${idA}/chunks/${i}?hash=${hashes[i]}`, {
    method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: parts[i],
  });
}
await fetch(`${base}/${idA}/hash`, J({ fileHash: agg }));
r = await fetch(`${base}/${idA}/complete`, { method: 'POST' });
ok(r.status === 200, 'A complete 成功');

/* ---- B 完全相同内容：带清单 init 直接秒传，不发任何分片 ---- */
const idB = fid('b-copy.bin', content.length, cs);
r = await fetch(`${base}/init`, J({
  fileId: idB, fileName: 'b-copy.bin', fileSize: content.length, chunkSize: cs,
  totalChunks: 3, fileHash: agg, chunkHashes: hashes,
}));
const b = await r.json();
ok(b.instant === true && b.file.status === 'completed', 'B 秒传命中，状态 completed');
const dlB = await fetch(`${base}/${idB}/download`);
ok(Buffer.compare(Buffer.from(await dlB.arrayBuffer()), content) === 0, 'B 零上传即可下载');

/* ---- precheck 只读接口 ---- */
r = await fetch(`${base}/precheck`, J({ fileHash: agg, chunkHashes: hashes }));
ok((await r.json()).instant === true, 'precheck 对已存在文件返回 instant=true');

/* ---- C 仅中间片不同：全局 CAS 命中 0、2，只传差异 ---- */
const partsC = [parts[0], Buffer.from('DIFFERNT'), parts[2]]; // 8B 差异片
const contentC = Buffer.concat(partsC);
const hashesC = partsC.map(sha256);
const aggC = sha256Text(hashesC.join(''));
const idC = fid('c.bin', contentC.length, cs);
r = await fetch(`${base}/init`, J({
  fileId: idC, fileName: 'c.bin', fileSize: contentC.length, chunkSize: cs,
  totalChunks: 3, fileHash: aggC, chunkHashes: hashesC,
}));
const cInit = await r.json();
ok(cInit.instant === false, 'C 内容不同不秒传');
ok(cInit.hits['0'] === hashes[0] && cInit.hits['2'] === hashes[2], 'init.hits 标出全局已存在的 #0/#2');

// 反向验证：若客户端无视 hits、一片都不关联/上传就 complete，必然缺片
const idLazy = fid('lazy.bin', contentC.length, cs);
await fetch(`${base}/init`, J({
  fileId: idLazy, fileName: 'lazy.bin', fileSize: contentC.length, chunkSize: cs,
  totalChunks: 3, fileHash: aggC, chunkHashes: hashesC,
}));
const lazyDone = await fetch(`${base}/${idLazy}/complete`, { method: 'POST' });
ok(lazyDone.status === 409 && (await lazyDone.json()).error.code === 'CHUNKS_INCOMPLETE',
  '无视 hits 裸跳过 → complete 报 CHUNKS_INCOMPLETE');
await fetch(`${base}/${idLazy}`, { method: 'DELETE' }); // 清理该任务

// 正确做法：#0/#2 是全局命中 → 只关联不传字节；#1 是差异片 → 上传字节
const lk0 = await fetch(`${base}/${idC}/chunks/0/link?hash=${hashesC[0]}`, { method: 'POST' });
ok((await lk0.json()).linked === true, 'C 的 #0 只关联（零字节）');
const up1 = await fetch(`${base}/${idC}/chunks/1?hash=${hashesC[1]}`, {
  method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: partsC[1],
});
ok((await up1.json()).dedup === false, 'C 的差异片 #1 上传 dedup=false');
const lk2 = await fetch(`${base}/${idC}/chunks/2/link?hash=${hashesC[2]}`, { method: 'POST' });
ok((await lk2.json()).linked === true, 'C 的 #2 只关联（零字节）');
r = await fetch(`${base}/${idC}/complete`, { method: 'POST' });
ok(r.status === 200, 'C 关联命中片 + 上传差异片后 complete 成功');

// 物理共享分片路径唯一
const sharedAbs = path.join(process.env.STORAGE_DIR, 'cas', hashes[0].slice(0, 2), `${hashes[0]}.part`);
ok(await fsp.access(sharedAbs).then(() => true).catch(() => false), '共享分片物理只存一份');

/* ---- 删除：只减引用 ---- */
r = await fetch(`${base}/${idA}`, { method: 'DELETE' });
ok((await r.json()).dereferencedChunks === 3, '删除 A 解除 3 个分片引用');
let gcResult = await gc(0);
ok(gcResult.removedChunks === 0, 'A 删除后 B/C 仍引用，GC 不删除物理分片');

// B、C 仍可下载
ok((await fetch(`${base}/${idB}/download`)).status === 200, 'B 仍可下载');
ok((await fetch(`${base}/${idC}/download`)).status === 200, 'C 仍可下载');

await fetch(`${base}/${idB}`, { method: 'DELETE' });
await fetch(`${base}/${idC}`, { method: 'DELETE' });
gcResult = await gc(0);
// 物理分片：3(A/B 全同) + 1(C 的差异片) = 4 个唯一 CAS 分片
ok(gcResult.removedChunks === 4, `全部引用删除后 GC 回收 4 个唯一物理分片（实际 ${gcResult.removedChunks}）`);
ok(gcResult.removedMerged >= 2, 'GC 回收至少 2 个内容寻址合并产物');
ok(await fsp.access(sharedAbs).then(() => false).catch(() => true), '共享分片物理最终被删除');

// 删除不存在 → 404
ok((await fetch(`${base}/${idA}`, { method: 'DELETE' })).status === 404, '重复删除 404');

server.close();
console.log(`\nCAS 秒传/去重/引用计数/GC 冒烟通过 ✅（${n} 项检查）`);
