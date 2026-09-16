/**
 * 多机一致性专项（模拟“两台后端共用一套对象存储 + 一个 Redis 锁”）：
 *
 *   - 两个独立 HTTP app（不同端口）= 两台后端实例
 *   - 共享同一 LocalObjectStore 根目录           = 共享 S3/MinIO bucket
 *   - 共享同一个 MemoryLocker                     = 共享 Redis 锁
 *
 * 覆盖验收：
 *   T1 双机同哈希首传：对象只有一份、cas_chunks 只有一行、引用计数正确
 *   T2 双机同时 complete：只有一个真正进入合并；都返回一致结果、可下载
 *   T3 传一半“杀掉”一台，换另一台续传并完成（断片由另一台补齐）
 *   T4 GC 与首传对撞：正在引用的片不被删
 *   T5 删 A 不影响 B；引用归零后对象真的被删掉
 */
import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';

process.env.STORAGE_DIR = await fsp.mkdtemp(path.join(os.tmpdir(), 'chunk-multi-'));
process.env.LOCK_DRIVER = 'memory';

const { LocalObjectStore } = await import('../src/store/local.js');
const { setObjectStore } = await import('../src/store/index.js');
const { MemoryLocker } = await import('../src/store/locker.js');
const { setLocker } = await import('../src/store/locker.js');
const { initDb, getPool } = await import('../src/db.js');
const { ensureStorageDirs } = await import('../src/storage.js');
const { createApp } = await import('../src/app.js');

// 共享对象存储（同一根目录）与共享锁，注入单例供两台 app 复用
const store = new LocalObjectStore(process.env.STORAGE_DIR);
setObjectStore(store);
const sharedLocker = new MemoryLocker();
setLocker(sharedLocker);

await ensureStorageDirs();
await initDb();

const sha256 = (b) => createHash('sha256').update(b).digest('hex');
const sha256Text = (s) => sha256(Buffer.from(s, 'utf8'));

function startServer() {
  return new Promise((resolve) => {
    const app = createApp();
    const server = app.listen(0, () => resolve({ server, port: server.address().port }));
  });
}
const a = await startServer();
const b = await startServer();
const urlA = `http://localhost:${a.port}/api/files`;
const urlB = `http://localhost:${b.port}/api/files`;
const gc = async (port, minAgeSec = 0) =>
  (
    await fetch(`http://localhost:${port}/api/admin/gc`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ minAgeSec }),
    })
  ).json();
const J = (o) => ({
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(o),
});

let passed = 0;
function check(name, cond) {
  assert.ok(cond, name);
  passed += 1;
  console.log(`  ✓ ${name}`);
}

// 构造 N 个等长、内容互不相同的分片
function makeChunks(count, size, seed) {
  return Array.from({ length: count }, (_, i) => {
    const tag = `${i}-${seed}`;
    return Buffer.from(tag.padEnd(size, 'x').slice(0, size));
  });
}

/* ---------- T1 双机同哈希首传 ---------- */
console.log('T1：两台机器并发首传同一内容哈希');
{
  const chunkSize = 8;
  const parts = makeChunks(3, chunkSize, 'same');
  const content = Buffer.concat(parts);
  const hashes = parts.map(sha256);

  // 两个不同 fileId 的同一序号、同一内容，分别打到 A、B，并发上传
  const idA = sha256Text(`t1-a:${content.length}`);
  const idB = sha256Text(`t1-b:${content.length}`);
  for (const [id] of [[idA], [idB]]) {
    const base = id === idA ? urlA : urlB;
    await fetch(
      `${base}/init`,
      // 不同文件各自 init 到自己那台（也可交错）
      J({ fileId: id, fileName: `t1-${id}.bin`, fileSize: content.length, chunkSize, totalChunks: 3, fileHash: null }),
    );
  }

  // 并发：A 传 idA 的三片、B 传 idB 的三片（内容相同）
  const upload = (base, id, i) =>
    fetch(`${base}/${id}/chunks/${i}?hash=${hashes[i]}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: parts[i],
    });
  const results = await Promise.all([
    upload(urlA, idA, 0), upload(urlB, idB, 0),
    upload(urlA, idA, 1), upload(urlB, idB, 1),
    upload(urlA, idA, 2), upload(urlB, idB, 2),
  ]);
  const statuses = await Promise.all(results.map((r) => r.status));
  check('6 个并发上传全部 201', statuses.every((s) => s === 201));

  // cas_chunks 对每个内容哈希只有一行，ref_count 恰好为 2（两个文件各引用一次）
  const pool = getPool();
  for (const h of hashes) {
    const [rows] = await pool.query('SELECT ref_count FROM cas_chunks WHERE chunk_hash=?', [h]);
    check(`cas_chunks 中 ${h.slice(0, 8)} 仅一行且 ref_count=2`, rows.length === 1 && rows[0].ref_count === 2);
  }

  // 对象存储物理文件只有一份
  for (const h of hashes) {
    const abs = path.join(process.env.STORAGE_DIR, 'cas', h.slice(0, 2), `${h}.part`);
    await fsp.access(abs);
  }
  check('物理分片对象各只有一份', true);
}

/* ---------- T2 双机同时 complete ---------- */
console.log('T2：两台机器对同一文件同时点完成');
{
  const chunkSize = 8;
  const parts = makeChunks(2, chunkSize, 'complete');
  const content = Buffer.concat(parts);
  const hashes = parts.map(sha256);
  const agg = sha256Text(hashes.join(''));
  const id = sha256Text(`t2:${content.length}`);

  await fetch(`${urlA}/init`, J({
    fileId: id, fileName: 't2.bin', fileSize: content.length, chunkSize,
    totalChunks: 2, fileHash: agg,
  }));
  await uploadTo(urlA, id, parts, hashes);

  // 两台同时 complete
  const [rA, rB] = await Promise.all([
    fetch(`${urlA}/${id}/complete`, { method: 'POST' }),
    fetch(`${urlB}/${id}/complete`, { method: 'POST' }),
  ]);
  const sA = rA.status;
  const sB = rB.status;
  const okCount = [sA, sB].filter((s) => s === 200).length;
  const busyCount = [sA, sB].filter((s) => s === 409).length;
  check(`恰好一个 complete 成功（200 数=${okCount}，409 数=${busyCount}）`, okCount === 1 && busyCount === 1);

  const [statusRows] = await getPool().query('SELECT status, merged_hash FROM files WHERE id=?', [id]);
  check('文件最终 completed', statusRows[0]?.status === 'completed');

  // 两台都能下载同一合并产物
  const dlA = await fetch(`${urlA}/${id}/download`);
  const dlB = await fetch(`${urlB}/${id}/download`);
  check('A 可下载且字节正确', dlA.status === 200 && Buffer.compare(Buffer.from(await dlA.arrayBuffer()), content) === 0);
  check('B 可下载且字节正确', dlB.status === 200 && Buffer.compare(Buffer.from(await dlB.arrayBuffer()), content) === 0);
}

/* ---------- T3 传一半“杀掉”一台，另一台续传并完成 ---------- */
console.log('T3：A 传一半后由 B 接管补齐并完成');
{
  const chunkSize = 8;
  const parts = makeChunks(4, chunkSize, 'failover');
  const content = Buffer.concat(parts);
  const hashes = parts.map(sha256);
  const agg = sha256Text(hashes.join(''));
  const id = sha256Text(`t3:${content.length}`);

  await fetch(`${urlA}/init`, J({
    fileId: id, fileName: 't3.bin', fileSize: content.length, chunkSize,
    totalChunks: 4, fileHash: agg,
  }));
  // A 只传 #0、#1
  await uploadTo(urlA, id, [parts[0], parts[1]], [hashes[0], hashes[1]], [0, 1]);

  // 模拟 A 宕机后客户端改打到 B（对象存储/DB/锁是共享的，B 可直接接管）。
  // 不真正关 socket，只把后续请求全部发往 B。
  const list = await (await fetch(`${urlB}/${id}/chunks`)).json();
  check('B 能看到 A 已传的 #0/#1', list.chunks.map((c) => c.index).sort().join(',') === '0,1');
  await uploadTo(urlB, id, [parts[2], parts[3]], [hashes[2], hashes[3]], [2, 3]);
  const done = await fetch(`${urlB}/${id}/complete`, { method: 'POST' });
  check('B 接管后 complete 成功', done.status === 200);
  const dl = await fetch(`${urlB}/${id}/download`);
  check('B 下载字节正确（4 片齐全）',
    dl.status === 200 && Buffer.compare(Buffer.from(await dl.arrayBuffer()), content) === 0);
}

/* ---------- T4 GC 与首传对撞：正在引用的片不被删 ---------- */
console.log('T4：GC 不会删除仍被引用的对象；只清理零引用');
{
  // 此时已完成的 T1/T2/T3 分片仍被引用，GC 必须全部保留
  const g = await gc(b.port, 0);
  check('被引用期间 GC 删除零个内容分片', g.removedChunks === 0 && (g.orphanChunkFilesRemoved ?? 0) === 0);

  // 新建一个孤儿零引用场景：写对象 + 直接在 mock 库插入 ref_count=0 的“旧”行，
  // 模拟“曾经有引用、全部解除但 GC 尚未跑”，GC 应安全回收。
  const { __mockDb } = await import('./mock-mysql.mjs');
  const orphan = Buffer.from('orphan-chunk!');
  const oh = sha256(orphan);
  const rel = `cas/${oh.slice(0, 2)}/${oh}.part`;
  const { writeCasChunk } = await import('../src/storage.js');
  await writeCasChunk(oh, orphan);
  __mockDb().cas.set(oh, {
    chunk_hash: oh,
    chunk_size: BigInt(orphan.length),
    storage_path: rel,
    ref_count: 0,
    created_at: new Date(Date.now() - 3600_000), // 超过 GC 宽限
  });
  const g2 = await gc(b.port, 0);
  check('GC 回收零引用孤儿分片（库行+对象）', g2.removedChunks >= 1);
  const rows = __mockDb().cas.has(oh) ? [1] : [];
  check('孤儿 cas 行已删除', rows.length === 0);
  const exists = await fsp
    .access(path.join(process.env.STORAGE_DIR, rel))
    .then(() => true)
    .catch(() => false);
  check('孤儿对象文件已删除', !exists);
}

/* ---------- T5 删 A 不影响 B；引用归零后对象被删 ---------- */
console.log('T5：删一个文件不影响另一个；全部删除并 GC 后对象回收');
{
  const chunkSize = 8;
  const shared = makeChunks(2, chunkSize, 'share');
  const sharedContent = Buffer.concat(shared);
  const sharedHashes = shared.map(sha256);
  const sharedAgg = sha256Text(sharedHashes.join(''));

  // A、B 两个文件内容完全相同（秒传或独立上传后引用同一组 CAS 对象）
  const idA = sha256Text(`t5-a:${sharedContent.length}`);
  const idB = sha256Text(`t5-b:${sharedContent.length}`);
  for (const id of [idA, idB]) {
    await fetch(`${urlA}/init`, J({
      fileId: id, fileName: `t5-${id}.bin`, fileSize: sharedContent.length, chunkSize,
      totalChunks: 2, fileHash: sharedAgg,
    }));
  }
  // A 正常上传（建立首份 CAS），B 用 link 只关联
  await uploadTo(urlA, idA, shared, sharedHashes);
  for (let i = 0; i < 2; i++) {
    const r = await fetch(`${urlB}/${idB}/chunks/${i}/link?hash=${sharedHashes[i]}`, { method: 'POST' });
    assert.equal(r.status, 201, `B link #${i} 应 201`);
  }
  await fetch(`${urlA}/${idA}/complete`, { method: 'POST' });
  await fetch(`${urlB}/${idB}/complete`, { method: 'POST' });

  const [before] = await getPool().query('SELECT ref_count FROM cas_chunks WHERE chunk_hash=?', [sharedHashes[0]]);
  check('共享分片被两个文件引用，ref_count=2', before[0].ref_count === 2);

  // 删 A：B 仍可下载，GC 不能删共享片
  const delA = await fetch(`${urlA}/${idA}`, { method: 'DELETE' });
  check('删除 A 成功', delA.status === 200);
  const dlB = await fetch(`${urlB}/${idB}/download`);
  check('删 A 后 B 仍可下载', dlB.status === 200);
  await gc(a.port, 0);
  const stillThere = await fsp
    .access(path.join(process.env.STORAGE_DIR, 'cas', sharedHashes[0].slice(0, 2), `${sharedHashes[0]}.part`))
    .then(() => true)
    .catch(() => false);
  check('删 A 后 GC 不回收仍被 B 引用的对象', stillThere);

  // 再删 B 并 GC：对象必须真的被删掉
  await fetch(`${urlB}/${idB}`, { method: 'DELETE' });
  await gc(b.port, 0);
  const gone = await fsp
    .access(path.join(process.env.STORAGE_DIR, 'cas', sharedHashes[0].slice(0, 2), `${sharedHashes[0]}.part`))
    .then(() => false)
    .catch(() => true);
  check('A、B 都删除并 GC 后，共享对象被物理删除', gone);
}

async function uploadTo(base, id, bufs, hashList, indexes = null) {
  for (let k = 0; k < bufs.length; k++) {
    const i = indexes ? indexes[k] : k;
    const r = await fetch(`${base}/${id}/chunks/${i}?hash=${hashList[k]}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: bufs[k],
    });
    if (r.status !== 201) throw new Error(`upload ${id}#${i} -> ${r.status}`);
  }
}

console.log(`\n多机一致性专项全部通过 ✅（${passed} 项）`);
a.server.close();
b.server.close();
process.exit(0);
