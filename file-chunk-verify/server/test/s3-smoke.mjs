/**
 * S3/MinIO 代码路径端到端冒烟（无 Docker 时用进程内 S3 兼容桩）。
 *
 * 真实使用 @aws-sdk/client-s3 的 S3ObjectStore（条件写、multipart、UploadPartCopy、
 * CopyObject、Head/Get/Delete/List），后端是 test/s3-stub.mjs 的内存 S3；
 * 元数据走内存 mock MySQL；锁用共享 memory（模拟 Redis）。
 *
 * 覆盖两个刚修的 bug：
 *   Bug1 秒传捐赠者校验必须走对象存储（fsp.stat 在 S3 模式下永远 false）：
 *        A 上传完成后，B 带整文件哈希+清单 init/precheck 必须 instant=true。
 *   Bug2 分片 >=5MB 时合并走 UploadPartCopy（之前命令未导入直接 ReferenceError，
 *        且失败置 failed 永久卡死）：complete 必须成功、可下载、字节一致；
 *        且失败后任务回退 uploading 可重试。
 */
process.env.STORAGE_DIR = ''; // 明确不使用本机磁盘
process.env.LOCK_DRIVER = 'memory';

import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createHash, randomFill } from 'node:crypto';

const tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), 's3e2e-'));
const { S3ObjectStore } = await import('../src/store/s3.js');
const { setObjectStore } = await import('../src/store/index.js');
const { MemoryLocker } = await import('../src/store/locker.js');
const { setLocker } = await import('../src/store/locker.js');
const { initDb, getPool } = await import('../src/db.js');
const { createApp } = await import('../src/app.js');
const { createS3Stub } = await import('./s3-stub.mjs');

const stub = createS3Stub();
const s3Port = await stub.start();

// 真实 S3ObjectStore 指向本地桩（forcePathStyle，关闭 SSL）
const s3 = new S3ObjectStore({
  endpoint: `http://127.0.0.1:${s3Port}`,
  region: 'us-east-1',
  bucket: 'chunk-verify-test',
  accessKeyId: 'test',
  secretAccessKey: 'test',
  forcePathStyle: true,
});
setObjectStore(s3);
setLocker(new MemoryLocker());
await s3.init();
await initDb();

const sha256 = (b) => createHash('sha256').update(b).digest('hex');
const sha256Text = (s) => sha256(Buffer.from(s, 'utf8'));

function startServer() {
  return new Promise((resolve) => {
    const app = createApp();
    const server = app.listen(0, () => resolve({ server, port: server.address().port }));
  });
}
const A = await startServer();
const B = await startServer();
const urlA = `http://localhost:${A.port}/api/files`;
const urlB = `http://localhost:${B.port}/api/files`;
const J = (o) => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(o) });

let passed = 0;
function check(name, cond) {
  assert.ok(cond, name);
  passed += 1;
  console.log(`  ✓ ${name}`);
}

// 构造 3 个互不相同、均 >=5MB（最后一片可不同大小但也取 5MB 简化断言）的随机分片
function randomBuf(size, seed) {
  const b = Buffer.alloc(size);
  // 确定性伪随机（避免真 randomFill 慢），保证每片内容不同
  let x = seed;
  for (let i = 0; i < size; i++) {
    x = (x * 1103515245 + 12345) & 0x7fffffff;
    b[i] = x & 0xff;
  }
  return b;
}
void randomFill;

const chunkSize = 5 * 1024 * 1024 + 123; // 略大于 5MB，强制走 UploadPartCopy
const parts = [randomBuf(chunkSize, 1), randomBuf(chunkSize, 2), randomBuf(chunkSize, 3)];
const content = Buffer.concat(parts);
const hashes = parts.map(sha256);
const agg = sha256Text(hashes.join(''));

/* ---------- T1 上传：3 个大分片（其中交错打到 A/B） ---------- */
console.log('T1：>=5MB 分片上传到大对象存储');
{
  const idA = sha256Text(`s3-donor:${content.length}`);
  // 打到 A 注册
  const init = await fetch(`${urlA}/init`, J({
    fileId: idA, fileName: 'big.bin', fileSize: content.length, chunkSize,
    totalChunks: 3, fileHash: agg,
  }));
  check('A init 201', init.status === 201);

  const bases = [urlA, urlB, urlA];
  for (let i = 0; i < 3; i++) {
    const r = await fetch(`${bases[i]}/${idA}/chunks/${i}?hash=${hashes[i]}`, {
      method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: parts[i],
    });
    const body = await r.json().catch(() => ({}));
    check(`分片 #${i} 上传成功（201，首次写对象）`, r.status === 201 && body.healed === false);
  }

  /* ---------- T2 complete：触发 S3 multipart + UploadPartCopy ---------- */
  console.log('T2：complete 走 S3 multipart（UploadPartCopy）');
  const done = await fetch(`${urlA}/${idA}/complete`, { method: 'POST' });
  const doneBody = await done.json().catch(() => ({}));
  check(`complete 200（无 ReferenceError/不卡死），实际 ${done.status}`, done.status === 200);
  check('返回 mergedHash', /^[a-f0-9]{64}$/.test(doneBody.mergedHash || ''));

  // merged 对象确实在 S3 桩里，且是 multipart 组装（大小=三片之和）
  const mStat = await s3.stat(doneBody.mergedPath);
  check('合并产物在对象存储上且大小=文件总大小', mStat.exists && mStat.size === content.length);

  // 下载字节完全一致（流式读 S3）
  const dl = await fetch(`${urlB}/${idA}/download`); // 从 B 下载，验证对象共享
  check('从另一台 B 可下载', dl.status === 200);
  const dlBuf = Buffer.from(await dl.arrayBuffer());
  check('下载字节与源文件完全一致', dlBuf.equals(content));

  /* ---------- T2b 小分片合并（验证 carry 累积满足 S3 5MB part 限制） ---------- */
  console.log('T2b：多个 <5MB 小分片合并（part 累积，最后一片可小）');
  {
    const smallSize = 1024 * 1024; // 1MB，6 片都小于 5MB
    const smallParts = [0, 1, 2, 3, 4, 5].map((i) => randomBuf(smallSize, 100 + i));
    const smallContent = Buffer.concat(smallParts);
    const smallHashes = smallParts.map(sha256);
    const smallAgg = sha256Text(smallHashes.join(''));
    const idS = sha256Text(`s3-small:${smallContent.length}`);
    await fetch(`${urlA}/init`, J({
      fileId: idS, fileName: 'small.bin', fileSize: smallContent.length,
      chunkSize: smallSize, totalChunks: 6, fileHash: smallAgg,
    }));
    for (let i = 0; i < 6; i++) {
      const r = await fetch(`${urlA}/${idS}/chunks/${i}?hash=${smallHashes[i]}`, {
        method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: smallParts[i],
      });
      assert.equal(r.status, 201, `小分片 #${i} 上传应 201`);
    }
    const doneS = await fetch(`${urlA}/${idS}/complete`, { method: 'POST' });
    check(`6 个 1MB 分片 complete 成功（不触发 EntityTooSmall），实际 ${doneS.status}`, doneS.status === 200);
    const dsb = await doneS.json();
    const st = await s3.stat(dsb.mergedPath);
    check('小文件合并产物大小=6MB', st.exists && st.size === smallContent.length);
    const dlS = await fetch(`${urlB}/${idS}/download`);
    check('小文件下载字节一致', dlS.status === 200 && Buffer.from(await dlS.arrayBuffer()).equals(smallContent));
  }

  /* ---------- T3 秒传：B 用整文件哈希+清单，捐赠者校验走对象存储 ---------- */
  console.log('T3：B 带清单 init 秒传（捐赠者对象在 S3 而非本机磁盘）');
  const idC = sha256Text(`s3-instant:${content.length}`);
  const instant = await fetch(`${urlB}/init`, J({
    fileId: idC, fileName: 'big-copy.bin', fileSize: content.length, chunkSize,
    totalChunks: 3, fileHash: agg, chunkHashes: hashes,
  }));
  const ib = await instant.json();
  check(`带清单 init 在 B 秒传 instant=true，实际 ${instant.status}`, instant.status === 200 && ib.instant === true);
  check('秒传文件状态 completed 且可零上传下载', ib.file?.status === 'completed');
  const dl2 = await fetch(`${urlA}/${idC}/download`);
  check('秒传文件从 A 下载字节一致', dl2.status === 200 && Buffer.from(await dl2.arrayBuffer()).equals(content));

  // precheck 同样必须命中（只读）
  const pre = await fetch(`${urlB}/precheck`, J({ fileHash: agg, chunkHashes: hashes }));
  const pb = await pre.json();
  check('precheck 返回 instant=true（捐赠者对象存储可读）', pre.status === 200 && pb.instant === true);

  /* ---------- T4 分片 CAS 物理对象只一份（UploadPartCopy 引用同一源） ---------- */
  console.log('T4：大分片对象去重与引用计数');
  const pool = getPool();
  for (const h of hashes) {
    const [rows] = await pool.query('SELECT ref_count FROM cas_chunks WHERE chunk_hash=?', [h]);
    // 捐赠文件 + 秒传文件各引用一次 = 2
    check(`cas_chunks ${h.slice(0, 8)} 仅一行 ref_count=2`, rows.length === 1 && rows[0].ref_count === 2);
    const st = await s3.stat(`cas/${h.slice(0, 2)}/${h}.part`);
    check('物理对象在 S3 存在且大小=分片大小', st.exists && st.size === chunkSize);
  }

  /* ---------- T5 合并失败可恢复（不卡死 failed） ---------- */
  console.log('T5：合并异常后任务回退 uploading 可重试');
  // 造一个引用齐全但让合并抛错的场景较复杂；这里直接验证状态机：
  // 对一个分片不足的任务 complete 报 CHUNKS_INCOMPLETE 后状态仍是 uploading（可补齐重试）
  const idBad = sha256Text('s3-incomplete');
  await fetch(`${urlA}/init`, J({
    fileId: idBad, fileName: 'bad.bin', fileSize: chunkSize, chunkSize,
    totalChunks: 1, fileHash: sha256('x'),
  }));
  const bad = await fetch(`${urlA}/${idBad}/complete`, { method: 'POST' });
  check('缺片 complete 返回 409 CHUNKS_INCOMPLETE', bad.status === 409);
  const [stRows] = await pool.query('SELECT status FROM files WHERE id=?', [idBad]);
  check('业务错误（409）后状态回退 uploading', stRows[0]?.status === 'uploading');

  /* ---------- T6 合并 500（读源对象失败）后不卡 failed，自愈后可重试完成 ---------- */
  console.log('T6：合并读对象失败（500）后回退 uploading，重传自愈再 complete 成功');
  {
    const idR = sha256Text('s3-recover');
    await fetch(`${urlA}/init`, J({
      fileId: idR, fileName: 'recover.bin', fileSize: content.length,
      chunkSize, totalChunks: 3, fileHash: agg,
    }));
    for (let i = 0; i < 3; i++) {
      await fetch(`${urlA}/${idR}/chunks/${i}?hash=${hashes[i]}`, {
        method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: parts[i],
      });
    }
    // 物理删除 #1 的 CAS 对象（库行仍在）：complete 读源失败（409/5xx）
    await s3.delete(`cas/${hashes[1].slice(0, 2)}/${hashes[1]}.part`);
    const fail = await fetch(`${urlA}/${idR}/complete`, { method: 'POST' });
    check(`对象缺失时 complete 失败（非 200，实际 ${fail.status}）`, fail.status !== 200);
    const [rows] = await pool.query('SELECT status, merge_owner FROM files WHERE id=?', [idR]);
    check('失败后回退 uploading 且清租约（不是 failed 终态，可恢复）',
      rows[0]?.status === 'uploading' && rows[0]?.merge_owner == null);

    // 重传 #1 自愈（服务端发现对象缺失会用字节重写，healed=true）
    const heal = await fetch(`${urlA}/${idR}/chunks/1?hash=${hashes[1]}`, {
      method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: parts[1],
    });
    const hb = await heal.json();
    check('重传触发对象自愈（healed=true，200/201 均可）',
      hb.healed === true && (heal.status === 201 || heal.status === 200));

    // 再次 complete：租约可重新抢占，合并成功
    const redone = await fetch(`${urlB}/${idR}/complete`, { method: 'POST' });
    check('自愈后在另一台 B complete 成功', redone.status === 200);
    const dlR = await fetch(`${urlA}/${idR}/download`);
    check('恢复后下载字节一致', dlR.status === 200 && Buffer.from(await dlR.arrayBuffer()).equals(content));
  }

  console.log(`\nS3 对象存储冒烟全部通过 ✅（${passed} 项）`);
}

A.server.close();
B.server.close();
stub.stop();
process.exit(0);
