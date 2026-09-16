/**
 * 端到端测试（无 MySQL 时通过 mock-mysql-loader 注入内存数据库）：
 *   node --loader ./test/mock-mysql-loader.mjs test/e2e.mjs
 * 覆盖：正常分片上传→聚合校验→合并、断点续传跳过、分片大小错误、
 *       分片哈希错误、落盘后磁盘分片被篡改检测、空文件。
 */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

process.env.STORAGE_DIR = await fsp.mkdtemp(path.join(os.tmpdir(), 'chunk-e2e-'));

const { createApp } = await import('../src/app.js');
const { initDb } = await import('../src/db.js');
const { ensureStorageDirs } = await import('../src/storage.js');

await ensureStorageDirs();
await initDb();

const sha256 = (buf) => createHash('sha256').update(buf).digest('hex');
const sha256Text = (s) => sha256(Buffer.from(s, 'utf8'));

const app = createApp();

let server;
await new Promise((resolve) => {
  server = app.listen(0, resolve);
});
const port = server.address().port;
const base = `http://localhost:${port}/api/files`;
const adminBase = `http://localhost:${port}/api/admin`;

async function call(method, urlPath, body, headers = {}) {
  const res = await fetch(`${base}${urlPath}`, {
    method,
    body,
    headers,
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = text;
  }
  return { status: res.status, json };
}

/** 调用孤儿对象 GC（独立挂载于 /api/admin/gc） */
async function gcCall(minAgeSec = 0) {
  const res = await fetch(`${adminBase}/gc`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ minAgeSec }),
  });
  return { status: res.status, json: await res.json() };
}

let passed = 0;
function check(name, cond) {
  assert.ok(cond, `断言失败：${name}`);
  passed += 1;
  console.log(`  ✓ ${name}`);
}

/* ---------------- 场景 1：正常上传 + 聚合校验 + 合并 ---------------- */
console.log('场景 1：正常分片上传 → 服务端聚合校验 → 合并完整文件');
{
  const content = Buffer.alloc(5_000_000, 0);
  for (let i = 0; i < content.length; i++) content[i] = i % 251;
  const fileName = 'demo-5mb.bin';
  const chunkSize = 2_000_000;
  const totalChunks = 3; // 2M + 2M + 1M
  const chunkBufs = [
    content.subarray(0, 2_000_000),
    content.subarray(2_000_000, 4_000_000),
    content.subarray(4_000_000),
  ];
  const chunkHashes = chunkBufs.map(sha256);
  const fileHash = sha256Text(chunkHashes.join(''));
  const fileId = sha256Text(`${fileName}:${content.length}:1700000000000:${chunkSize}`);

  const init = await call(
    'POST',
    '/init',
    JSON.stringify({
      fileId,
      fileName,
      fileSize: content.length,
      chunkSize,
      totalChunks,
      fileHash,
    }),
    { 'Content-Type': 'application/json' },
  );
  check('init 返回 201', init.status === 201);
  check('init.resumed=false', init.json.resumed === false);
  check('init 已上传分片为 0', init.json.uploadedChunks.length === 0);

  // 重复 init 幂等
  const init2 = await call(
    'POST',
    '/init',
    JSON.stringify({
      fileId,
      fileName,
      fileSize: content.length,
      chunkSize,
      totalChunks,
      fileHash,
    }),
    { 'Content-Type': 'application/json' },
  );
  check('重复 init 返回 200 resumed=true', init2.status === 200 && init2.json.resumed === true);

  // 参数冲突
  const badInit = await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: 1, chunkSize, totalChunks: 1, fileHash }),
    { 'Content-Type': 'application/json' },
  );
  check('参数冲突返回 409 FILE_PARAM_MISMATCH',
    badInit.status === 409 && badInit.json.error.code === 'FILE_PARAM_MISMATCH');

  // 上传分片 0
  const up0 = await call('POST', `/${fileId}/chunks/0?hash=${chunkHashes[0]}`, chunkBufs[0], {
    'Content-Type': 'application/octet-stream',
  });
  check('分片0 上传 201 skipped=false', up0.status === 201 && up0.json.skipped === false);

  // 重复上传同哈希 → 幂等跳过
  const up0again = await call(
    'POST',
    `/${fileId}/chunks/0?hash=${chunkHashes[0]}`,
    chunkBufs[0],
    { 'Content-Type': 'application/octet-stream' },
  );
  check('分片0 重传 200 skipped=true', up0again.status === 200 && up0again.json.skipped === true);

  // 大小错误：把 2MB 声明成序号 2（期望 1MB）
  const wrongSize = await call(
    'POST',
    `/${fileId}/chunks/2?hash=${chunkHashes[0]}`,
    chunkBufs[0],
    { 'Content-Type': 'application/octet-stream' },
  );
  check('大小不符返回 413 CHUNK_SIZE_MISMATCH',
    wrongSize.status === 413 && wrongSize.json.error.code === 'CHUNK_SIZE_MISMATCH');

  // 哈希错误：内容与 hash 参数不一致 → 拒绝写盘
  const wrongHash = await call(
    'POST',
    `/${fileId}/chunks/1?hash=${'a'.repeat(64)}`,
    chunkBufs[1],
    { 'Content-Type': 'application/octet-stream' },
  );
  check('哈希不符返回 422 CHUNK_HASH_MISMATCH',
    wrongHash.status === 422 && wrongHash.json.error.code === 'CHUNK_HASH_MISMATCH');

  // 正确补齐 1、2
  const up1 = await call('POST', `/${fileId}/chunks/1?hash=${chunkHashes[1]}`, chunkBufs[1], {
    'Content-Type': 'application/octet-stream',
  });
  check('分片1 上传 201', up1.status === 201);
  const up2 = await call('POST', `/${fileId}/chunks/2?hash=${chunkHashes[2]}`, chunkBufs[2], {
    'Content-Type': 'application/octet-stream',
  });
  check('最后一片(1MB) 上传 201', up2.status === 201 && up2.json.size === 1_000_000);

  // 查询已上传分片（断点续传接口）
  const list = await call('GET', `/${fileId}/chunks`);
  check('GET chunks 返回 3 片且序号连续',
    list.status === 200 &&
    list.json.chunks.length === 3 &&
    list.json.chunks.map((c) => c.index).join(',') === '0,1,2');

  // 提前完成不了（构造缺片场景放到场景 3，这里直接 complete）
  const done = await call('POST', `/${fileId}/complete`);
  check('complete 200 verified=true', done.status === 200 && done.json.verified === true);
  check('complete 聚合哈希与客户端一致', done.json.aggregateHash === fileHash);
  check('complete 合并文件 sha256 等于原始内容 sha256',
    done.json.mergedHash === sha256(content));

  // 合并文件真实落盘且字节一致
  const mergedAbs = path.join(process.env.STORAGE_DIR, done.json.mergedPath);
  const mergedBuf = await fsp.readFile(mergedAbs);
  check('合并文件与原内容字节一致', Buffer.compare(mergedBuf, content) === 0);

  // 分片被标记 verified
  const status = await call('GET', `/${fileId}/status`);
  check('status=completed 且 mergedHash 回填',
    status.json.status === 'completed' && status.json.mergedHash === done.json.mergedHash);

  // 完成后再传分片 → 409
  const afterDone = await call('POST', `/${fileId}/chunks/0?hash=${chunkHashes[0]}`, chunkBufs[0]);
  check('完成后再传返回 409 FILE_ALREADY_VERIFIED',
    afterDone.status === 409 && afterDone.json.error.code === 'FILE_ALREADY_VERIFIED');
}

/* ---------------- 场景 2：断点续传跳过 ---------------- */
console.log('场景 2：重新 init 已传 1 片的任务，只补传剩余分片');
{
  const content = Buffer.from('0123456789'.repeat(500)); // 5000B
  const fileName = 'resume.bin';
  const chunkSize = 2000;
  const totalChunks = 3;
  const chunkBufs = [
    content.subarray(0, 2000),
    content.subarray(2000, 4000),
    content.subarray(4000),
  ];
  const chunkHashes = chunkBufs.map(sha256);
  const fileHash = sha256Text(chunkHashes.join(''));
  const fileId = sha256Text(`${fileName}:${content.length}:1700000000001:${chunkSize}`);

  await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: content.length, chunkSize, totalChunks, fileHash }),
    { 'Content-Type': 'application/json' },
  );
  await call('POST', `/${fileId}/chunks/0?hash=${chunkHashes[0]}`, chunkBufs[0]);

  // 模拟刷新页面后重新 init
  const resumed = await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: content.length, chunkSize, totalChunks, fileHash }),
    { 'Content-Type': 'application/json' },
  );
  check('续传 init 返回已存在的分片0',
    resumed.status === 200 &&
    resumed.json.uploadedChunks.length === 1 &&
    resumed.json.uploadedChunks[0].index === 0 &&
    resumed.json.uploadedChunks[0].hash === chunkHashes[0]);

  // 只补传 1、2 即可完成
  await call('POST', `/${fileId}/chunks/1?hash=${chunkHashes[1]}`, chunkBufs[1]);
  await call('POST', `/${fileId}/chunks/2?hash=${chunkHashes[2]}`, chunkBufs[2]);
  const done = await call('POST', `/${fileId}/complete`);
  check('续传补齐后 complete 成功', done.status === 200);
}

/* ---------------- 场景 3：分片不齐禁止 complete ---------------- */
console.log('场景 3：分片数量不足时 complete 返回 CHUNKS_INCOMPLETE');
{
  const fileName = 'incomplete.bin';
  const chunkSize = 10;
  const content = Buffer.from('012345678901234789'); // 19B → 2 片
  const hashes = [sha256(content.subarray(0, 10)), sha256(content.subarray(10))];
  const fileHash = sha256Text(hashes.join(''));
  const fileId = sha256Text(`${fileName}:${content.length}:1700000000002:${chunkSize}`);
  await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: content.length, chunkSize, totalChunks: 2, fileHash }),
    { 'Content-Type': 'application/json' },
  );
  await call('POST', `/${fileId}/chunks/0?hash=${hashes[0]}`, content.subarray(0, 10));
  const done = await call('POST', `/${fileId}/complete`);
  check('缺片 complete 返回 409 且状态回退 uploading',
    done.status === 409 && done.json.error.code === 'CHUNKS_INCOMPLETE');
  const status = await call('GET', `/${fileId}/status`);
  check('失败后状态仍为 uploading（允许补传）', status.json.status === 'uploading');
}

/* ---------------- 场景 4：磁盘分片被篡改 → 完成时必须检出 ---------------- */
console.log('场景 4：落盘分片在磁盘上被篡改，complete 重算哈希时检出');
{
  const fileName = 'tampered.bin';
  const chunkSize = 8;
  const content = Buffer.from('abcdefgh12345678'); // 16B → 2 片
  const hashes = [sha256(content.subarray(0, 8)), sha256(content.subarray(8))];
  const fileHash = sha256Text(hashes.join(''));
  const fileId = sha256Text(`${fileName}:${content.length}:1700000000003:${chunkSize}`);
  await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: content.length, chunkSize, totalChunks: 2, fileHash }),
    { 'Content-Type': 'application/json' },
  );
  await call('POST', `/${fileId}/chunks/0?hash=${hashes[0]}`, content.subarray(0, 8));
  await call('POST', `/${fileId}/chunks/1?hash=${hashes[1]}`, content.subarray(8));

  // 直接篡改磁盘上的 CAS 物理分片（按内容哈希寻址）
  const storage = process.env.STORAGE_DIR;
  const part = path.join(storage, 'cas', hashes[0].slice(0, 2), `${hashes[0]}.part`);
  await fsp.writeFile(part, Buffer.from('xxxxxxxx'));

  const done = await call('POST', `/${fileId}/complete`);
  check('篡改分片被检出 422 CHUNK_HASH_MISMATCH',
    done.status === 422 && done.json.error.code === 'CHUNK_HASH_MISMATCH');
}

/* ---------------- 场景 5：空文件（0 分片） ---------------- */
console.log('场景 5：空文件 0 分片，聚合哈希为空哈希链，正常合并');
{
  const fileName = 'empty.bin';
  const chunkSize = 1024;
  const fileHash = sha256Text('');
  const fileId = sha256Text(`${fileName}:0:1700000000004:${chunkSize}`);
  const init = await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: 0, chunkSize, totalChunks: 0, fileHash }),
    { 'Content-Type': 'application/json' },
  );
  check('空文件 init 201', init.status === 201);
  const done = await call('POST', `/${fileId}/complete`);
  check('空文件 complete 200', done.status === 200 && done.json.totalChunks === 0);
  const mergedAbs = path.join(process.env.STORAGE_DIR, done.json.mergedPath);
  const stat = await fsp.stat(mergedAbs);
  check('合并产物为 0 字节文件', stat.size === 0);
}

/* ---------------- 场景 6：分阶段任务（先 init(无哈希) → 边传边补 → complete） ---------------- */
console.log('场景 6：分阶段任务：init 无 fileHash，上传与补报哈希交错，最后 complete');
{
  const content = Buffer.from('PIPELINE-'.repeat(1000)); // 9000B
  const fileName = 'pipeline.bin';
  const chunkSize = 3000;
  const totalChunks = 3;
  const chunkBufs = [
    content.subarray(0, 3000),
    content.subarray(3000, 6000),
    content.subarray(6000),
  ];
  const chunkHashes = chunkBufs.map(sha256);
  const fileHash = sha256Text(chunkHashes.join(''));
  const fileId = sha256Text(`${fileName}:${content.length}:1700000000010:${chunkSize}`);

  const init = await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: content.length, chunkSize, totalChunks, fileHash: null }),
    { 'Content-Type': 'application/json' },
  );
  check('分阶段 init 201 且 fileHash=null/hashLocked=false',
    init.status === 201 &&
    init.json.file.fileHash === null &&
    init.json.file.hashLocked === false);

  // complete 在补哈希之前 → 明确报错，任务不卡死
  const early = await call('POST', `/${fileId}/complete`);
  check('未补哈希就 complete 返回 409 FILE_HASH_REQUIRED 且状态保持 uploading',
    early.status === 409 &&
    early.json.error.code === 'FILE_HASH_REQUIRED' &&
    (await call('GET', `/${fileId}/status`)).json.status === 'uploading');

  // 哈希未补报时即可接收分片（分阶段上传）
  const up0 = await call('POST', `/${fileId}/chunks/0?hash=${chunkHashes[0]}`, chunkBufs[0]);
  check('补哈希前允许上传分片', up0.status === 201);

  // 补报哈希
  const hashResp = await call(
    'POST',
    `/${fileId}/hash`,
    JSON.stringify({ fileHash }),
    { 'Content-Type': 'application/json' },
  );
  check('首次 /hash 201 locked=true changed=true',
    hashResp.status === 201 && hashResp.json.locked === true && hashResp.json.changed === true);

  // 重复相同哈希 → 幂等
  const hashAgain = await call(
    'POST',
    `/${fileId}/hash`,
    JSON.stringify({ fileHash }),
    { 'Content-Type': 'application/json' },
  );
  check('相同 /hash 重复提交 200 changed=false',
    hashAgain.status === 200 && hashAgain.json.changed === false);

  // 提交不同哈希 → 冲突锁定
  const hashConflict = await call(
    'POST',
    `/${fileId}/hash`,
    JSON.stringify({ fileHash: 'b'.repeat(64) }),
    { 'Content-Type': 'application/json' },
  );
  check('不同 /hash 返回 409 FILE_HASH_LOCKED',
    hashConflict.status === 409 && hashConflict.json.error.code === 'FILE_HASH_LOCKED');

  // init 带 fileHash 恢复时也能把空哈希补登记
  const otherId = sha256Text(`${fileName}-x:${content.length}:1700000000011:${chunkSize}`);
  await call(
    'POST',
    '/init',
    JSON.stringify({ fileId: otherId, fileName: `${fileName}-x`, fileSize: content.length, chunkSize, totalChunks, fileHash: null }),
    { 'Content-Type': 'application/json' },
  );
  const resumeWithHash = await call(
    'POST',
    '/init',
    JSON.stringify({ fileId: otherId, fileName: `${fileName}-x`, fileSize: content.length, chunkSize, totalChunks, fileHash }),
    { 'Content-Type': 'application/json' },
  );
  check('恢复 init 带哈希可补登记 hashLocked=true',
    resumeWithHash.status === 200 && resumeWithHash.json.file.hashLocked === true);

  // 补齐剩余分片
  await call('POST', `/${fileId}/chunks/1?hash=${chunkHashes[1]}`, chunkBufs[1]);
  await call('POST', `/${fileId}/chunks/2?hash=${chunkHashes[2]}`, chunkBufs[2]);
  const done = await call('POST', `/${fileId}/complete`);
  check('分阶段任务 complete 成功且聚合哈希一致',
    done.status === 200 && done.json.aggregateHash === fileHash);

  // 完成后 /hash 也被拒绝
  const hashAfterDone = await call(
    'POST',
    `/${fileId}/hash`,
    JSON.stringify({ fileHash }),
    { 'Content-Type': 'application/json' },
  );
  check('完成后补哈希返回 409', hashAfterDone.status === 409);
}

/* ---------------- 场景 7：并发安全——complete 期间的分片/重复 complete 被拒绝 ---------------- */
console.log('场景 7：并发：complete 与分片上传/二次 complete 竞争，merging 状态必须拦截');
{
  const fileName = 'race.bin';
  const chunkSize = 4;
  const content = Buffer.from('abcdEFGH'); // 8B → 2 片
  const hashes = [sha256(content.subarray(0, 4)), sha256(content.subarray(4))];
  const fileHash = sha256Text(hashes.join(''));
  const fileId = sha256Text(`${fileName}:${content.length}:1700000000012:${chunkSize}`);
  await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: content.length, chunkSize, totalChunks: 2, fileHash }),
    { 'Content-Type': 'application/json' },
  );
  await call('POST', `/${fileId}/chunks/0?hash=${hashes[0]}`, content.subarray(0, 4));
  await call('POST', `/${fileId}/chunks/1?hash=${hashes[1]}`, content.subarray(4));

  // 同步并发两次 complete（mock DB 下无真锁，但状态机必须接受其一）
  const [c1, c2] = await Promise.all([
    call('POST', `/${fileId}/complete`),
    call('POST', `/${fileId}/complete`),
  ]);
  const codes = [c1.status, c2.status].sort().join(',');
  check(
    `并发 complete 结果为 200+409（实际 ${c1.status},${c2.status}）`,
    c1.status === 200 && c2.status === 409 && c2.json.error.code === 'FILE_VERIFYING',
  );
  check('状态码组合校验: ' + codes, codes === '200,409');

  // merging/completed 后上传均被拒绝（completed → FILE_ALREADY_VERIFIED）
  const upAfter = await call('POST', `/${fileId}/chunks/0?hash=${hashes[0]}`, content.subarray(0, 4));
  check('完成后并发残留上传被拒绝 409', upAfter.status === 409);
}

/* ---------------- 场景 8：分阶段任务完成时仍能抓坏片（磁盘篡改） ---------------- */
console.log('场景 8：分阶段流程下篡改磁盘分片，complete 强校验依旧检出');
{
  const fileName = 'pipeline-tampered.bin';
  const chunkSize = 8;
  const content = Buffer.from('ZZZZZZZzyyyyyyyy'); // 16B → 2 片
  const hashes = [sha256(content.subarray(0, 8)), sha256(content.subarray(8))];
  const fileHash = sha256Text(hashes.join(''));
  const fileId = sha256Text(`${fileName}:${content.length}:1700000000013:${chunkSize}`);

  await call(
    'POST',
    '/init',
    JSON.stringify({ fileId, fileName, fileSize: content.length, chunkSize, totalChunks: 2, fileHash: null }),
    { 'Content-Type': 'application/json' },
  );
  await call('POST', `/${fileId}/chunks/0?hash=${hashes[0]}`, content.subarray(0, 8));
  await call('POST', `/${fileId}/chunks/1?hash=${hashes[1]}`, content.subarray(8));
  await call('POST', `/${fileId}/hash`, JSON.stringify({ fileHash }), {
    'Content-Type': 'application/json',
  });

  const part = path.join(process.env.STORAGE_DIR, 'cas', hashes[1].slice(0, 2), `${hashes[1]}.part`);
  await fsp.writeFile(part, Buffer.from('TAMPERED'));

  const done = await call('POST', `/${fileId}/complete`);
  check('分阶段任务篡改分片仍被 422 检出',
    done.status === 422 && done.json.error.code === 'CHUNK_HASH_MISMATCH');
  const status = await call('GET', `/${fileId}/status`);
  check('检出后状态回退 uploading，允许重传修复', status.json.status === 'uploading');
}

/* ---------------- 场景 9：相同大文件第二次秒传（零上传） ---------------- */
console.log('场景 9：相同文件不同 fileId，带清单 init 直接秒传，零上传');
{
  // 12 字节，3 片
  const content = Buffer.from('ABCDEFGHIJKL');
  const chunkSize = 4;
  const parts = [content.subarray(0, 4), content.subarray(4, 8), content.subarray(8, 12)];
  const hashes = parts.map(sha256);
  const agg = sha256Text(hashes.join(''));
  const mergedHash = sha256(content);

  const idA = sha256Text(`movie-a.bin:${content.length}:1700000001001:${chunkSize}`);
  const initA = await call(
    'POST',
    '/init',
    JSON.stringify({
      fileId: idA, fileName: 'movie-a.bin', fileSize: content.length, chunkSize,
      totalChunks: 3, fileHash: agg, chunkHashes: hashes,
    }),
    { 'Content-Type': 'application/json' },
  );
  check('文件A 首次无秒传', initA.status === 201 && initA.json.instant === false);

  for (let i = 0; i < 3; i++) {
    await call('POST', `/${idA}/chunks/${i}?hash=${hashes[i]}`, parts[i]);
  }
  const doneA = await call('POST', `/${idA}/complete`);
  check('文件A complete 成功', doneA.status === 200);
  const casPath = path.join(process.env.STORAGE_DIR, 'cas', hashes[0].slice(0, 2), `${hashes[0]}.part`);
  check('CAS 物理分片按内容寻址落盘', await fsp.access(casPath).then(() => true).catch(() => false));

  // 文件B：内容完全相同（同聚合哈希），但 fileId/文件名不同 → 秒传
  const idB = sha256Text(`movie-copy.bin:${content.length}:1700000001002:${chunkSize}`);
  const initB = await call(
    'POST',
    '/init',
    JSON.stringify({
      fileId: idB, fileName: 'movie-copy.bin', fileSize: content.length, chunkSize,
      totalChunks: 3, fileHash: agg, chunkHashes: hashes,
    }),
    { 'Content-Type': 'application/json' },
  );
  check('文件B 带清单 init 秒传 instant=true/status=completed',
    initB.status === 200 && initB.json.instant === true && initB.json.file.status === 'completed');
  check('秒传返回已有合并哈希', initB.json.file.mergedHash === mergedHash);
  check('秒传返回全部分片关联', initB.json.uploadedChunks.length === 3);

  // 不发任何分片直接 complete/下载：文件B 应已可下载（共享合并产物）
  const dl = await fetch(`http://localhost:${port}/api/files/${idB}/download`);
  const dlBuf = Buffer.from(await dl.arrayBuffer());
  check('秒传文件可直接下载且字节一致',
    dl.status === 200 && Buffer.compare(dlBuf, content) === 0);

  // precheck 只读接口也应返回 instant
  const pre = await call(
    'POST',
    '/precheck',
    JSON.stringify({ fileHash: agg, chunkHashes: hashes }),
    { 'Content-Type': 'application/json' },
  );
  check('precheck 返回 instant=true', pre.json.instant === true);

  // 全引用都在 → GC 不删任何物理分片
  const gc1 = await gcCall(0);
  check('两个文件引用期间 GC 删除 0 个分片', gc1.json.removedChunks === 0);
}

/* ---------------- 场景 10：跨文件共享部分分片，物理只存一份 ---------------- */
console.log('场景 10：不同文件共享部分相同内容分片（去重）');
{
  const shared = Buffer.from('SHARED-PAYLOAD!!'); // 16B
  const onlyA = Buffer.from('AAAA-suffix-data'); // 16B
  const onlyB = Buffer.from('BBBB-suffix-data'); // 16B
  const chunkSize = 16;
  const hShared = sha256(shared);
  const hOnlyA = sha256(onlyA);
  const hOnlyB = sha256(onlyB);

  const contentA = Buffer.concat([shared, onlyA]);
  const contentB = Buffer.concat([shared, onlyB]);
  const hashesA = [hShared, hOnlyA];
  const hashesB = [hShared, hOnlyB];
  const aggA = sha256Text(hashesA.join(''));
  const aggB = sha256Text(hashesB.join(''));
  const idA = sha256Text(`doc-A:${contentA.length}:1700000002001:${chunkSize}`);
  const idB = sha256Text(`doc-B:${contentB.length}:1700000002002:${chunkSize}`);

  await call('POST', '/init', JSON.stringify({
    fileId: idA, fileName: 'doc-A.bin', fileSize: contentA.length, chunkSize,
    totalChunks: 2, fileHash: aggA, chunkHashes: hashesA,
  }), { 'Content-Type': 'application/json' });
  await call('POST', `/${idA}/chunks/0?hash=${hShared}`, shared);
  await call('POST', `/${idA}/chunks/1?hash=${hOnlyA}`, onlyA);
  const doneA = await call('POST', `/${idA}/complete`);
  check('文件A 完成', doneA.status === 200);

  // 文件B 上传：分片0 是去重命中（同 chunkHash 物理已存在），分片1 全新
  await call('POST', '/init', JSON.stringify({
    fileId: idB, fileName: 'doc-B.bin', fileSize: contentB.length, chunkSize,
    totalChunks: 2, fileHash: aggB, chunkHashes: hashesB,
  }), { 'Content-Type': 'application/json' });
  const upShared = await call('POST', `/${idB}/chunks/0?hash=${hShared}`, shared);
  check('共享分片上传返回 dedup=true（物理复用，不重复落盘）',
    upShared.status === 201 && upShared.json.dedup === true);
  const upNew = await call('POST', `/${idB}/chunks/1?hash=${hOnlyB}`, onlyB);
  check('新分片 dedup=false', upNew.json.dedup === false);
  const doneB = await call('POST', `/${idB}/complete`);
  check('文件B 完成', doneB.status === 200);

  // 物理只有一份共享分片
  const casShared = path.join(process.env.STORAGE_DIR, 'cas', hShared.slice(0, 2), `${hShared}.part`);
  check('共享分片物理只存一份', await fsp.access(casShared).then(() => true).catch(() => false));
  const dlB = await fetch(`http://localhost:${port}/api/files/${idB}/download`);
  check('文件B 下载内容正确（含共享头）',
    Buffer.compare(Buffer.from(await dlB.arrayBuffer()), contentB) === 0);

  // 保存给场景 11 使用（全局变量）
  globalThis.__shareCase = { idA, idB, hShared, hOnlyA, hOnlyB, chunkSize, contentB, aggB };
}

/* ---------------- 场景 11：删 A 不影响其它文件的 complete/下载（引用计数保护） ---------------- */
console.log('场景 11：删除共享方 A 后，未完成文件仍可 complete，已完成文件仍可下载');
{
  const c = globalThis.__shareCase;

  // 再建第三个文件 B2，内容与 B 相同，但此刻不带清单/哈希，处于上传中
  const idB2 = sha256Text(`doc-B-pending:${c.contentB.length}:1700000003001:${c.chunkSize}`);
  await call('POST', '/init', JSON.stringify({
    fileId: idB2, fileName: 'doc-B-pending.bin', fileSize: c.contentB.length,
    chunkSize: c.chunkSize, totalChunks: 2, fileHash: null,
  }), { 'Content-Type': 'application/json' });
  // 两个分片都是全局已存在内容（dedup 命中）
  const up0 = await call('POST', `/${idB2}/chunks/0?hash=${c.hShared}`, c.contentB.subarray(0, 16));
  const up1 = await call('POST', `/${idB2}/chunks/1?hash=${c.hOnlyB}`, c.contentB.subarray(16));
  check('B2 上传命中全局去重（dedup=true）', up0.json.dedup === true && up1.json.dedup === true);

  // 删除 A（引用 shared + onlyA）。B、B2 都引用 shared，故 shared 不能被回收
  const delA = await call('DELETE', `/${c.idA}`);
  check('删除 A 成功，仅解除引用（physicalRemoved=false）',
    delA.status === 200 && delA.json.deleted === true && delA.json.physicalRemoved === false);
  check('A 解除了 2 个分片引用', delA.json.dereferencedChunks === 2);

  // 立即 GC（minAge=0）：onlyA 归零可回收；shared 仍被 B、B2 引用
  const gc = await gcCall(0);
  check('删 A 后 GC 只回收归零的 onlyA（shared 仍被引用）', gc.json.removedChunks === 1);

  // A 已删，但 B2 仍可补哈希并 complete（强校验读的是仍存活的共享 CAS 分片）
  const aggB2 = sha256Text([c.hShared, c.hOnlyB].join(''));
  const lock = await call('POST', `/${idB2}/hash`, JSON.stringify({ fileHash: aggB2 }), {
    'Content-Type': 'application/json',
  });
  check('B2 删除 A 后补报哈希成功', lock.status === 201);
  const doneB2 = await call('POST', `/${idB2}/complete`);
  check('删除 A 后 B2 仍可 complete（共享分片存活）', doneB2.status === 200);

  // 已完成的 B 仍可下载
  const dlB = await fetch(`http://localhost:${port}/api/files/${c.idB}/download`);
  check('删除 A 后 B 仍可下载且内容正确',
    dlB.status === 200 &&
    Buffer.compare(Buffer.from(await dlB.arrayBuffer()), c.contentB) === 0);

  const casShared = path.join(process.env.STORAGE_DIR, 'cas', c.hShared.slice(0, 2), `${c.hShared}.part`);
  check('被引用的共享分片物理仍在', await fsp.access(casShared).then(() => true).catch(() => false));
  const casOnlyA = path.join(process.env.STORAGE_DIR, 'cas', c.hOnlyA.slice(0, 2), `${c.hOnlyA}.part`);
  check('归零的 onlyA 物理分片已被 GC 删除',
    await fsp.access(casOnlyA).then(() => false).catch(() => true));

  globalThis.__shareCase.idB2 = idB2;
}

/* ---------------- 场景 12：引用归零后 GC 删除物理，合并产物同步回收 ---------------- */
console.log('场景 12：最后一个引用删除后，GC 回收分片与合并产物');
{
  const c = globalThis.__shareCase;
  const delB = await call('DELETE', `/${c.idB}`);
  check('删除 B 成功', delB.status === 200);
  // B2 与 B 内容相同，仍引用 shared/onlyB，故此刻 GC 不回收
  const gcBusy = await gcCall(0);
  check('B2 仍引用期间 GC 不回收（removedChunks=0）', gcBusy.json.removedChunks === 0);

  // 删除最后一个引用 B2 后再 GC
  const delB2 = await call('DELETE', `/${c.idB2}`);
  check('删除 B2 成功', delB2.status === 200);
  const gc = await gcCall(0);
  check('所有引用删除后 GC 回收剩余 2 个分片', gc.json.removedChunks === 2);
  check('合并产物引用归零后被 GC 回收', gc.json.removedMerged >= 1);
  const casShared = path.join(process.env.STORAGE_DIR, 'cas', c.hShared.slice(0, 2), `${c.hShared}.part`);
  check('共享分片物理最终被删除',
    await fsp.access(casShared).then(() => false).catch(() => true));

  // 删除不存在的文件 → 404
  const ghost = await call('DELETE', `/${c.idA}`);
  check('重复删除返回 404', ghost.status === 404);
}

/* ---------------- 场景 13：同一 chunkHash 并发首传幂等，只落盘一份、计数正确 ---------------- */
console.log('场景 13：两个新文件并发首传相同 chunkHash，幂等且引用计数正确');
{
  const body = Buffer.from('concurrent-identical-chunk!!!'); // 30B 一片
  const h = sha256(body);
  const chunkSize = body.length;
  const agg = sha256Text(h);
  const idX = sha256Text(`race-X:${body.length}:1700000004001:${chunkSize}`);
  const idY = sha256Text(`race-Y:${body.length}:1700000004002:${chunkSize}`);

  await call('POST', '/init', JSON.stringify({
    fileId: idX, fileName: 'x.bin', fileSize: body.length, chunkSize,
    totalChunks: 1, fileHash: agg, chunkHashes: [h],
  }), { 'Content-Type': 'application/json' });
  await call('POST', '/init', JSON.stringify({
    fileId: idY, fileName: 'y.bin', fileSize: body.length, chunkSize,
    totalChunks: 1, fileHash: agg, chunkHashes: [h],
  }), { 'Content-Type': 'application/json' });

  // 并发首传同一 hash（不同 fileId、同序号 0）
  const [rx, ry] = await Promise.all([
    call('POST', `/${idX}/chunks/0?hash=${h}`, body),
    call('POST', `/${idY}/chunks/0?hash=${h}`, body),
  ]);
  check('两个并发首传都成功（201）', rx.status === 201 && ry.status === 201);

  // 两边都能 complete（共享同一物理分片，引用计数=2）
  const cx = await call('POST', `/${idX}/complete`);
  const cy = await call('POST', `/${idY}/complete`);
  check('两个并发文件都 complete 成功', cx.status === 200 && cy.status === 200);

  // 删一个，GC 不能删物理分片（还被另一个引用）
  await call('DELETE', `/${idX}`);
  const gc = await gcCall(0);
  check('删其一后并发共享分片不被 GC（removedChunks=0）', gc.json.removedChunks === 0);
  const dlY = await fetch(`http://localhost:${port}/api/files/${idY}/download`);
  check('剩余文件仍可下载', dlY.status === 200);

  // 同文件重复传同 hash：幂等 skipped（用独立内容，避免触发秒传）
  const zb = Buffer.from('unique-idempotent-content!!!');
  const hz = sha256(zb);
  const idZ = sha256Text(`race-Z:${zb.length}:1700000004003:${zb.length}`);
  await call('POST', '/init', JSON.stringify({
    fileId: idZ, fileName: 'z.bin', fileSize: zb.length, chunkSize: zb.length,
    totalChunks: 1, fileHash: null,
  }), { 'Content-Type': 'application/json' });
  const z1 = await call('POST', `/${idZ}/chunks/0?hash=${hz}`, zb);
  const z2 = await call('POST', `/${idZ}/chunks/0?hash=${hz}`, zb);
  check('同文件重复传：首传 201，重传 skipped=true',
    z1.status === 201 && z2.status === 200 && z2.json.skipped === true);
}

/* ---------------- 场景 14：未完成文件禁止下载；路径不可遍历 ---------------- */
console.log('场景 14：未完成文件下载被拒；非法 hash/fileId 返回 400');
{
  const content = Buffer.from('not-ready-yet!!');
  const chunkSize = content.length;
  const h = sha256(content);
  const id = sha256Text(`pending-dl:${content.length}:1700000005001:${chunkSize}`);
  await call('POST', '/init', JSON.stringify({
    fileId: id, fileName: 'pending.bin', fileSize: content.length, chunkSize,
    totalChunks: 1, fileHash: sha256Text(h), chunkHashes: [h],
  }), { 'Content-Type': 'application/json' });
  await call('POST', `/${id}/chunks/0?hash=${h}`, content);
  // 未 complete
  const dl = await fetch(`http://localhost:${port}/api/files/${id}/download`);
  check('未完成文件下载返回 409 FILE_NOT_READY',
    dl.status === 409);

  // 非法 fileId / hash
  const badId = await fetch(`http://localhost:${port}/api/files/../etc/passwd/status`);
  check('路径遍历被路由/校验拦截（非 200）', badId.status !== 200);
  const badHash = await call('POST', '/not-a-valid-hash/chunks/0?hash=x', Buffer.from('a'));
  check('非法 fileId 返回 400', badHash.status === 400);
}

/* ---------------- 场景 15：bug1 —— 全局 hits 只关联不传字节，否则 complete 缺片 ---------------- */
console.log('场景 15：init.hits 命中片必须经 /link 建立本文件关联，裸跳过会 CHUNKS_INCOMPLETE');
{
  // 捐赠文件 D：3 个 8B 分片，全部上传完成
  const dParts = [Buffer.from('AAAA-001'), Buffer.from('BBBB-002'), Buffer.from('CCCC-003')];
  const dContent = Buffer.concat(dParts);
  const dHashes = dParts.map(sha256);
  const dAgg = sha256Text(dHashes.join(''));
  const idD = sha256Text(`donor15:${dContent.length}:1700000006001:8`);
  await call('POST', '/init', JSON.stringify({
    fileId: idD, fileName: 'donor15.bin', fileSize: dContent.length, chunkSize: 8,
    totalChunks: 3, fileHash: dAgg,
  }), { 'Content-Type': 'application/json' });
  for (let i = 0; i < 3; i++) {
    await call('POST', `/${idD}/chunks/${i}?hash=${dHashes[i]}`, dParts[i]);
  }
  await call('POST', `/${idD}/complete`);

  // 新文件 F：分片 0、2 与 D 相同（全局命中），分片 1 不同
  const xPart = Buffer.from('XXXX-new');
  const fParts = [dParts[0], xPart, dParts[2]];
  const fContent = Buffer.concat(fParts);
  const fHashes = fParts.map(sha256);
  const fAgg = sha256Text(fHashes.join(''));
  const idF = sha256Text(`f15:${fContent.length}:1700000006002:8`);
  const fInit = await call('POST', '/init', JSON.stringify({
    fileId: idF, fileName: 'f15.bin', fileSize: fContent.length, chunkSize: 8,
    totalChunks: 3, fileHash: fAgg, chunkHashes: fHashes,
  }), { 'Content-Type': 'application/json' });
  check('F 非秒传（聚合哈希不同）', fInit.json.instant === false);
  check('hits 标出全局已存在的 #0/#2',
    fInit.json.hits[0] === fHashes[0] && fInit.json.hits[2] === fHashes[2] &&
    fInit.json.hits[1] === undefined);

  // 错误演示：一片都不关联/不上传就 complete → CHUNKS_INCOMPLETE
  const premature = await call('POST', `/${idF}/complete`);
  check('裸跳过 hits 直接 complete → 409 CHUNKS_INCOMPLETE（bug 复现路径）',
    premature.status === 409 && premature.json.error.code === 'CHUNKS_INCOMPLETE');

  // /link 不存在的 CAS 哈希 → 409 CAS_CHUNK_NOT_FOUND
  const ghost = sha256(Buffer.from('never-uploaded!'));
  const linkGhost = await call('POST', `/${idF}/chunks/0/link?hash=${ghost}`);
  check('link 全局不存在的哈希 → 409 CAS_CHUNK_NOT_FOUND',
    linkGhost.status === 409 && linkGhost.json.error.code === 'CAS_CHUNK_NOT_FOUND');

  // #0/#2 只关联（零字节请求体）
  const l0 = await call('POST', `/${idF}/chunks/0/link?hash=${fHashes[0]}`);
  check('link #0 成功 201 linked=true', l0.status === 201 && l0.json.linked === true);
  const l0again = await call('POST', `/${idF}/chunks/0/link?hash=${fHashes[0]}`);
  check('重复 link 同哈希幂等 skipped=true 且不增加引用',
    l0again.status === 200 && l0again.json.skipped === true);
  // #1 走真实上传（新内容）
  const up1 = await call('POST', `/${idF}/chunks/1?hash=${fHashes[1]}`, xPart);
  check('差异片 #1 正常上传 201', up1.status === 201);
  // #2 关联
  const l2 = await call('POST', `/${idF}/chunks/2/link?hash=${fHashes[2]}`);
  check('link #2 成功', l2.status === 201);

  // 此时 F 关联齐全，complete 成功（强校验读的是共享 CAS 物理片）
  const fDone = await call('POST', `/${idF}/complete`);
  check('link + 差异片上传后 F complete 成功', fDone.status === 200);
  const dlF = await fetch(`http://localhost:${port}/api/files/${idF}/download`);
  check('F 下载内容正确（共享片来自 D）',
    Buffer.compare(Buffer.from(await dlF.arrayBuffer()), fContent) === 0);

  // 物理共享分片只存一份
  const sharedAbs = path.join(process.env.STORAGE_DIR, 'cas', fHashes[0].slice(0, 2), `${fHashes[0]}.part`);
  check('共享分片物理只存一份', await fsp.access(sharedAbs).then(() => true).catch(() => false));

  // 删除 D：F 仍引用 #0/#2，GC 不能回收它们
  await call('DELETE', `/${idD}`);
  let gc = await gcCall(0);
  check('删 D 后 GC 不回收 F 仍引用的共享片（仅 D 独有片归零）',
    gc.json.removedChunks === 1);
  const dlF2 = await fetch(`http://localhost:${port}/api/files/${idF}/download`);
  check('删 D 后 F 仍可下载', dlF2.status === 200);
  // 再删 F，剩余 2 个物理片（#0 共享、#1 新片；#2 与 #0 是不同哈希也归零）全部回收
  await call('DELETE', `/${idF}`);
  gc = await gcCall(0);
  check('F 也删除后其全部引用归零，GC 回收剩余物理片', gc.json.removedChunks === 3);
}

/* ---------------- 场景 16：bug2 —— 先上传若干片再秒传，引用计数必须精确 ---------------- */
console.log('场景 16：秒传仲裁竞态：在途片已建关联，秒传不得重复 ref_count+1（GC 应能全部回收）');
{
  // 捐赠文件 G（另一 fileId）完整上传：4 片
  const gParts = [0, 1, 2, 3].map((i) => Buffer.from(`G-chunk-${i}!!`)); // 11B，统一大小
  // 统一为 11B 以便整除：补齐
  for (let i = 0; i < gParts.length; i++) {
    gParts[i] = Buffer.concat([gParts[i], Buffer.alloc(0)]);
  }
  const gContent = Buffer.concat(gParts);
  const gHashes = gParts.map(sha256);
  const gAgg = sha256Text(gHashes.join(''));
  const idG = sha256Text(`donor16:${gContent.length}:1700000007001:11`);
  await call('POST', '/init', JSON.stringify({
    fileId: idG, fileName: 'donor16.bin', fileSize: gContent.length, chunkSize: 11,
    totalChunks: 4, fileHash: gAgg,
  }), { 'Content-Type': 'application/json' });
  for (let i = 0; i < 4; i++) {
    await call('POST', `/${idG}/chunks/${i}?hash=${gHashes[i]}`, gParts[i]);
  }
  await call('POST', `/${idG}/complete`);

  // 目标文件 H：先以“无哈希分阶段”init，只上传 #0、#1（模拟边算边传的在途片）
  const idH = sha256Text(`h16:${gContent.length}:1700000007002:11`);
  await call('POST', '/init', JSON.stringify({
    fileId: idH, fileName: 'h16.bin', fileSize: gContent.length, chunkSize: 11,
    totalChunks: 4, fileHash: null,
  }), { 'Content-Type': 'application/json' });
  await call('POST', `/${idH}/chunks/0?hash=${gHashes[0]}`, gParts[0]);
  await call('POST', `/${idH}/chunks/1?hash=${gHashes[1]}`, gParts[1]);
  check('H 秒传前已存在 2 个在途片关联',
    (await call('GET', `/${idH}/chunks`)).json.chunks.length === 2);

  // 此时带完整清单 init（模拟 onAllHashed 秒传仲裁）。服务端必须对 #0/#1 幂等，
  // 只对 #2/#3 新增引用，不能把 #0/#1 重复 +1。
  const hInstant = await call('POST', '/init', JSON.stringify({
    fileId: idH, fileName: 'h16.bin', fileSize: gContent.length, chunkSize: 11,
    totalChunks: 4, fileHash: gAgg, chunkHashes: gHashes,
  }), { 'Content-Type': 'application/json' });
  check('带清单 init 秒传成功 instant=true',
    hInstant.status === 200 && hInstant.json.instant === true);
  check('秒传后 H 拥有完整 4 片关联（在途 2 片被幂等吸收）',
    hInstant.json.uploadedChunks.length === 4);
  const dlH = await fetch(`http://localhost:${port}/api/files/${idH}/download`);
  check('H 秒传后可下载且内容正确',
    Buffer.compare(Buffer.from(await dlH.arrayBuffer()), gContent) === 0);

  // 关键断言：删除 G、H 后，每个 CAS 分片引用必须精确归零，GC 能删除全部 4 个物理片。
  // 若秒传对在途片重复 +1（引用虚高），这里会残留物理文件、removedChunks < 4。
  await call('DELETE', `/${idG}`);
  let gc = await gcCall(0);
  check('仅删 G 时 4 个分片仍被 H 引用，GC 删除 0', gc.json.removedChunks === 0);
  await call('DELETE', `/${idH}`);
  gc = await gcCall(0);
  check(`删 G+H 后引用精确归零，GC 回收全部 4 个物理片（实测 ${gc.json.removedChunks}，` +
    '若 <4 说明秒传对在途片重复计数导致虚高)',
    gc.json.removedChunks === 4);
  for (let i = 0; i < 4; i++) {
    const abs = path.join(process.env.STORAGE_DIR, 'cas', gHashes[i].slice(0, 2), `${gHashes[i]}.part`);
    const gone = await fsp.access(abs).then(() => false).catch(() => true);
    check(`物理片 #${i} 已从磁盘删除（引用不虚高）`, gone);
  }
}

/* ---------- 场景 17：GC 幽灵分片自愈 + GC 可重入/可恢复 ---------- */
console.log('场景 17：GC 留下「有行无文件」幽灵片时，去重上传自愈；GC 重复执行幂等');
{
  const parts = [Buffer.from('ghost-001!'), Buffer.from('ghost-002!'), Buffer.from('ghost-003!')]; // 10B
  const content = Buffer.concat(parts);
  const hashes = parts.map(sha256);
  const agg = sha256Text(hashes.join(''));
  const idA = sha256Text(`ghostA:${content.length}:1700000008001:10`);

  // 文件 A 正常上传完成（物理片齐全）
  await call('POST', '/init', JSON.stringify({
    fileId: idA, fileName: 'ghostA.bin', fileSize: content.length, chunkSize: 10,
    totalChunks: 3, fileHash: agg,
  }), { 'Content-Type': 'application/json' });
  for (let i = 0; i < 3; i++) {
    await call('POST', `/${idA}/chunks/${i}?hash=${hashes[i]}`, parts[i]);
  }
  await call('POST', `/${idA}/complete`);

  // 模拟“旧 GC 先删物理后回滚”：手工删掉 #1 的物理文件，但保留 cas_chunks 行
  const ghostRel = `cas/${hashes[1].slice(0, 2)}/${hashes[1]}.part`;
  await fsp.rm(path.join(process.env.STORAGE_DIR, ghostRel), { force: true });

  // 此刻文件 A 的 complete 已完成（状态 completed），不影响；
  // 关键：文件 B 去重重传 #1 时，库行存在但物理缺失，必须自愈重写而不是只 +ref
  const idB = sha256Text(`ghostB:${content.length}:1700000008002:10`);
  await call('POST', '/init', JSON.stringify({
    fileId: idB, fileName: 'ghostB.bin', fileSize: content.length, chunkSize: 10,
    totalChunks: 3, fileHash: agg, chunkHashes: hashes,
  }), { 'Content-Type': 'application/json' });

  // /link 对幽灵片应失败（只关联无法自愈，因为没有字节）→ CAS_CHUNK_NOT_FOUND
  const linkGhost = await call('POST', `/${idB}/chunks/1/link?hash=${hashes[1]}`);
  check('link 物理缺失的幽灵片被拒绝（不建立悬空关联）',
    linkGhost.status === 409 &&
    (linkGhost.json.error.code === 'CHUNK_FILE_MISSING' || linkGhost.json.error.code === 'CAS_CHUNK_NOT_FOUND'));

  // 带字节上传：库行存在但物理缺失 → 自愈重写，响应 healed=true
  const healUp = await call('POST', `/${idB}/chunks/1?hash=${hashes[1]}`, parts[1]);
  check('去重上传命中幽灵行时自愈重写（healed=true，201）',
    healUp.status === 201 && healUp.json.healed === true);
  check('物理文件已被重写', await fsp.access(path.join(process.env.STORAGE_DIR, ghostRel)).then(() => true).catch(() => false));

  // 其余两片 link（物理健康），B complete 成功——证明自愈后内容可读、强校验通过
  await call('POST', `/${idB}/chunks/0/link?hash=${hashes[0]}`);
  await call('POST', `/${idB}/chunks/2/link?hash=${hashes[2]}`);
  const bDone = await call('POST', `/${idB}/complete`);
  check('自愈后 B complete 成功（不再 CHUNK_FILE_MISSING）', bDone.status === 200);
  const dlB = await fetch(`http://localhost:${port}/api/files/${idB}/download`);
  check('自愈后 B 下载字节正确',
    Buffer.compare(Buffer.from(await dlB.arrayBuffer()), content) === 0);

  // GC 幂等：连续执行两次不应报错、不应误删被引用片
  const gc1 = await gcCall(0);
  check('被引用期间第一次 GC 删除 0 库行', gc1.json.removedChunks === 0);
  const gc2 = await gcCall(0);
  check('GC 重复执行幂等（仍删除 0，无报错）', gc2.json.removedChunks === 0);
  check('B 仍可下载',
    (await fetch(`http://localhost:${port}/api/files/${idB}/download`)).status === 200);
}

/* ---------- 场景 18：多个捐赠者中跳过损坏者，选中更老的完好捐赠者 ---------- */
console.log('场景 18：最新捐赠者关联不完整时，秒传逐个校验后选中更老的完好捐赠者');
{
  const { __mockDb } = await import('./mock-mysql.mjs');
  const parts = [Buffer.from('donorA-001'), Buffer.from('donorA-002')]; // 10B x2
  const content = Buffer.concat(parts);
  const hashes = parts.map(sha256);
  const agg = sha256Text(hashes.join(''));

  // 老捐赠者 OLD：完整上传
  const idOld = sha256Text(`donorOld:${content.length}:1700000009001:10`);
  await call('POST', '/init', JSON.stringify({
    fileId: idOld, fileName: 'old.bin', fileSize: content.length, chunkSize: 10,
    totalChunks: 2, fileHash: agg,
  }), { 'Content-Type': 'application/json' });
  for (let i = 0; i < 2; i++) {
    await call('POST', `/${idOld}/chunks/${i}?hash=${hashes[i]}`, parts[i]);
  }
  await call('POST', `/${idOld}/complete`);

  // 新捐赠者 NEW：先正常秒传（updated_at 更新、排在候选首位）
  const idNew = sha256Text(`donorNew:${content.length}:1700000009002:10`);
  const newInit = await call('POST', '/init', JSON.stringify({
    fileId: idNew, fileName: 'new.bin', fileSize: content.length, chunkSize: 10,
    totalChunks: 2, fileHash: agg, chunkHashes: hashes,
  }), { 'Content-Type': 'application/json' });
  check('NEW 首次秒传成功', newInit.json.instant === true);

  // 制造脏数据：删掉 NEW 的一个 file_chunks 关联（关联不完整），
  // 并把其 updated_at 推后，保证它在候选顺序中排第一。物理 CAS 仍完好（OLD 可用）。
  const mdb = __mockDb();
  mdb.fileChunks = mdb.fileChunks.filter(
    (l) => !(l.file_id === idNew && l.chunk_index === 1),
  );
  mdb.files.get(idNew).updated_at = new Date(Date.now() + 60_000);

  // 第三个文件：候选顺序 [NEW(脏), OLD(完好)]，必须跳过 NEW 选中 OLD
  const idInst = sha256Text(`instant18:${content.length}:1700000009003:10`);
  const instInit = await call('POST', '/init', JSON.stringify({
    fileId: idInst, fileName: 'inst.bin', fileSize: content.length, chunkSize: 10,
    totalChunks: 2, fileHash: agg, chunkHashes: hashes,
  }), { 'Content-Type': 'application/json' });
  check('最新捐赠者关联不完整时仍秒传（跳过脏候选、选中完好的 OLD）',
    instInit.json.instant === true);
  check('秒传建立完整 2 片关联', instInit.json.uploadedChunks.length === 2);
  const dl = await fetch(`http://localhost:${port}/api/files/${idInst}/download`);
  check('秒传文件可下载且字节正确',
    dl.status === 200 &&
    Buffer.compare(Buffer.from(await dl.arrayBuffer()), content) === 0);
}

server.close();
console.log(`\n全部 ${passed} 条断言通过 ✅`);
