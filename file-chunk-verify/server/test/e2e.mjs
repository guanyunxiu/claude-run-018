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

  // 直接篡改磁盘上的 0 号分片
  const storage = process.env.STORAGE_DIR;
  const part = path.join(storage, 'chunks', fileId, '00000000.part');
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

server.close();
console.log(`\n全部 ${passed} 条断言通过 ✅`);
