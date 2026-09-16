/**
 * 回归：旧库 files.file_hash 为 NOT NULL 时，首次启动迁移会把它改为可空，
 * 之后分阶段 init(fileHash=null) 不再 ER_BAD_NULL_ERROR（真 MySQL 主路径可用）。
 *
 * MOCK_LEGACY_NOTNULL=1 让内存 mock 的列元数据初始为 NO（模拟旧 schema），
 * initDb() 内部的迁移应检测并 ALTER；脚本验证：
 *   - 首次 initDb 后 information_schema 显示 YES（迁移生效）
 *   - 历史 NOT NULL 数据保留、新任务允许 NULL 哈希
 *   - 迁移可重复执行（幂等）
 */
process.env.STORAGE_DIR = await (async () => {
  const fsp = await import('node:fs/promises');
  const os = await import('node:os');
  const path = await import('node:path');
  return fsp.mkdtemp(path.join(os.tmpdir(), 'chunk-migrate-'));
})();
process.env.MOCK_LEGACY_NOTNULL = '1';

const { initDb, getPool } = await import('../src/db.js');
const mock = await import('./mock-mysql.mjs');
const { ensureStorageDirs } = await import('../src/storage.js');
await ensureStorageDirs();
await initDb();

const pool = getPool();

function assert(cond, msg) {
  if (!cond) {
    console.error('✗ ' + msg);
    process.exit(1);
  }
  console.log('  ✓ ' + msg);
}

const DB_NAME = process.env.DB_NAME || 'chunk_verify';

// MOCK_LEGACY_NOTNULL=1 模拟旧库：首次启动必须执行一次 ALTER 把 file_hash 改可空
const alters = mock.__mockDb().alterLog.filter(
  (a) => a.table === 'files' && a.column === 'file_hash',
);
assert(alters.length === 1, `旧 NOT NULL 列启动时恰好执行一次 ALTER（实测 ${alters.length}）`);
assert(/NULL/i.test(alters[0].def) && !/NOT NULL/i.test(alters[0].def), 'ALTER 定义为可空');

// 首次 initDb 后列应为 YES
let [cols] = await pool.query(
  `SELECT IS_NULLABLE AS n FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'files' AND COLUMN_NAME = 'file_hash'`,
  [DB_NAME],
);
assert(cols.length === 1 && cols[0].n === 'YES', '旧 NOT NULL 列经启动迁移变为 NULLABLE');

// 历史数据（带哈希）保留
const crypto = await import('node:crypto');
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const oldId = sha('legacy-row');
await pool.query(
  `INSERT INTO files (id,file_name,file_size,chunk_size,total_chunks,file_hash,status)
   VALUES (?,?,?,?,?,?,?)`,
  [oldId, 'legacy.bin', '10', 10, 1, sha('x'), 'uploading'],
);

// 迁移幂等：重启式再跑一次，不应再产生 ALTER、列仍为 YES
await initDb();
assert(
  mock.__mockDb().alterLog.filter((a) => a.table === 'files' && a.column === 'file_hash').length === 1,
  '已是 NULLABLE 时重启不再重复 ALTER（幂等）',
);
[cols] = await pool.query(
  `SELECT IS_NULLABLE AS n FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'files' AND COLUMN_NAME = 'file_hash'`,
  [DB_NAME],
);
assert(cols[0].n === 'YES', '迁移重复执行后列保持 NULLABLE');

// 关键：分阶段 init(fileHash=null) 在“迁移后的旧库”上成功（走真实 HTTP）
const { createApp } = await import('../src/app.js');
const app = createApp();
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const base = `http://localhost:${server.address().port}/api/files`;
const J = (o) => ({
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(o),
});

const part = Buffer.from('1234567890'); // 10B
const partHash = sha(part);
const newId = sha('staged-after-migration');
let r = await fetch(`${base}/init`, J({
  fileId: newId, fileName: 'new.bin', fileSize: 10, chunkSize: 10,
  totalChunks: 1, fileHash: null,
}));
assert(r.status === 201, '迁移后分阶段 init(fileHash=null) 成功（不再 ER_BAD_NULL_ERROR）');

r = await fetch(`${base}/${newId}/chunks/0?hash=${partHash}`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/octet-stream' },
  body: part,
});
assert(r.status === 201, '分阶段任务上传分片成功');

const agg = sha(partHash);
await fetch(`${base}/${newId}/hash`, J({ fileHash: agg }));
r = await fetch(`${base}/${newId}/complete`, { method: 'POST' });
assert(r.status === 200, '补报聚合哈希后 complete 成功');

// 历史行仍在
const [oldRows] = await pool.query('SELECT id, file_hash FROM files WHERE id = ?', [oldId]);
assert(oldRows.length === 1 && oldRows[0].file_hash !== null, '历史带哈希数据迁移后保留');

server.close();
console.log('\n旧库迁移 + 分阶段主路径回归通过 ✅');

