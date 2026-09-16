import { createApp } from './app.js';
import { config } from './config.js';
import { initDb } from './db.js';
import { ensureStorageDirs } from './storage.js';
import { objectStore } from './store/index.js';
import { getLocker } from './store/locker.js';

async function main() {
  const store = objectStore();
  await store.init(); // local 建目录 / S3 建 bucket
  await initDb();

  const locker = await getLocker();
  if (locker) {
    // 触发一次锁能力自检（redis 驱动在首次 acquire 时才真正连通）
    const lease = await locker.acquire('boot', { ttlMs: 1000 });
    if (lease) await lease.release();
  }

  console.log(`[db] 已连接 MySQL ${config.db.host}:${config.db.port}/${config.db.database}`);
  if (store.kind === 's3') {
    console.log(`[storage] 对象存储 S3/MinIO endpoint=${config.s3.endpoint} bucket=${config.s3.bucket}`);
  } else {
    console.log(`[storage] 本机磁盘模式: ${config.storageDir}`);
  }
  console.log(`[lock] 跨机器协调: ${locker ? locker.kind : 'none'}`);

  const app = createApp();
  app.listen(config.port, () => {
    console.log(`[server] 分片校验服务已启动: http://localhost:${config.port}`);
  });
}

main().catch((err) => {
  console.error('启动失败:', err);
  process.exit(1);
});
