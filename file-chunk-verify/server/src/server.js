import { createApp } from './app.js';
import { config } from './config.js';
import { initDb } from './db.js';
import { ensureStorageDirs } from './storage.js';

async function main() {
  await ensureStorageDirs();
  await initDb();
  console.log(`[db] 已连接 MySQL ${config.db.host}:${config.db.port}/${config.db.database}`);
  console.log(`[storage] 分片/合并文件目录: ${config.storageDir}`);

  const app = createApp();
  app.listen(config.port, () => {
    console.log(`[server] 分片校验服务已启动: http://localhost:${config.port}`);
  });
}

main().catch((err) => {
  console.error('启动失败:', err);
  process.exit(1);
});
