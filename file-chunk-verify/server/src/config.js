/**
 * 配置加载：优先读环境变量，其次尝试加载 server/.env（零依赖的极简 .env parser）。
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const serverRoot = path.resolve(__dirname, '..');

function loadDotEnv() {
  const envPath = path.join(serverRoot, '.env');
  if (!fs.existsSync(envPath)) return;
  const text = fs.readFileSync(envPath, 'utf8');
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (process.env[key] === undefined) process.env[key] = value;
  }
}

loadDotEnv();

const int = (v, fallback) => {
  const n = Number.parseInt(v ?? '', 10);
  return Number.isFinite(n) ? n : fallback;
};

export const config = {
  port: int(process.env.PORT, 3000),
  db: {
    host: process.env.DB_HOST || '127.0.0.1',
    port: int(process.env.DB_PORT, 3306),
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD ?? 'root',
    database: process.env.DB_NAME || 'chunk_verify',
  },
  storageDir: path.isAbsolute(process.env.STORAGE_DIR || '')
    ? process.env.STORAGE_DIR
    : path.join(serverRoot, process.env.STORAGE_DIR || 'storage'),
  chunkLimitBytes: int(process.env.CHUNK_LIMIT_BYTES, 64 * 1024 * 1024),
  serverRoot,

  /* ---- 物理存储后端：s3(MinIO/Amazon S3) | local（仅单测/单机） ---- */
  objectStore: (process.env.OBJECT_STORE || 'local').toLowerCase(), // s3 | local
  s3: {
    endpoint: process.env.S3_ENDPOINT || 'http://127.0.0.1:9000',
    region: process.env.S3_REGION || 'us-east-1',
    bucket: process.env.S3_BUCKET || 'chunk-verify',
    accessKeyId: process.env.S3_ACCESS_KEY || 'minioadmin',
    secretAccessKey: process.env.S3_SECRET_KEY || 'minioadmin',
    forcePathStyle: process.env.S3_FORCE_PATH_STYLE !== '0', // MinIO 必须 true
  },

  /* ---- 跨机器协调：redis（默认） | memory（测试：进程内共享） | none ---- */
  lock: {
    driver: (process.env.LOCK_DRIVER || 'memory').toLowerCase(),
    redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
    ttlMs: int(process.env.LOCK_TTL_MS, 60_000),
  },
};
