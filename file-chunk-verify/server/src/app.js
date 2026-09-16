/**
 * Express 应用组装。全局不挂 body parser（分片是 raw 二进制），
 * 仅在 /init 等 JSON 接口上单独启用 express.json()。
 */
import express from 'express';
import cors from 'cors';
import filesRouter from './routes/files.js';
import { config } from './config.js';

export function createApp() {
  const app = express();
  app.disable('x-powered-by');
  app.use(cors());

  // 健康检查（不连库）
  app.get('/health', (_req, res) => {
    res.json({ ok: true, ts: new Date().toISOString() });
  });

  // JSON 接口（分片上传路由自带 raw parser，不受影响）
  app.use('/api/files/init', express.json({ limit: '1mb' }));
  app.use('/api/files', filesRouter);

  // 404
  app.use((_req, res) => {
    res.status(404).json({ error: { code: 'NOT_FOUND', message: '接口不存在' } });
  });

  // 统一错误处理
  // eslint-disable-next-line no-unused-vars
  app.use((err, _req, res, _next) => {
    // body parser 产生的异常（如 payload too large）
    const status = err.status || err.statusCode || 500;
    const code =
      err.code && Number.isNaN(Number(err.code)) ? err.code : 'INTERNAL_ERROR';
    const payload = { error: { code, message: err.message || '服务器内部错误' } };
    if (err.details !== undefined) payload.error.details = err.details;
    if (status >= 500) {
      console.error('[unhandled]', err);
      payload.error.message = '服务器内部错误';
    }
    res.status(status).json(payload);
  });

  return app;
}

export { config };
