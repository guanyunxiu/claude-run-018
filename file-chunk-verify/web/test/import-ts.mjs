/**
 * 用 esbuild 在内存把 pipeline.ts 转译成 ESM，供 Node 测试直接 import。
 * 用法：
 *   const { runPipeline } = await importTs('./src/pipeline.ts');
 */
import { build } from 'esbuild';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import os from 'node:os';
import fsp from 'node:fs/promises';

const cache = new Map();

export async function importTs(relPath) {
  if (cache.has(relPath)) return cache.get(relPath);
  const abs = path.resolve(relPath);
  const result = await build({
    entryPoints: [abs],
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    write: false,
    sourcemap: 'inline',
  });
  const tmp = path.join(
    await fsp.mkdtemp(path.join(os.tmpdir(), 'ts-eval-')),
    path.basename(relPath).replace(/\.ts$/, '.mjs'),
  );
  await fsp.writeFile(tmp, result.outputFiles[0].text);
  const mod = await import(pathToFileURL(tmp).href);
  cache.set(relPath, mod);
  return mod;
}
