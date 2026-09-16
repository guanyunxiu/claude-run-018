/**
 * 仅供无 MySQL 环境的端到端测试使用：通过 ESM resolve hook 把
 * 'mysql2/promise' 重定向到一个内存实现，业务代码零改动。
 */
import { pathToFileURL } from 'node:url';

const mockUrl = new URL('./mock-mysql.mjs', import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (specifier === 'mysql2/promise' || specifier === 'mysql2') {
    return { url: mockUrl, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
