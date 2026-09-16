/**
 * 本机磁盘实现（ObjectStore）。单测/单机使用；多实例测试时多个 app 指向同一目录，
 * 配合分布式锁即可模拟“共享存储”。
 */
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { config } from '../config.js';
import { tmpKey } from './base.js';

class LocalObjectStore {
  constructor(rootDir) {
    this.kind = 'local';
    this.root = rootDir;
  }

  _abs(key) {
    // 防目录穿越：key 全部由内部生成（哈希/uuid），再规范化校验
    const abs = path.join(this.root, key);
    if (!abs.startsWith(path.resolve(this.root) + path.sep) && abs !== path.resolve(this.root)) {
      throw Object.assign(new Error('非法对象路径'), { code: 'OBJECT_PATH_ESCAPE' });
    }
    return abs;
  }

  async init() {
    await fsp.mkdir(this.root, { recursive: true });
    await fsp.mkdir(path.join(this.root, 'cas'), { recursive: true });
    await fsp.mkdir(path.join(this.root, 'merged'), { recursive: true });
    await fsp.mkdir(path.join(this.root, 'tmp'), { recursive: true });
  }

  async put(key, data) {
    const abs = this._abs(key);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    const tmp = `${abs}.${process.pid}.${randomUUID()}.tmp`;
    await fsp.writeFile(tmp, data);
    await fsp.rename(tmp, abs);
    return { size: data.length };
  }

  /**
   * 内容寻址路径上的“仅当不存在时写入”。本地 rename 对同一路径是原子替换；
   * 由于路径由内容哈希决定、并发双方写入的是相同字节，替换结果仍然唯一且正确。
   */
  async putIfAbsent(key, data) {
    const abs = this._abs(key);
    if (fs.existsSync(abs)) {
      const st = await fsp.stat(abs);
      return { written: false, size: st.size };
    }
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    const tmp = `${abs}.${process.pid}.${randomUUID()}.tmp`;
    await fsp.writeFile(tmp, data);
    try {
      await fsp.link(tmp, abs); // 硬链接：目标已存在则失败（不覆盖）
      await fsp.rm(tmp, { force: true });
      return { written: true, size: data.length };
    } catch (err) {
      await fsp.rm(tmp, { force: true });
      if (err.code === 'EEXIST') {
        const st = await fsp.stat(abs).catch(() => null);
        return { written: false, size: st ? st.size : data.length };
      }
      // 某些文件系统不支持硬链接：回退为存在性判断 + rename（同内容覆盖无害）
      if (fs.existsSync(abs)) {
        const st = await fsp.stat(abs);
        return { written: false, size: st.size };
      }
      await fsp.rename(
        `${abs}.${process.pid}.${randomUUID()}.tmp`,
        abs,
      ).catch(() => {});
      return { written: true, size: data.length };
    }
  }

  async getBuffer(key) {
    return fsp.readFile(this._abs(key));
  }

  getStream(key) {
    return fs.createReadStream(this._abs(key), { highWaterMark: 64 * 1024 });
  }

  async stat(key) {
    try {
      const st = await fsp.stat(this._abs(key));
      return { exists: true, size: st.size };
    } catch {
      return { exists: false, size: 0 };
    }
  }

  async delete(key) {
    await fsp.rm(this._abs(key), { force: true });
    return true;
  }

  /** 列出前缀下对象，含 mtimeMs（GC 宽限/对账用） */
  async listPrefixMeta(prefix) {
    const root = path.resolve(this.root, prefix);
    const out = [];
    const walk = async (dir) => {
      let entries;
      try {
        entries = await fsp.readdir(dir, { withFileTypes: true });
      } catch {
        return;
      }
      for (const e of entries) {
        const abs = path.join(dir, e.name);
        if (e.isDirectory()) await walk(abs);
        else if (e.isFile()) {
          const key = path.relative(path.resolve(this.root), abs).split(path.sep).join('/');
          const st = await fsp.stat(abs);
          out.push({ key, size: st.size, mtimeMs: st.mtimeMs });
        }
      }
    };
    await walk(root);
    return out;
  }

  /** 列出前缀下 {key:size}（兼容旧调用） */
  async listByPrefix(prefix) {
    const out = {};
    for (const it of await this.listPrefixMeta(prefix)) {
      if (!it.key.endsWith('.tmp')) out[it.key] = it.size;
    }
    return out;
  }

  /** 兼容旧名 */
  async listByPrefixWithMeta(prefix) {
    const out = {};
    for (const it of await this.listPrefixMeta(prefix)) out[it.key] = it;
    return out;
  }

  /** 条件复制（仅当目标不存在）；返回 {written,size} */
  async copy(srcKey, destKey, { ifAbsent = false } = {}) {
    const dest = this._abs(destKey);
    if (ifAbsent && fs.existsSync(dest)) {
      const st = await fsp.stat(dest);
      return { written: false, size: st.size };
    }
    await fsp.mkdir(path.dirname(dest), { recursive: true });
    await fsp.copyFile(this._abs(srcKey), dest);
    const st = await fsp.stat(dest);
    return { written: true, size: st.size };
  }

  /** 本地原子改名（临时对象 → 内容寻址 key） */
  async rename(srcKey, destKey) {
    const dest = this._abs(destKey);
    await fsp.mkdir(path.dirname(dest), { recursive: true });
    await fsp.rename(this._abs(srcKey), dest);
    return { size: (await fsp.stat(dest)).size };
  }

  /** 服务端“拼接”：本地流式按序读取并写出，再装到内容寻址 destKey。 */
  async compose(sources, destKey, { ifAbsent = false } = {}) {
    const destAbs = this._abs(destKey);
    if (ifAbsent && fs.existsSync(destAbs)) {
      const st = await fsp.stat(destAbs);
      return { written: false, size: st.size };
    }
    const tmpKeyName = tmpKey(randomUUID() + '.tmp');
    const tmpAbs = this._abs(tmpKeyName);
    await fsp.mkdir(path.dirname(tmpAbs), { recursive: true });
    const out = fs.createWriteStream(tmpAbs);
    let size = 0;
    await new Promise((resolve, reject) => {
      out.on('error', reject);
      let i = 0;
      const pump = () => {
        if (i >= sources.length) return out.end(() => resolve());
        const stream = this.getStream(sources[i++]);
        stream.on('data', (d) => {
          size += d.length;
        });
        stream.on('error', reject);
        stream.on('end', pump);
        stream.pipe(out, { end: false });
      };
      pump();
    });
    if (ifAbsent) {
      try {
        await fsp.link(tmpAbs, destAbs);
        await fsp.rm(tmpAbs, { force: true });
        return { written: true, size };
      } catch (err) {
        await fsp.rm(tmpAbs, { force: true });
        if (err.code === 'EEXIST') {
          const st = await fsp.stat(destAbs);
          return { written: false, size: st.size };
        }
        throw err;
      }
    }
    await fsp.mkdir(path.dirname(destAbs), { recursive: true });
    await fsp.rename(tmpAbs, destAbs);
    return { written: true, size };
  }
}

let singleton;
export function getLocalStore() {
  if (!singleton) singleton = new LocalObjectStore(config.storageDir);
  return singleton;
}

export { LocalObjectStore };
