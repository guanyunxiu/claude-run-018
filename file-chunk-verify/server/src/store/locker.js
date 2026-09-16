/**
 * 跨机器协调锁（DistributedLocker）
 * --------------------------------
 *  - redis   ：SET NX PX + 唯一 token，Lua 保证“只解自己的锁”，带续租
 *  - memory  ：进程内异步互斥（测试用：多个 app 实例共享同一 Locker 模拟多机协调）
 *
 * 锁粒度：
 *   cas:<chunkHash>   首传/自愈同一内容分片
 *   merge:<fileId>    同一文件只有一台机器进入合并
 *   gc                全集群同一时刻只跑一个清理
 */
import { config } from '../config.js';

/* ---------------- 进程内异步互斥锁（含 TTL） ---------------- */
class MemoryLocker {
  constructor() {
    this.kind = 'memory';
    this.held = new Map(); // key -> token
    this.waiters = new Map(); // key -> resolve[]
  }

  async acquire(key, { ttlMs = 60_000, waitMs = 0 } = {}) {
    const token = `${process.pid}-${Math.random().toString(36).slice(2)}`;
    const tryLock = () => {
      if (!this.held.has(key)) {
        const expiresAt = Date.now() + ttlMs;
        this.held.set(key, { token, expiresAt });
        return true;
      }
      // TTL 到期（模拟持锁进程崩溃）：可被他人抢占
      if (this.held.get(key).expiresAt <= Date.now()) {
        const expiresAt = Date.now() + ttlMs;
        this.held.set(key, { token, expiresAt });
        return true;
      }
      return false;
    };

    if (tryLock()) return makeLease(this, key, token);
    if (waitMs <= 0) return null;

    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline) {
      await new Promise((r) => {
        const arr = this.waiters.get(key) || [];
        arr.push(r);
        this.waiters.set(key, arr);
        setTimeout(r, Math.min(50, deadline - Date.now())).unref?.();
      });
      if (tryLock()) return makeLease(this, key, token);
    }
    return null;
  }

  _release(key, token) {
    const cur = this.held.get(key);
    if (cur && cur.token === token) {
      this.held.delete(key);
      const arr = this.waiters.get(key) || [];
      this.waiters.delete(key);
      for (const r of arr.splice(0)) r();
    }
  }
}

function makeLease(locker, key, token) {
  let released = false;
  return {
    key,
    token,
    async release() {
      if (released) return;
      released = true;
      locker._release(key, token);
    },
    async renew(ttlMs) {
      const cur = locker.held.get(key);
      if (cur && cur.token === token) cur.expiresAt = Date.now() + ttlMs;
    },
  };
}

/* ---------------- Redis 锁（SET NX PX + token 校验） ---------------- */
const RELEASE_LUA =
  "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end";

class RedisLocker {
  constructor(redis, ttlMs) {
    this.kind = 'redis';
    this.redis = redis;
    this.defaultTtl = ttlMs;
  }

  async acquire(key, { ttlMs = this.defaultTtl, waitMs = 0 } = {}) {
    const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const deadline = Date.now() + waitMs;
    for (;;) {
      const ok = await this.redis.set(`lock:${key}`, token, 'PX', ttlMs, 'NX');
      if (ok === 'OK') return this._lease(key, token, ttlMs);
      if (Date.now() >= deadline) return null;
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  _lease(key, token, ttlMs) {
    const redis = this.redis;
    let released = false;
    return {
      key,
      token,
      async release() {
        if (released) return;
        released = true;
        await redis.eval(RELEASE_LUA, 1, `lock:${key}`, token).catch(() => {});
      },
      async renew(nextTtlMs = ttlMs) {
        const cur = await redis.get(`lock:${key}`);
        if (cur === token) await redis.pexpire(`lock:${key}`, nextTtlMs);
      },
    };
  }
}

let singleton;

/** 按配置创建单例 Locker；测试可用 setLocker 注入共享 memory 锁模拟多机 */
export async function getLocker() {
  if (singleton) return singleton;
  if (config.lock.driver === 'none') {
    singleton = null; // 不加锁（不推荐；仅极端降级）
  } else if (config.lock.driver === 'redis') {
    const Redis = (await import('ioredis')).default;
    const redis = new Redis(config.lock.redisUrl, { lazyConnect: false, maxRetriesPerRequest: null });
    singleton = new RedisLocker(redis, config.lock.ttlMs);
  } else {
    singleton = new MemoryLocker();
  }
  return singleton;
}

export function setLocker(locker) {
  singleton = locker;
}

export { MemoryLocker };
