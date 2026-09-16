/**
 * ObjectStore 工厂：按 OBJECT_STORE 选择 local（默认，单测/单机）或 s3（MinIO/S3）。
 * 业务层统一从这里取单例，物理存放处对路由透明。
 */
import { config } from '../config.js';
import { getLocalStore } from './local.js';
import { getS3Store } from './s3.js';

let instance;

export function objectStore() {
  if (instance) return instance;
  instance = config.objectStore === 's3' ? getS3Store() : getLocalStore();
  return instance;
}

/** 测试/多实例时显式注入自定义 store */
export function setObjectStore(store) {
  instance = store;
}
