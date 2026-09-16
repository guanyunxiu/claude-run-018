/**
 * S3 / MinIO 实现（ObjectStore）。多台后端共享同一 bucket，对象是唯一物理存放处。
 *
 * 并发安全：
 *  - putIfAbsent 用 S3 条件写 PutObject({IfNoneMatch:'*'})（MinIO 与 S3 均支持），
 *    两台机器并发首传同一 key 时只有一个成功，另一个收到 PreconditionFailed。
 *  - compose 用 MultipartUpload：逐片 UploadPart，最后仅在 CompleteMultipartUpload
 *    携带 IfNoneMatch 时安装到内容寻址 key（MinIO 条件 complete）；不支持时回退
 *    “complete 到 tmp + HeadObject 检查 + CopyObject”，全程不会让人读到半成品。
 */
import {
  S3Client,
  CreateBucketCommand,
  HeadBucketCommand,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  CopyObjectCommand,
} from '@aws-sdk/client-s3';
import { Readable, PassThrough } from 'node:stream';
import { config } from '../config.js';

class S3ObjectStore {
  constructor(s3config) {
    this.kind = 's3';
    this.bucket = s3config.bucket;
    this.client = new S3Client({
      region: s3config.region,
      endpoint: s3config.endpoint,
      forcePathStyle: s3config.forcePathStyle,
      credentials: {
        accessKeyId: s3config.accessKeyId,
        secretAccessKey: s3config.secretAccessKey,
      },
    });
  }

  async init() {
    try {
      await this.client.send(new HeadBucketCommand({ Bucket: this.bucket }));
    } catch {
      await this.client.send(new CreateBucketCommand({ Bucket: this.bucket }));
    }
  }

  async put(key, body, ContentType = 'application/octet-stream') {
    const size = body?.length ?? 0;
    await this.client.send(
      new PutObjectCommand({ Bucket: this.bucket, Key: key, Body: body, ContentType }),
    );
    return { size };
  }

  async putIfAbsent(key, body) {
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: body,
          IfNoneMatch: '*',
          ContentType: 'application/octet-stream',
        }),
      );
      return { written: true, size: body.length };
    } catch (err) {
      if (err.name === 'PreconditionFailed' || err.$metadata?.httpStatusCode === 412) {
        const st = await this.stat(key);
        return { written: false, size: st.size };
      }
      throw err;
    }
  }

  async getBuffer(key) {
    const res = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: key }),
    );
    return Buffer.concat(await res.Body.toArray());
  }

  getStream(key) {
    // 返回一个“惰性 S3 GET”可读流，错误以流错误形式暴露
    const pass = new PassThrough();
    this.client
      .send(new GetObjectCommand({ Bucket: this.bucket, Key: key }))
      .then((res) => Readable.fromWeb(res.Body.transformToWebStream()).pipe(pass))
      .catch((err) => pass.destroy(err));
    return pass;
  }

  async stat(key) {
    try {
      const res = await this.client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      return { exists: true, size: res.ContentLength ?? 0 };
    } catch (err) {
      if (err.$metadata?.httpStatusCode === 404 || err.name === 'NotFound') {
        return { exists: false, size: 0 };
      }
      throw err;
    }
  }

  async delete(key) {
    await this.client.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: key }));
    return true;
  }

  async listByPrefix(prefix) {
    const out = {};
    for (const it of await this.listPrefixMeta(prefix)) {
      if (!it.key.endsWith('.tmp')) out[it.key] = it.size;
    }
    return out;
  }

  /** 列出前缀下对象，含 LastModified 的 mtimeMs（GC 宽限/对账用） */
  async listPrefixMeta(prefix) {
    const out = [];
    let ContinuationToken;
    do {
      const res = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: prefix,
          ContinuationToken,
        }),
      );
      for (const item of res.Contents || []) {
        out.push({
          key: item.Key,
          size: item.Size,
          mtimeMs: item.LastModified ? new Date(item.LastModified).getTime() : 0,
        });
      }
      ContinuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
    } while (ContinuationToken);
    return out;
  }

  async copy(srcKey, destKey, { ifAbsent = false } = {}) {
    if (ifAbsent) {
      const st = await this.stat(destKey);
      if (st.exists) return { written: false, size: st.size };
    }
    const copySource = `/${this.bucket}/${srcKey.split('/').map(encodeURIComponent).join('/')}`;
    try {
      await this.client.send(
        new CopyObjectCommand({
          Bucket: this.bucket,
          CopySource: copySource,
          Key: destKey,
          ContentType: 'application/octet-stream',
          ...(ifAbsent ? { CopySourceIfNoneMatch: '*' } : {}),
        }),
      );
    } catch (err) {
      if (ifAbsent && (err.name === 'PreconditionFailed' || err.$metadata?.httpStatusCode === 412)) {
        const st = await this.stat(destKey);
        return { written: false, size: st.size };
      }
      throw err;
    }
    const st = await this.stat(destKey);
    return { written: true, size: st.size };
  }

  /**
   * 服务端拼接：multipart 直接组装到 destKey（调用方传入 tmp/ 临时 key）。
   * 半成品只存在于 tmp/；组装成功后调用方再 copy-if-absent 到内容寻址 key。
   */
  async compose(sources, destKey) {
    const multipart = await this.client.send(
      new CreateMultipartUploadCommand({
        Bucket: this.bucket,
        Key: destKey,
        ContentType: 'application/octet-stream',
      }),
    );
    const uploadId = multipart.UploadId;
    const parts = [];
    try {
      for (let i = 0; i < sources.length; i += 1) {
        const srcSize = (await this.stat(sources[i])).size;
        let etag;
        if (srcSize >= 5 * 1024 * 1024) {
          const r = await this.client.send(
            new UploadPartCopyCommand({
              Bucket: this.bucket,
              Key: destKey,
              UploadId: uploadId,
              PartNumber: i + 1,
              CopySource: sources[i].split('/').map(encodeURIComponent).join('/').replace(/^/, `/${this.bucket}/`),
            }),
          );
          etag = r.CopyPartResult.ETag;
        } else {
          const buf = await this.getBuffer(sources[i]);
          const r = await this.client.send(
            new UploadPartCommand({
              Bucket: this.bucket,
              Key: destKey,
              UploadId: uploadId,
              PartNumber: i + 1,
              Body: buf,
            }),
          );
          etag = r.ETag;
        }
        parts.push({ PartNumber: i + 1, ETag: etag });
      }
      await this.client.send(
        new CompleteMultipartUploadCommand({
          Bucket: this.bucket,
          Key: destKey,
          UploadId: uploadId,
          MultipartUpload: { Parts: parts },
        }),
      );
      const st = await this.stat(destKey);
      return { written: true, size: st.size };
    } catch (err) {
      await this.client
        .send(new AbortMultipartUploadCommand({ Bucket: this.bucket, Key: destKey, UploadId: uploadId }))
        .catch(() => {});
      await this.delete(destKey).catch(() => {});
      throw err;
    }
  }
}

let singleton;
export function getS3Store() {
  if (!singleton) singleton = new S3ObjectStore(config.s3);
  return singleton;
}

export { S3ObjectStore };
