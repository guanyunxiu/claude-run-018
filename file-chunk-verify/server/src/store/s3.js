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
  UploadPartCopyCommand,
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
   * 服务端拼接（不把整个合并结果拉到本机内存再回传）：
   *  - 每个 >=5MB 的源对象作为一个 part，直接 UploadPartCopy（S3 侧拷贝）。
   *  - 小于 5MB 的源对象按序读取并累积到 part 缓冲，凑满 PART_MIN 即 UploadPart，
   *    保证除最后一个 part 外每个 part 都 >=5MB（S3 multipart 硬限制）。
   * 半成品只存在于调用方指定的 tmp/ key；失败 Abort，绝不污染内容寻址目标。
   */
  async compose(sources, destKey) {
    const PART_MIN = 5 * 1024 * 1024;
    const multipart = await this.client.send(
      new CreateMultipartUploadCommand({
        Bucket: this.bucket,
        Key: destKey,
        ContentType: 'application/octet-stream',
      }),
    );
    const uploadId = multipart.UploadId;
    const parts = [];
    let partNo = 0;
    let carry = Buffer.alloc(0); // 累积的小分片字节

    const flushBuffer = async (isLast) => {
      if (carry.length === 0) return;
      // 非最后 part 必须 >=5MB；若仍不足（总文件就很小），作为唯一 part 也合法
      if (!isLast && carry.length < PART_MIN) return; // 等后续源继续累积
      partNo += 1;
      const r = await this.client.send(
        new UploadPartCommand({
          Bucket: this.bucket,
          Key: destKey,
          UploadId: uploadId,
          PartNumber: partNo,
          Body: carry,
        }),
      );
      parts.push({ PartNumber: partNo, ETag: r.ETag });
      carry = Buffer.alloc(0);
    };

    try {
      for (let i = 0; i < sources.length; i += 1) {
        const srcKey = sources[i];
        const srcSize = (await this.stat(srcKey)).size;

        if (srcSize >= PART_MIN && carry.length === 0) {
          // 大源且没有挂起的小字节：服务端直接拷贝整对象为一个 part
          partNo += 1;
          const r = await this.client.send(
            new UploadPartCopyCommand({
              Bucket: this.bucket,
              Key: destKey,
              UploadId: uploadId,
              PartNumber: partNo,
              // CopySource = /bucket/key；key 为 cas/<xx>/<hash>.part，无特殊字符
              CopySource: `/${this.bucket}/${srcKey}`,
            }),
          );
          parts.push({ PartNumber: partNo, ETag: r.CopyPartResult.ETag });
        } else {
          // 小源，或前面已有累积字节：读字节追加（保持顺序），凑满即冲刷
          const buf = await this.getBuffer(srcKey);
          carry = carry.length === 0 ? buf : Buffer.concat([carry, buf]);
          if (carry.length >= PART_MIN) {
            partNo += 1;
            const r = await this.client.send(
              new UploadPartCommand({
                Bucket: this.bucket,
                Key: destKey,
                UploadId: uploadId,
                PartNumber: partNo,
                Body: carry,
              }),
            );
            parts.push({ PartNumber: partNo, ETag: r.ETag });
            carry = Buffer.alloc(0);
          }
        }
      }

      // 冲刷剩余字节（最后一个 part 允许 <5MB）
      await flushBuffer(true);
      // 极端情况：所有源都是 0 字节，也要完成一个空对象
      if (parts.length === 0) {
        partNo += 1;
        const r = await this.client.send(
          new UploadPartCommand({
            Bucket: this.bucket,
            Key: destKey,
            UploadId: uploadId,
            PartNumber: partNo,
            Body: Buffer.alloc(0),
          }),
        );
        parts.push({ PartNumber: partNo, ETag: r.ETag });
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
