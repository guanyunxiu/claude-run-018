/**
 * 极简 S3/MinIO 兼容 HTTP 桩（仅用于无 Docker 环境的端到端测试）。
 *
 * 内存实现 @aws-sdk/client-s3 在本项目用到的命令，重点：
 *   - CreateBucket / HeadBucket
 *   - PutObject（If-NoneMatch:'*' 条件写，冲突 412）
 *   - HeadObject / GetObject / DeleteObject / ListObjectsV2 / CopyObject
 *   - Multipart：Create / UploadPart / UploadPartCopy / Complete / Abort
 *     除最后一片外 part 必须 >=5MB，否则 Complete 返回 400 EntityTooSmall
 * 不做签名校验。
 */
import http from 'node:http';
import { createHash } from 'node:crypto';

const MIN_PART = 5 * 1024 * 1024;

const etagOf = (buf) => `"${createHash('md5').update(buf).digest('hex')}"`;

function escapeXml(s) {
  return String(s).replace(/[<>&'"]/g, (c) =>
    ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', "'": '&apos;', '"': '&quot;' }[c]));
}

export function createS3Stub() {
  /** @type {Map<string,{body:Buffer,mtime:Date,etag:string}>} */
  const objects = new Map();
  /** @type {Map<string,{parts:Map<number,Buffer>}>} */
  const uploads = new Map();
  let bucketCreated = false;

  const readBody = (req) =>
    new Promise((resolve) => {
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => resolve(Buffer.concat(chunks)));
    });

  const xml = (res, status, body, extraHeaders = {}) => {
    res.writeHead(status, { 'Content-Type': 'application/xml', ...extraHeaders });
    res.end(body);
  };
  const xmlError = (res, status, code) =>
    xml(
      res,
      status,
      `<?xml version="1.0" encoding="UTF-8"?><Error><Code>${code}</Code><Message>${code}</Message></Error>`,
    );

  async function handle(req, res) {
    const url = new URL(req.url, 'http://x');
    const pathname = url.pathname;
    const method = req.method;
    const seg = pathname.split('/').filter(Boolean);
    const bucket = seg[0];
    const key = decodeURIComponent(seg.slice(1).join('/'));
    const uploadId = url.searchParams.get('uploadId');
    const q = url.searchParams;

    // ---- bucket 级 ----
    if (seg.length <= 1) {
      if (method === 'HEAD') return res.writeHead(bucketCreated ? 200 : 404).end();
      if (method === 'PUT') {
        bucketCreated = true;
        return res.writeHead(200).end();
      }
      if (method === 'GET') {
        const prefix = q.get('prefix') || '';
        const contents = [...objects.entries()]
          .filter(([k]) => k.startsWith(prefix))
          .map(
            ([k, v]) =>
              `<Contents><Key>${escapeXml(k)}</Key><Size>${v.body.length}</Size><LastModified>${v.mtime.toISOString()}</LastModified></Contents>`,
          )
          .join('');
        return xml(
          res,
          200,
          `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><KeyCount>${contents.length}</KeyCount>${contents}<IsTruncated>false</IsTruncated></ListBucketResult>`,
        );
      }
    }

    // ---- multipart ----
    if (method === 'POST' && q.has('uploads')) {
      const id = `up-${Math.random().toString(16).slice(2)}`;
      uploads.set(id, { parts: new Map() });
      return xml(
        res,
        200,
        `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>${bucket}</Bucket><Key>${escapeXml(key)}</Key><UploadId>${id}</UploadId></InitiateMultipartUploadResult>`,
      );
    }

    if (method === 'POST' && uploadId) {
      const up = uploads.get(uploadId);
      if (!up) return xmlError(res, 404, 'NoSuchUpload');
      const nums = [...up.parts.keys()].sort((a, b) => a - b);
      for (let i = 0; i < nums.length - 1; i++) {
        if (up.parts.get(nums[i]).length < MIN_PART) return xmlError(res, 400, 'EntityTooSmall');
      }
      const merged = Buffer.concat(nums.map((n) => up.parts.get(n)));
      objects.set(key, { body: merged, mtime: new Date(), etag: etagOf(merged) });
      uploads.delete(uploadId);
      return xml(
        res,
        200,
        `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>${bucket}</Bucket><Key>${escapeXml(key)}</Key><ETag>${etagOf(merged)}</ETag></CompleteMultipartUploadResult>`,
        { ETag: etagOf(merged) },
      );
    }

    if (method === 'DELETE' && uploadId) {
      uploads.delete(uploadId);
      return res.writeHead(204).end();
    }

    // 注意：UploadPartCopy 也带 uploadId，必须先于普通 UploadPart 判定
    if (method === 'PUT' && (q.has('x-amz-copy-source') || req.headers['x-amz-copy-source'])) {
      // copy-source 在 SDK 里是请求头 x-amz-copy-source: /bucket/key
      const rawSrc = decodeURIComponent(
        q.get('x-amz-copy-source') || req.headers['x-amz-copy-source'] || '',
      );
      const srcKey = rawSrc.replace(/^\/?[^/]+\//, '');
      const data = objects.get(srcKey)?.body;
      if (!data) return xmlError(res, 404, 'NoSuchKey');
      if (uploadId) {
        const up = uploads.get(uploadId);
        if (!up) return xmlError(res, 404, 'NoSuchUpload');
        const partNumber = Number(q.get('partNumber'));
        up.parts.set(partNumber, Buffer.from(data));
        return xml(
          res,
          200,
          `<?xml version="1.0" encoding="UTF-8"?><CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ETag>${etagOf(data)}</ETag><LastModified>${new Date().toISOString()}</LastModified></CopyPartResult>`,
        );
      }
      objects.set(key, { body: Buffer.from(data), mtime: new Date(), etag: etagOf(data) });
      return xml(
        res,
        200,
        `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></CopyObjectResult>`,
      );
    }

    if (method === 'PUT' && uploadId) {
      const up = uploads.get(uploadId);
      if (!up) return xmlError(res, 404, 'NoSuchUpload');
      const partNumber = Number(q.get('partNumber'));
      up.parts.set(partNumber, await readBody(req));
      return res.writeHead(200, { ETag: etagOf(up.parts.get(partNumber)) }).end();
    }

    // ---- 对象级 ----
    if (method === 'HEAD') {
      const obj = objects.get(key);
      if (!obj) return res.writeHead(404, { 'Content-Type': 'application/xml' }).end();
      return res
        .writeHead(200, {
          'Content-Length': obj.body.length,
          ETag: obj.etag,
          'Last-Modified': obj.mtime.toUTCString(),
        })
        .end();
    }

    if (method === 'GET') {
      const obj = objects.get(key);
      if (!obj) return xmlError(res, 404, 'NoSuchKey');
      res.writeHead(200, {
        'Content-Length': obj.body.length,
        'Content-Type': 'application/octet-stream',
        ETag: obj.etag,
      });
      return res.end(obj.body);
    }

    if (method === 'PUT') {
      const body = await readBody(req);
      if (req.headers['if-none-match'] === '*' && objects.has(key)) {
        return xmlError(res, 412, 'PreconditionFailed');
      }
      objects.set(key, { body, mtime: new Date(), etag: etagOf(body) });
      return res.writeHead(200, { ETag: etagOf(body) }).end();
    }

    if (method === 'DELETE') {
      objects.delete(key);
      return res.writeHead(204).end();
    }

    res.writeHead(501, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ code: 'NotImplemented', method, pathname }));
  }

  const server = http.createServer((req, res) => {
    handle(req, res).catch((err) => {
      // eslint-disable-next-line no-console
      console.error('[s3-stub]', err);
      if (!res.headersSent) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ code: 'StubError', message: String(err && err.message) }));
      }
    });
  });

  return {
    start: () =>
      new Promise((resolve) => {
        server.listen(0, '127.0.0.1', () => resolve(server.address().port));
      }),
    stop: () => new Promise((resolve) => server.close(resolve)),
    _objects: objects,
  };
}
