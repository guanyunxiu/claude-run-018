# 浏览器端 GB 级文件分布式分片上传与校验系统

前端（TypeScript + 单 WebWorker + Web Crypto + Fetch）将 GB 级本地文件固定大小分片、
在 Worker 中计算每个分片的 SHA-256，再并发上传二进制分片，支持基于服务端状态的断点续传；
后端（Node.js + Express + MySQL + 本地磁盘）接收分片、维护元数据，并在全部分片上传完成后
重新读取分片做**聚合完整性校验**，校验通过后合并成完整文件。

> 说明：需求描述中 “Node.js，Web 框架（Gin/SpringBoot）” 存在冲突，Gin 为 Go 框架、
> Spring Boot 为 Java 框架。本实现按 Node.js 生态选择 **Express**。

## 目录结构

```
file-chunk-verify/
├── docker-compose.yml          # 本地 MySQL 8
├── README.md
├── server/                     # 后端：Express + MySQL + 磁盘存储
│   ├── .env.example
│   ├── package.json
│   ├── sql/schema.sql
│   └── src/
│       ├── config.js           # 配置 / 极简 .env 加载（零依赖）
│       ├── db.js               # 连接池 + 自动建库建表
│       ├── hash.js             # SHA-256 工具
│       ├── storage.js          # 分片原子写盘 / 流式合并
│       ├── app.js              # Express 应用、错误处理
│       ├── server.js           # 启动入口
│       └── routes/files.js     # 全部业务接口
└── web/                        # 前端：Vite + TypeScript
    ├── index.html
    ├── package.json
    ├── tsconfig.json
    ├── tsconfig.worker.json
    ├── vite.config.ts
    └── src/
        ├── main.ts             # 页面 UI 与交互
        ├── uploader.ts         # 分片/上传编排（含 3 并发上传池、重试、进度）
        ├── hash.worker.ts      # 唯一的 WebWorker：SHA-256 计算
        ├── api.ts              # Fetch 接口封装
        ├── types.ts
        └── style.css
```

## 快速开始

### 1. 启动 MySQL

```bash
docker compose up -d mysql
```

或使用已有 MySQL，执行 `server/sql/schema.sql`（后端启动时也会自动 `CREATE DATABASE/TABLE`）。

### 2. 启动后端（:3000）

```bash
cd server
cp .env.example .env          # 按实际修改数据库账号密码
npm install
npm start                     # 开发时可用 npm run dev（--watch）
```

磁盘布局（默认 `server/storage/`）：

```
storage/
├── chunks/<fileId>/00000000.part   # 分片文件（原子写入：tmp + rename）
├── merged/<fileId>__<原文件名>      # 校验通过后合并出的完整文件
```

### 3. 启动前端（:5173）

```bash
cd web
npm install
npm run dev
```

浏览器打开 http://localhost:5173 ，选择大文件 → 选择分片大小（2/4/8/16 MB）→ 开始。
Vite 已配置 `/api` 代理到 `http://localhost:3000`。

## 断点续传演示

1. 上传过程中刷新页面或重新选择同一个文件（文件名/大小/修改时间/分片大小不变 → fileId 相同）。
2. 前端仍会先扫描分片哈希（Worker 计算），但 `init` 返回的已上传分片会直接跳过上传。
3. 已上传分片越多，恢复后需要上传的数据越少；`storage/chunks/<fileId>/` 中的分片可随时查看。

## 完整性校验模型

- **分片哈希**：`SHA-256(分片字节)`，前端在 Worker 中通过 Web Crypto 计算。
- **聚合哈希（fileHash）**：`SHA-256( concat(每个分片哈希的 hex 字符串) )`，
  由前端随 `init` 上报，后端在 `complete` 时用磁盘上的分片重算并比对。
  它是整个文件的内容指纹（对分片有序拼接敏感），无需在浏览器里把 GB 级文件整体喂给哈希器。
- **合并文件哈希**：后端流式合并分片时同时计算 `SHA-256(完整文件)`，一并返回用于端到端确认。
  注意它与聚合哈希是两个不同的值，页面都会展示。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/files/init` | 注册/恢复一个文件任务（幂等），返回已上传分片列表 |
| `GET`  | `/api/files/:fileId/chunks` | 查询已上传分片（序号、哈希、大小） |
| `GET`  | `/api/files/:fileId/status` | 查询任务状态/进度 |
| `POST` | `/api/files/:fileId/chunks/:index?hash=<sha256>` | 上传单个分片二进制（幂等） |
| `POST` | `/api/files/:fileId/complete` | 分片齐全后聚合校验 + 合并完整文件 |

### 错误响应

```json
{ "error": { "code": "CHUNK_HASH_MISMATCH", "message": "..." } }
```

常见 code：`VALIDATION_ERROR` / `FILE_PARAM_MISMATCH` / `FILE_NOT_FOUND` /
`CHUNK_SIZE_MISMATCH` / `CHUNK_HASH_MISMATCH` / `CHUNKS_INCOMPLETE` /
`AGGREGATE_HASH_MISMATCH` / `FILE_ALREADY_VERIFIED`。

## 设计要点

- **不阻塞主线程**：唯一的 WebWorker 负责全部 SHA-256 计算，`ArrayBuffer` 以
  Transferable 所有权转移给 Worker，零拷贝；主线程只做切片（`Blob.slice`，不产生副本）与调度。
- **低内存**：同一时刻 Worker 中只有一个分片 buffer；上传用 3 并发的有界任务池，
  分片随用随从 File 切片，浏览器与 Node 端都不会把 GB 文件整体载入内存。
- **断点续传**：文件 ID = `SHA-256(文件名:大小:最后修改时间:分片大小)`，
  同一文件同一分片策略得到稳定 ID；init 返回已上传分片，命中（序号+哈希一致）即跳过。
- **上传幂等**：服务端按 `(file_id, chunk_index)` 唯一键去重；重复上传且哈希一致返回
  `skipped:true`；哈希不同则覆盖重写，可容忍半截坏分片。
- **落盘前校验**：分片先在内存校验大小与 SHA-256，通过后才 `tmp + rename` 原子落盘并写库。
- **完成时强校验**：`complete` 会校验数量、序号连续性、累计大小，并**重新流式读取每个分片
  重算哈希**，再算聚合哈希与前端上报值比对，全部通过才流式合并（合并同时产出整文件哈希）。

## 已知边界 / 可扩展点

- 分片断点续传基于“文件元数据 + 分片哈希”，未额外抽样本地磁盘比对（哈希已能覆盖内容）。
- 未包含鉴权/多租户、秒传（相同 fileHash 直接复用）、分片垃圾 GC、分布式多节点存储，
  接口与表结构已为此预留扩展空间。
- 生产部署建议把存储目录换成对象存储（S3/OSS），并用 Nginx 处理大请求体与限速。
