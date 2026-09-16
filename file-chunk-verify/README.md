# 浏览器端 GB 级文件分布式分片上传与校验系统

前端（TypeScript + 单 WebWorker + Web Crypto + Fetch + IndexedDB）把 GB 级本地文件固定大小分片，
在 Worker 中计算每片 SHA-256，采用**有界「哈希 → 上传」流水线**边算边传；分片哈希持久化到
IndexedDB 支持刷新后续算。后端（Node.js + Express + MySQL + 本地磁盘）为**内容寻址（CAS）**：
物理分片按 `chunkHash` 全局去重存放、引用计数管理生命周期；**相同文件秒传**（零上传直接完成）、
**不同文件共享相同内容分片物理只存一份**；`complete` 时从磁盘重算全部哈希做强校验后流式合并，
合并产物同样内容寻址并可被秒传复用。

> 说明：需求描述中 “Node.js，Web 框架（Gin/SpringBoot）” 存在冲突，Gin 为 Go 框架、
> Spring Boot 为 Java 框架。本实现按 Node.js 生态选择 **Express**。

## 迭代三：内容寻址（CAS）· 秒传 · 跨文件去重 · 引用计数 GC（当前版本）

### 核心能力

- **秒传**：`init` 或独立 `precheck` 提交聚合哈希 + 全分片哈希清单；若已有**校验完整**的相同文件，
  服务端直接把新文件置为 `completed` 并复用其分片与合并产物，客户端**零上传**即可下载。
- **跨文件去重**：物理分片改为内容寻址，路径由分片内容哈希推导，不同文件的相同内容分片
  **物理只存一份**，靠 `files ↔ cas_chunks` 多对多关联 + 引用计数共享。
- **引用计数生命周期**：删除文件只减引用，引用为 0 的物理对象由 GC 回收；删 A 绝不影响仍引用
  同一分片的 B。
- **并发幂等**：同 `chunkHash` 并发首传只落盘一份（唯一键 + 原子 rename），引用计数精确。
- **安全**：所有物理路径均由 64 位 hex 哈希推导，无法通过猜测 fileId/hash 路径穿越；
  无登录态下删除也仅作用于该 fileId 自身的引用。

### CAS 物理布局

```
storage/
├── cas/
│   └── <chunkHash 前2位>/<chunkHash>.part     # 全局唯一内容分片，多文件共享
└── merged/
    └── <mergedHash 前2位>/<mergedHash>.bin    # 内容寻址合并产物（秒传共享）
```

旧版本的 `storage/chunks/<fileId>/<index>.part`（按文件+序号存放）已废弃。

### 数据表（多对多 + 引用计数）

```
files            文件任务：id, file_name, file_size, chunk_size, total_chunks,
                 file_hash(聚合,可空), merged_hash(→merged_blobs), status, merged_path
cas_chunks       全局物理分片：chunk_hash(PK), chunk_size, storage_path, ref_count
file_chunks      files↔cas_chunks 关联：UNIQUE(file_id, chunk_index), chunk_hash(→cas_chunks), status
merged_blobs     合并产物：merged_hash(PK), file_size, storage_path, ref_count
```

引用计数规则：

- 一个 `file_chunks` 行 = 对一个 `cas_chunks` 的 1 个引用（同文件内不同序号即使哈希相同也各计 1）。
- 上传分片：CAS 已存在则 `ref_count+1`；同文件同序号同哈希重传为幂等（撤销多加的计数）；
  同序号不同内容则替换关联、旧哈希 `ref_count-1`。
- 秒传：复用捐赠文件的全部分片与合并产物，所有计数 `+1`。
- 删除文件：逐关联 `ref_count-1`、删关联行、删 files 行、合并产物 `ref_count-1`，**不删物理文件**。
- GC：`POST /api/admin/gc` 回收 `ref_count=0` 且创建超过 `minAgeSec`（默认 300s）的物理对象，
  避免与“先写元数据后建关联”的正常首传竞争。

### 秒传时序

```
客户端                              服务端
 │ init { fileHash, chunkHashes[] }  │  找 file_hash 相同且 status=completed 的捐赠文件
 │ ───────────────────────────────► │  校验：合并产物存在 + 全分片关联齐 + 物理可读
 │                                   │  复用：新 files 置 completed，分片/合并产物 ref_count+1
 │ ◄──── instant=true, completed ────│  → 客户端零上传，直接可下载
 │
 │ （仅部分分片命中）                 │  init 返回 hits={index:hash}（全局 CAS 已存在）
 │ ◄──── instant=false, hits ───────│  客户端只上传未命中分片；命中分片上传时返回 dedup=true
```

边算边传场景下，全部分片哈希算完的瞬间前端还会用“带清单 init”做一次秒传仲裁：
若此刻服务端恰好已有完整文件（例如别人刚传完），即中止剩余上传。

### 与前两轮的关系

- 迭代一：固定分片、单 Worker SHA-256、二进制上传、落盘前校验、完成时全量重算。
- 迭代二：边算边传有界流水线 + IndexedDB 哈希断点续算 + 分阶段任务（init 无聚合哈希）。
- 迭代三（本轮）：分片物理存储从“按文件序号”改为“按内容哈希 CAS”，新增秒传、跨文件去重、
  引用计数与 GC；**前两轮的流水线、续算、续传、强校验全部保留并在 CAS 之上工作**。

### HTTP 接口（v3）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/files/init` | 注册/恢复；可带 `fileHash`+`chunkHashes[]`，命中完整文件则秒传；响应含 `instant` 与 `hits` |
| `POST` | `/api/files/precheck` | 只读预检：`{instant, donor, hits}`，不产生副作用 |
| `POST` | `/api/files/:fileId/hash` | 补报/锁定聚合哈希 |
| `GET`  | `/api/files/:fileId/chunks` | 本文件已上传关联 |
| `GET`  | `/api/files/:fileId/status` | 任务状态 |
| `POST` | `/api/files/:fileId/chunks/:index?hash=` | 上传分片；响应新增 `dedup`（命中全局 CAS） |
| `POST` | `/api/files/:fileId/complete` | 重算 CAS 分片强校验 → 流式合并（共享分片可读）→ 引用合并产物 |
| `DELETE` | `/api/files/:fileId` | 删除文件（仅减引用，`physicalRemoved:false`） |
| `GET`  | `/api/files/:fileId/download` | 流式下载已完成文件（读共享合并产物） |
| `POST` | `/api/admin/gc` | 回收零引用孤儿分片/合并产物，body `{minAgeSec?}` |

错误码（新增/相关）：`FILE_NOT_READY`（未完成禁止下载）、`MERGED_BLOB_MISSING`、
`CHUNK_FILE_MISSING`（CAS 元数据指向空文件）、`FILE_HASH_REQUIRED`、`FILE_HASH_LOCKED`、
`FILE_VERIFYING`、`FILE_ALREADY_VERIFIED`、`CHUNK_HASH_MISMATCH`、`AGGREGATE_HASH_MISMATCH`、
`CHUNKS_INCOMPLETE`。

### CAS 强校验为何仍安全

- 分片上传：先在内存重算 SHA-256，与 `?hash=` 不符直接 422 拒绝，绝不写 CAS。
- 秒传：不仅比对聚合哈希，还校验捐赠文件的合并产物与**每一个 CAS 物理分片都存在/可读**，
  防止“元数据指到空文件”的脏秒传。
- complete：无论分片是否去重命中，都**逐个从 CAS 物理路径重读重算哈希**，再算聚合哈希比对，
  因此磁盘位翻转/人为篡改/共享分片损坏都会被 422 检出（测试已覆盖篡改共享分片场景）。

### 自动化测试

```bash
cd server
npm run test:e2e       # 79 条：含秒传、跨文件去重、删 A 不影响 B complete/下载、
                       #       引用归零 GC、并发同 hash 首传幂等、篡改共享分片检出
npm run smoke:staged   # 分阶段边传边补哈希 + 强校验（40MB）
npm run smoke:cas      # 23 项真实 HTTP：A 正常→B 秒传零上传→C 部分去重→删除→GC 物理回收

cd ../web
npm run test:pipeline  # 52 条：含全局 CAS 命中跳过、秒传仲裁中止排队项、续算续传
npm run smoke:resume   # 4GB/512 片中途刷新的复用与有界内存
```

---

## 迭代二：边算边传 + 哈希断点续算（保留）

### 与迭代一「先全量哈希再上传」的差异

| 维度 | 迭代一（旧） | 迭代二（当前） |
| --- | --- | --- |
| 时序 | 先顺序哈希**全文件** → 再 init → 再上传 | 先 `init`（fileHash=null）→ **边算边传** → 算完补报聚合哈希 → `complete` |
| 首字节上传时间 | 整个文件哈希结束后（GB 文件可能数十秒/分钟） | 第 1 片哈希算完即可（通常 < 1s） |
| 哈希失败/刷新代价 | 全部重算 | 从 IndexedDB 游标续算，只算缺失片 |
| 内存模型 | 哈希阶段 1 片 + 上传阶段 3 片 | 统一有界槽位：哈希中+排队+上传中 ≤ `maxInflight`（默认 6）片 |
| 服务端 fileHash | init 时必须提交 | 分阶段：init 可空，`POST /hash` 补报锁定，complete 前必须锁定 |
| 已算未传分片 | 不存在该状态（全算完才传） | 刷新后读字节直接上传，**不重复哈希** |

### 新流程时序

```
前端                                              后端
  │  POST /init { fileId, 大小/片数, fileHash:null } │  建分阶段任务（file_hash=NULL）
  │ ───────────────────────────────────────────────►│
  │  ◄────────── uploadedChunks（已传清单，用于续传） │
  │                                                  │
  │  对每个分片（有界流水线，最多 6 片在途）：          │
  │   1. IndexedDB 命中哈希？  是→复用，跳过 Worker    │
  │   2. 否→ file.slice → 单 Worker 算 SHA-256        │
  │      → 立即写 IndexedDB（游标+1，可随时刷新）      │
  │   3. 服务端已有同哈希？ 是→跳过上传、释放槽位       │
  │      否→进入上传队列（3 路 HTTP 并发）             │
  │  POST /:id/chunks/:i?hash=xx  （raw 二进制）       │  校验大小/哈希→原子落盘→写库
  │ ───────────────────────────────────────────────► │
  │  ◄──────── 201 created / 200 skipped（幂等）       │
  │                                                  │
  │  全部片算完 → 聚合哈希 = sha256(∑片哈希hex)        │
  │  POST /:id/hash { fileHash }   （补报并锁定）      │  NULL → 锁定；同值幂等；异值 409
  │ ───────────────────────────────────────────────► │
  │  POST /:id/complete                              │  原子抢占 uploading→merging
  │ ───────────────────────────────────────────────► │  重读磁盘每片重算哈希
  │                                                  │  数量/序号/大小/聚合哈希 强校验
  │                                                  │  流式合并 + 整文件 SHA-256
  │  ◄──────── { verified, aggregateHash, mergedHash }│
```

### IndexedDB 键设计（哈希断点续算）

数据库 `chunk-hash-cache`（v1），两个对象仓库，均以 `fileId` 为归属：

- `meta`（keyPath = `fileId`）

  ```ts
  {
    fileId,
    fingerprint: `${fileName}:${fileSize}:${lastModified}:${chunkSize}`,
    fileName, fileSize, lastModified, chunkSize, totalChunks,
    cursor // 下一个待哈希序号；缓存保证 0..cursor-1 连续存在
  }
  ```

- `chunks`（keyPath = 复合键 `[fileId, index]`，天然支持按文件范围遍历/清除）

  ```ts
  { fileId, index, hash /*64 hex*/, size, ts }
  ```

要点：

1. **指纹防错配**：换了同名文件、改了大小或分片大小 → `fingerprint` 不符 → 整份旧缓存废弃重算，
   绝不会拿旧哈希配上新内容。
2. **只认连续前缀**：加载时按 index 排序，遇到第一个空洞即截断；因此“续算”只需从 `cursor` 开始，
   Worker 永远顺序工作。
3. **每片即时落库**：一片哈希算完就在同一个 IndexedDB 事务里写 chunk + 推进 cursor，
   刷新/掉电最多丢失正在算的那一片。
4. **只缓存哈希、不缓存字节**：GB 级分片字节不进 IndexedDB；续传“已算未传”片时，
   仍从 `File` 重新 `slice` 读字节（读取不占额外副本），只是不再算哈希。
5. 完成校验后前端主动 `clear(fileId)`，服务端成为事实来源。

### 有界流水线的内存保证

- 一个分片从「读字节 → Worker 哈希 → 排队 → HTTP 上传完成」全程占 **1 个槽位**；
- 槽位总数 `maxInflight = 6`（可配），所以浏览器中该文件的分片相关内存 ≈
  `6 × chunkSize`（8MB 片时 ≈ 48MB），**与文件是 4GB 还是 400GB 无关**；
- 单哈希器串行（Worker 同一时刻只处理 1 片）；HTTP 在途请求由独立信号量限制为
  `uploadConcurrency = 3`；
- 自动化测试断言了 `inflight 峰值 ≤ maxInflight` 与「单 Worker 哈希并发峰值 = 1」。

## 目录结构

```
file-chunk-verify/
├── docker-compose.yml          # 本地 MySQL 8
├── README.md
├── server/                     # 后端：Express + MySQL + CAS 磁盘存储
│   ├── sql/schema.sql          # files / cas_chunks / file_chunks / merged_blobs
│   ├── src/
│   │   ├── config.js  db.js  hash.js  storage.js   # storage 为 CAS 内容寻址
│   │   ├── app.js  server.js
│   │   └── routes/
│   │       ├── files.js        # init/precheck/hash/chunks/complete/delete/download
│   │       └── admin.js        # POST /api/admin/gc 孤儿对象回收
│   └── test/                   # 79 条端到端断言 + 2 个真实 HTTP 冒烟
└── web/                        # 前端：Vite + TypeScript
    ├── index.html
    └── src/
        ├── main.ts             # 页面 UI（哈希/上传/流水线三段状态，秒传提示）
        ├── uploader.ts         # 依赖装配：清单秒传/边算边传/算完仲裁/去重跳过
        ├── pipeline.ts         # ★ 有界流水线（含全局 CAS skip 集合与秒传仲裁钩子）
        ├── idb-cache.ts        # IndexedDB 哈希缓存（指纹/连续游标）
        ├── hash.worker.ts      # 唯一 WebWorker：SHA-256
        ├── api.ts  types.ts  style.css
    └── test/
        ├── pipeline.test.mjs   # 52 条流水线断言（边算边传/有界/续算/取消/去重/秒传中止）
        ├── smoke-resume.mjs    # 4GB 大样例中途刷新复用冒烟
        └── import-ts.mjs       # esbuild 内存转译 TS 供 Node 测试
```

## 快速开始

```bash
# 1. MySQL
docker compose up -d mysql

# 2. 后端 :3000
cd server && cp .env.example .env && npm install && npm start

# 3. 前端 :5173
cd ../web && npm install && npm run dev
```

浏览器打开 http://localhost:5173 ，选文件 → 选分片大小（2/4/8/16 MB）→ 开始。
上传中随时可「取消」或直接刷新页面，再点开始即可看到“IndexedDB 复用 N 片 / 服务端跳过 M 片”。

磁盘布局：

```
server/storage/
├── cas/<chunkHash 前2位>/<chunkHash>.part      # 内容寻址分片，全局去重，多文件共享
└── merged/<mergedHash 前2位>/<mergedHash>.bin  # 内容寻址合并产物，秒传共享
```

## 断点续算 / 续传 / 秒传演示

1. 选一个大文件开始，等哈希/上传进行到一半时**刷新页面**（或先点“取消”再开始）。
2. 重新选择同一个文件（`fileId = sha256(名:大小:mtime:分片大小)` 不变）。
3. 日志与进度区会显示：
   - `IndexedDB 复用 X 片`：这些片不再调用 Worker 哈希；
   - `本任务跳过 Y 片`：服务端已有该序号同哈希分片，不上传；
   - `全局 CAS 去重 Z 片`：其它文件上传过相同内容分片，本文件也免传；
   - 已算未传的片只读字节直接上传。
4. 大样例（4GB / 512×8MB，约 30% 处刷新）实测：复用哈希 153 片、跳过上传 149 片，
   第二次只算 359 片、只传 363 片，节省约 1.16 GB 上传流量，峰值在途内存 ≤ 48MB。
   复跑：`cd web && npm run smoke:resume`。
5. **秒传**：再次选择一个已成功上传过的相同文件（即使换文件名、fileId 不同），
   带清单 init 直接返回 `instant=true/status=completed`，零上传立即可下载。
   或在上传过程中服务端恰好已有完整文件，前端会在全哈希算完时自动仲裁秒传、中止剩余上传。

## 完整性校验模型（不变）

- **分片哈希** `SHA-256(分片字节)`：Worker 内 Web Crypto 计算；服务端落盘前重算一次。
- **聚合哈希** `SHA-256(concat(各分片哈希 hex))`：流水线算完后由 `POST /hash` 补报锁定；
  `complete` 时服务端用磁盘分片重算比对。
- **合并文件哈希**：流式合并时同时计算 `SHA-256(完整文件)`，用于端到端确认（与聚合哈希不同值）。

## HTTP 接口与顺序约束

> 完整接口（含 `precheck`、`DELETE`、`download`、`/admin/gc` 与 `instant/hits/dedup` 字段）
> 见上方「迭代三 · HTTP 接口（v3）」。此处仅说明分阶段顺序约束与错误码。

合法顺序：

```
init(fileHash=null)                         # 边算边传
  ├─ 可任意交错：POST chunks/*  与  POST /hash
  └─ 最后：POST /complete
init(fileHash+chunkHashes[])                # 直接秒传/命中判定
  ├─ instant=true  → 零上传完成
  └─ instant=false → 只传 hits 未命中的分片
```

状态机：`uploading → merging → completed`（校验失败回退 `uploading`；意外错误 `failed`）。

**错误顺序不会把任务卡死**，对应错误码：

| 场景 | HTTP | code |
| --- | --- | --- |
| 未 `init` 就传分片/补哈希 | 404 | `FILE_NOT_FOUND` |
| `complete` 时还没补报聚合哈希 | 409 | `FILE_HASH_REQUIRED`（状态保持 uploading，补 `/hash` 即可继续） |
| 补报与已锁定值不同的哈希 | 409 | `FILE_HASH_LOCKED`（防止在两个不同文件内容间摇摆） |
| 分片数量不足 / 序号不连续 | 409 | `CHUNKS_INCOMPLETE` |
| 分片大小不符 | 413 | `CHUNK_SIZE_MISMATCH` |
| 分片/聚合哈希不一致（含磁盘被篡改） | 422 | `CHUNK_HASH_MISMATCH` / `AGGREGATE_HASH_MISMATCH` |
| `merging` 期间传分片/补哈希/重复 complete | 409 | `FILE_VERIFYING` |
| `completed` 后任何写操作 | 409 | `FILE_ALREADY_VERIFIED` |
| init 基础参数（名/大小/片大小/片数）冲突 | 409 | `FILE_PARAM_MISMATCH` |
| 未完成就下载 | 409 | `FILE_NOT_READY` |
| 元数据指向的 CAS 物理分片/合并产物缺失 | 409 / 410 | `CHUNK_FILE_MISSING` / `MERGED_BLOB_MISSING` |

并发安全：

- `complete` 用**条件 UPDATE** `... WHERE status='uploading'` 原子抢占；
  MySQL 行锁保证并发的两个 complete 只有一个进入合并，另一个得到 `FILE_VERIFYING`。
- 分片上传在事务内 `SELECT ... FOR UPDATE`，同序号重传同哈希幂等、异哈希覆盖；
  状态变为 `merging` 后新上传被拒绝。

错误响应统一形如：

```json
{ "error": { "code": "CHUNK_HASH_MISMATCH", "message": "...", "details": { } } }
```

## 自动化测试

```bash
# 后端：79 条端到端断言（无需本机 MySQL，内置内存 mock + 真实磁盘 IO）
cd server && npm run test:e2e
#   覆盖：幂等/大小哈希错误/缺片/篡改检出/空文件、分阶段任务、并发 complete 抢占、
#         相同文件秒传零上传、两文件共享分片去重、删其一另一个仍可 complete/下载、
#         引用归零 GC 删除物理文件、并发同 hash 首传幂等且计数正确

cd server && npm run smoke:staged   # 真实 HTTP：40MB 边传边补哈希 + 强校验合并
cd server && npm run smoke:cas      # 真实 HTTP：A 上传→B 秒传→C 部分去重→删除→GC

# 前端流水线：52 条纯逻辑断言
cd web && npm run test:pipeline
#   覆盖：边算边传、有界槽位/HTTP 并发峰值、刷新续算、已算未传直传、取消后少算少传、
#         全局 CAS 命中跳过、全哈希就绪秒传仲裁中止排队项、缓存前缀+去重、失败冒泡、0 分片

cd web && npm run smoke:resume      # 模拟 4GB（512×8MB）约 30% 处刷新的复用收益
```

## 设计要点

- **主线程零阻塞**：所有 SHA-256 在唯一 Worker；`ArrayBuffer` 以 Transferable 转移，零拷贝。
- **内存恒定**：有界槽位 + 随用随切的 `Blob.slice`；服务端合并用高水位流式读写。
- **双端断点**：哈希断点在 IndexedDB（指纹+连续游标），上传断点在服务端清单，二者独立判定、可任意组合。
- **分阶段强一致**：哈希“后补”但 complete 前必须锁定；锁定异值直接冲突，杜绝歧义。
- **内容寻址去重**：物理路径 `cas/<xx>/<chunkHash>.part` 由内容推导，天然防穿越；
  同内容全局一份，靠引用计数安全共享。
- **秒传可信**：秒传不仅比对聚合哈希，还要求捐赠文件的合并产物与每片物理分片可读，
  并由下载/complete 路径的强校验兜底。
- **落盘即校验、完成再重算**：任何一环损坏（网络、磁盘位翻转、人为篡改）都会在 complete 暴露。

## 已知边界 / 可扩展点

- 指纹依赖文件 `lastModified`；极少数“同名同大小同 mtime 但内容变了”的场景，
  分片哈希与服务端清单不一致仍会触发重传/complete 校验失败（安全方向不漏判）。
- GC 当前为手动触发（`POST /api/admin/gc`，默认 5 分钟宽限）；可轻松改为定时任务。
- 未含鉴权/多租户：当前任何知道 fileId（64 位随机哈希）者可操作该任务；
  多租户场景应在 `files` 上加 owner 并在所有路由做归属校验，GC/删除需管理员权限。
- 物理存储为单机磁盘；多节点部署应把 CAS 层换成对象存储（S3/OSS），引用计数与关联表可原样保留。
- 秒传按“完整文件聚合哈希”判定；若要做更激进的“全文件零哈希预秒传”，可在客户端先比对
  文件大小/名称，再决定是否计算清单。
