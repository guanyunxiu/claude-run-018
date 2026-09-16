# 浏览器端 GB 级文件分布式分片上传与校验系统

前端（TypeScript + 单 WebWorker + Web Crypto + Fetch + IndexedDB）把 GB 级本地文件固定大小分片，
在 Worker 中计算每片 SHA-256，采用**有界「哈希 → 上传」流水线**边算边传；分片哈希持久化到
IndexedDB 支持刷新后续算。后端（Node.js + Express + MySQL）为**内容寻址（CAS）**，物理存放处
抽象为 **ObjectStore**：默认 **MinIO/S3 对象存储**（多台后端共享一套），可切本机磁盘（单测）；
跨机器用 **Redis 锁**协调。物理分片按 `chunkHash` 全局去重、引用计数管理生命周期；**相同文件
秒传**、**不同文件共享相同内容分片只存一份**；`complete` 时从对象存储重读全部哈希做强校验后
**服务端拼接合并**，合并产物同样内容寻址并可被秒传复用。多台后端一起传、一起合并、一起清理
都不会乱，进程中途被杀也能重跑到一致。

> 说明：需求描述中 “Node.js，Web 框架（Gin/SpringBoot）” 存在冲突，Gin 为 Go 框架、
> Spring Boot 为 Java 框架。本实现按 Node.js 生态选择 **Express**。

## 迭代四：对象存储（MinIO/S3）+ Redis 跨机器协调（当前版本）

### 架构

```
浏览器 ──► api-a(:3001) ┐
        └► api-b(:3002) ┼──► MySQL（元数据/引用计数/合并租约）
                          ├──► MinIO/S3（唯一物理存放处，内容寻址 key）
                          └──► Redis（跨机器锁：cas:<hash> / merge:<fileId> / gc）
```

- **ObjectStore 抽象**（`server/src/store/`）：`putIfAbsent / getBuffer / getStream /
  stat / delete / listPrefixMeta / compose / copy`。
  - `S3ObjectStore`：MinIO/Amazon S3；`putIfAbsent` 用条件写 `IfNoneMatch:'*'`，
    合并用 **MultipartUpload 服务端拼接**（大分片 `UploadPartCopy`、小分片 `UploadPart`），
    半成品只存在于 `tmp/`，再 copy-if-absent 到内容寻址 key。
  - `LocalObjectStore`：本机磁盘，仅单测/单机；多实例测试里两 app 指向同一目录即等价共享存储。
  - 对象 key 仍按内容哈希拼，**绝不使用 fileId**：`cas/<xx>/<hash>.part`、
    `merged/<xx>/<hash>.bin`、`tmp/<uuid>.tmp`。
- **跨机器锁**（`server/src/store/locker.js`）：`redis`（`SET NX PX` + token 校验释放 + 续租）
  或 `memory`（测试：多 app 共享一把）。

### 并发正确性

| 场景 | 保证 |
| --- | --- |
| 秒传/precheck 判定捐赠者 | 合并产物与每个分片的可读性**全部走 ObjectStore.stat**（S3/MinIO 或本地），不读本机磁盘；S3 模式下也能正确秒传 |
| S3 合并 | ≥5MB 分片走 `UploadPartCopy`（服务端拷贝），更小分片按序累积成 ≥5MB 的 part（最后一片可小），满足 multipart 限制；`UploadPartCopyCommand` 已正确导入 |
| 双机同哈希首传 | 分布式锁 `cas:<hash>` 串行 + 对象条件写只保留一份 + `INSERT IGNORE` 建行后统一 `ref_count+1`，cas_chunks 仅一行、计数精确 |
| 双机同时 complete | Redis `merge:<fileId>` 只放一个进入；DB 条件更新 + **合并租约**（`merge_owner/merge_lease_until`）保证唯一；输的一方 409 |
| 合并到一半崩溃/异常 | 半成品只在 `tmp/`，内容寻址目标要么不存在要么完整；**异常回退 `uploading` 并清租约（不置终态 failed）**，租约过期后可重试并自愈收敛 |
| 多机 GC | 全集群 `gc` 锁只跑一个；**先删库行（事务提交）后删对象**，删前再查无引用（防首传对撞），再对象对账删孤儿；中途被杀重跑幂等 |
| 库有行对象无 | 上传命中时走 ObjectStore 验盘，缺失用请求体重写（`healed:true`）；`/link` 无字节则报 `CHUNK_FILE_MISSING`，前端回退字节上传 |

### docker-compose（2 后端 + MySQL + MinIO + Redis）

```bash
docker compose up -d --build
# api-a: http://localhost:3001   api-b: http://localhost:3002
# MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
# 前端开发服务器代理可指向任意一台（web/vite.config.ts 已代理 /api → :3000）
```

复现并发问题（两台一起打）：

```bash
# 双机同哈希首传、双机同时 complete、A 传一半换 B、GC 对撞、删 A 不影响 B：
cd server && npm run test:multi     # 21 项断言（两个 app 端口 + 共享存储/锁）
```

### 对象存储/锁配置（环境变量）

`OBJECT_STORE=s3|local`（默认 local，单测用；生产/dockerkit 用 s3）、`S3_ENDPOINT/S3_BUCKET/
S3_ACCESS_KEY/S3_SECRET_KEY/S3_FORCE_PATH_STYLE`、`LOCK_DRIVER=redis|memory|none`、
`REDIS_URL`、`LOCK_TTL_MS`。详见 `server/.env.example`。

---

## 迭代三：内容寻址（CAS）· 秒传 · 跨文件去重 · 引用计数 GC

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
- 上传分片（`POST /chunks/:i`）：CAS 行已存在时**先确认物理文件存在且大小一致**，
  通过才 `ref_count+1`；若发现“有库行、无 `.part`”的幽灵片（旧版 GC 中途崩溃遗留），
  用本次请求的字节**原子重写物理片**再关联（响应 `healed:true`），不会带着悬空引用继续。
  同文件同序号同哈希重传为幂等（撤销多加的计数）；同序号不同内容则替换关联、旧哈希 `ref_count-1`。
- **只关联（`POST /chunks/:i/link?hash=`，无请求体）**：当 `init.hits` 表明某片的物理内容已在
  全局 CAS 中存在时，前端**不发送字节**，改调 `/link` 建立本文件的 `file_chunks` 关联并
  `ref_count+1`。注意：“CAS 里有物理片” ≠ “本 fileId 已有关联”，**绝不能因为命中 hits 就裸
  跳过**，否则 `complete` 会 `CHUNKS_INCOMPLETE`。`/link` 同样验盘，物理缺失时返回
  `CHUNK_FILE_MISSING`，前端据此**回退为带字节上传**自愈。
- 秒传：复用捐赠文件的全部分片与合并产物；关联建立是**幂等**的——对目标文件已存在的同序号同哈希
  关联（边算边传时在途上传所建）不重复 `ref_count+1`，仅补齐缺失序号，杜绝引用虚高。
- 删除文件：逐关联 `ref_count-1`、删关联行、删 files 行、合并产物 `ref_count-1`，**不删物理文件**。
- GC（**三阶段、可恢复、幂等**，`POST /api/admin/gc`）：
  1. **事务内只删库行**（`ref_count=0` 且超过 `minAgeSec`，默认 300s，`FOR UPDATE SKIP LOCKED`），
     先提交；不再“事务内先删物理文件”，杜绝回滚后留下「有行无文件」。
  2. **提交后删物理文件**：删盘失败不回滚库一致性，下次 GC 兜底。
  3. **磁盘对账**：删除“库中已无任何行”的物理孤儿（同样受 mtime 宽限保护，避免误删
     正在原子安装的首传分片）。任意阶段中断后重跑都收敛到一致状态。

### 秒传 / 只关联时序

```
客户端                              服务端
 │ init { fileHash, chunkHashes[] }  │  找 file_hash 相同且 status=completed 的捐赠文件
 │ ───────────────────────────────► │  校验：合并产物存在 + 全分片关联齐 + 物理可读
 │                                   │  复用：新 files 置 completed，对“缺失序号”计数+1
 │                                   │  （已存在的同序号同哈希关联幂等，不重复计数）
 │ ◄──── instant=true, completed ────│  → 客户端零上传，直接可下载
 │
 │ （仅部分分片命中）                 │  init 返回 hits={index:hash}（全局 CAS 已有物理片）
 │ ◄──── instant=false, hits ───────│  命中片：POST /chunks/:i/link?hash=（只关联、零字节）
 │                                   │  未命中片：POST /chunks/:i（上传字节）
 │                                   │  ⚠ 命中片若不 link 也不上传，complete 会 CHUNKS_INCOMPLETE
```

边算边传的秒传仲裁竞态：全部分片哈希算完的瞬间前端用“带清单 init”仲裁，若服务端此刻已有完整
文件即把任务置为 `completed`；仍在途的上传/关联请求可能收到 `FILE_ALREADY_VERIFIED`，
前端会**忽略该错误**（该文件已由秒传幂等补齐），不让整次上传失败；服务端秒传关联对这些在途片
幂等，因此删除后引用能精确归零、GC 可彻底回收（有“先传若干片再秒传”的专门测试）。

捐赠者选择：服务端取出**全部**同聚合哈希的已完成候选（不再 `LIMIT 1`），按新旧倒序
**逐个**校验合并产物可读、分片关联齐全、逐片物理可读且大小一致，跳过任何损坏候选直到找到
完好者；全部损坏则不秒传（可走字节上传自愈）。测试模拟“最新捐赠者关联不完整、更老者完好”。

旧库迁移：`files.file_hash` 自迭代二起允许 NULL。后端启动时查 `information_schema`，
若旧库该列仍为 `NOT NULL` 则幂等执行 `ALTER TABLE files MODIFY file_hash CHAR(64) NULL`，
保证真 MySQL 上分阶段 `init(fileHash=null)` 不再 `ER_BAD_NULL_ERROR`（`npm run test:migrate`）。

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
| `POST` | `/api/files/:fileId/chunks/:index?hash=` | 上传分片；响应含 `dedup`（命中全局 CAS） |
| `POST` | `/api/files/:fileId/chunks/:index/link?hash=` | **只关联**已存在的 CAS 分片（无请求体、不传字节）；哈希不存在返回 409 `CAS_CHUNK_NOT_FOUND` |
| `POST` | `/api/files/:fileId/complete` | 重算 CAS 分片强校验 → 流式合并（共享分片可读）→ 引用合并产物 |
| `DELETE` | `/api/files/:fileId` | 删除文件（仅减引用，`physicalRemoved:false`） |
| `GET`  | `/api/files/:fileId/download` | 流式下载已完成文件（读共享合并产物） |
| `POST` | `/api/admin/gc` | 回收零引用孤儿分片/合并产物，body `{minAgeSec?}` |

错误码（新增/相关）：`FILE_NOT_READY`（未完成禁止下载）、`MERGED_BLOB_MISSING`、
`CHUNK_FILE_MISSING`（CAS 元数据指向空文件）、`CAS_CHUNK_NOT_FOUND`（link 的哈希在全局 CAS
不存在，必须改走字节上传）、`FILE_HASH_REQUIRED`、`FILE_HASH_LOCKED`、
`FILE_VERIFYING`、`FILE_ALREADY_VERIFIED`、`CHUNK_HASH_MISMATCH`、`AGGREGATE_HASH_MISMATCH`、
`CHUNKS_INCOMPLETE`（本 fileId 缺少 `file_chunks` 关联——hits 命中片必须 `/link`，裸跳过即此错误）。

### CAS 强校验为何仍安全

- 分片上传：先在内存重算 SHA-256，与 `?hash=` 不符直接 422 拒绝，绝不写 CAS。
- 秒传：不仅比对聚合哈希，还校验捐赠文件的合并产物与**每一个 CAS 物理分片都存在/可读**，
  防止“元数据指到空文件”的脏秒传。
- complete：无论分片是否去重命中，都**逐个从 CAS 物理路径重读重算哈希**，再算聚合哈希比对，
  因此磁盘位翻转/人为篡改/共享分片损坏都会被 422 检出（测试已覆盖篡改共享分片场景）。

### 自动化测试

```bash
cd server
npm run test:multi     # ★ 21 项多机一致性：两台 app（不同端口）共享对象存储+锁，
                       #   双机同哈希首传(一行/ref=2/一份对象)、双机同时 complete(唯一成功)、
                       #   A 传一半换 B 续传完成、GC 与首传对撞、删 A 不影响 B、归零后对象真删
npm run test:e2e       # 115 条：秒传、跨文件去重、删 A 不影响 B、引用归零 GC、
                       #   并发同 hash 首传幂等、篡改共享分片检出、
                       #   hits 命中片必须 /link 否则 CHUNKS_INCOMPLETE、
                       #   先传若干片再秒传的引用精确性、GC 幽灵片自愈、多捐赠者选择
npm run test:migrate   # 旧库 file_hash NOT NULL → 启动迁移变 NULLABLE（恰好一次 ALTER、幂等）
npm run smoke:staged   # 分阶段边传边补哈希 + 强校验（40MB）
npm run smoke:cas      # 25 项真实 HTTP：A 正常→B 秒传零上传→C 用 link 只关联命中片+传差异片
                       #   →无视 hits 裸跳过被 CHUNKS_INCOMPLETE 拦截→删除→GC 物理回收
npm run smoke:s3       # ★ 29 项真实 @aws-sdk/client-s3 走进程内 S3 兼容桩（等价 MinIO 语义）：
                       #   ≥5MB 分片 UploadPartCopy 完成+下载、小分片 carry 累积、
                       #   捐赠者对象存储可读故秒传/precheck 命中、合并失败回退 uploading 自愈重试

cd ../web
npm run test:pipeline  # 69 条：边算边传、有界槽位、续算续传、全局命中走 link（非裸跳过）、
                       #   秒传仲裁确定性中止排队项、在途请求收 FILE_ALREADY_VERIFIED 被忽略、
                       #   link 发现幽灵片回退字节上传自愈、本任务关联优先
npm run smoke:resume   # 4GB/512 片中途刷新的复用与有界内存
```

> 默认 `OBJECT_STORE=local`、`LOCK_DRIVER=memory`，上述测试无需 Docker/MySQL 即可跑
> （内存 mock DB + 本机磁盘对象存储 + 进程内锁）。要对真 MinIO/Redis 验证，
> `docker compose up -d --build` 后把 `OBJECT_STORE=s3 LOCK_DRIVER=redis` 指向对应端口即可。

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
  │   3. 本 fileId 已有关联？ 是→彻底跳过（无请求）    │
  │      全局 CAS 有该内容？是→POST /chunks/:i/link    │
  │        （零字节，只建本文件关联，ref_count+1）      │
  │      否→进入上传队列 POST /chunks/:i（raw 二进制） │
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
├── docker-compose.yml          # 2 后端 + MySQL + MinIO(S3) + Redis + bucket 初始化
├── README.md
├── server/
│   ├── Dockerfile .dockerignore
│   ├── sql/schema.sql          # files / cas_chunks / file_chunks / merged_blobs
│   ├── src/
│   │   ├── config.js  db.js  hash.js  storage.js  # storage.js 是对象存储外观
│   │   ├── app.js  server.js
│   │   ├── store/              # ★ ObjectStore + 分布式锁
│   │   │   ├── base.js         #   接口约定 + casKey/mergedKey（按内容哈希）
│   │   │   ├── local.js        #   本机磁盘实现（单测）
│   │   │   ├── s3.js           #   MinIO/S3：条件写 + multipart 服务端合并
│   │   │   ├── index.js        #   工厂（OBJECT_STORE）
│   │   │   └── locker.js       #   Redis 锁 / memory 锁
│   │   ├── services/cas.js     # ensureCasChunk：锁+条件写+唯一键，自愈幽灵片
│   │   └── routes/
│   │       ├── files.js        # init/precheck/hash/chunks/link/complete/delete/download
│   │       └── admin.js        # POST /api/admin/gc 跨机安全垃圾回收
│   └── test/                   # multi(21)+e2e(115)+migrate+2 个 HTTP 冒烟
└── web/                        # 前端：Vite + TypeScript
    ├── index.html
    └── src/
        ├── main.ts             # 页面 UI（哈希/上传/流水线三段状态，秒传提示）
        ├── uploader.ts         # 依赖装配：清单秒传/边算边传/算完仲裁/去重 link
        ├── pipeline.ts         # ★ 有界流水线（全局 CAS link + 秒传仲裁 + 自愈回退）
        ├── idb-cache.ts        # IndexedDB 哈希缓存（指纹/连续游标）
        ├── hash.worker.ts      # 唯一 WebWorker：SHA-256
        ├── api.ts  types.ts  style.css
    └── test/
        ├── pipeline.test.mjs   # 69 条流水线断言
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
   - `全局 CAS 去重 Z 片`：其它文件已上传过相同内容分片；本文件**不发送字节**，
     但会调用 `/link` 建立自己的 `file_chunks` 关联（不能裸跳过，否则 complete 缺片）；
   - 已算未传的片只读字节直接上传；全局命中片只发一个零字节的 `/link` 请求。
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

> 最新测试清单与断言数见顶部「迭代三 · 自动化测试」（后端 `test:e2e` / `smoke:staged` /
> `smoke:cas`，前端 `test:pipeline` / `smoke:resume`）。
> 注意：全局 CAS 命中片在前端是调用 `/link` 建立本文件关联（非裸跳过），相关行为以迭代三为准。

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
