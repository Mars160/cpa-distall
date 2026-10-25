# cpa-distill

CLIProxyAPI 原生动态库插件：**只记录成功模型调用的输入和输出**，使用 **Go + sqlc**，支持 **SQLite / PostgreSQL**，内嵌按时间、客户端 Key、模型筛选的记录浏览面板。配置修改沿用 CPA 官方面板，不在本插件重复实现。

## 行为约定

- **透明旁路采集**：所有请求/响应 hook 都返回空修改 `{}`；不修改正文、headers、流式片段，不改变路由，也不吞掉模型错误。
- **只记录成功**：暂存 `request.intercept_before/after`、普通响应或流式片段；仅在 `request.complete.Outcome == "succeeded"` 后进入后台写入队列。失败、拒绝、取消直接丢弃，数据库不留下它们的输入输出。
- **数据库不在请求关键路径上**：hook 只做内存处理和非阻塞入队。`writer_count` 个后台 writer 共享同一个队列，各自用事务把一次成功调用的汇总与原始正文写入它所属的分片，全有或全无。
- **采集失败不影响模型调用**：数据库故障、队列满、容量超限、hook 缺少关联 ID 时跳过采集，只输出不含正文/连接串的限频诊断日志。不会因采集失败向客户端报错。
- **有界、尽力而为**：优先保证代理服务，而不是审计级“绝不丢失”。队列中的数据尚未持久化，进程崩溃、强制退出可能丢失。数据库失败不重试，也不反压模型调用。

流式 hook 仅保留解码后的分片引用，后台 writer 一次合并后写库。单次调用最多保留 65,536 个非空分片引用，超过后整次跳过；空分片不占引用名额。分片底层容量和引用数组容量计入缓冲预算，合并时另检查并预留输出缓冲所需的全局预算；预算不足时整次跳过，不等待、不截断。`max_request_bytes` 约束暂存调用，`max_buffer_bytes` 还约束后台合并暂态（不包括宿主信封、JSON 解析临时分配、数据库驱动及 Go 运行时开销），不是进程 RSS 硬限制。

“透明”不意味着零开销：仍有宿主序列化、跨 ABI 拷贝、JSON 处理和内存暂存开销。不修改 SSE，但不承诺与未加载插件完全相同的时延。

## 调用身份与分片

- **调用身份是 `capture_id`**：插件在 `request.intercept_before` 首次看到某次调用时，用 `crypto/rand` 生成 128 bit 随机值，对外统一表示为 **32 位小写十六进制**，数据库存 **16 字节 BLOB/BYTEA** 并作为主键。同一调用后续所有 hook 都复用它。
- **宿主 `RequestID` 原值保留**：写入 `request_id`（**原样字符串**，不哈希、不截断），**没有唯一约束、也没有索引**，只用于排查（可直接与宿主日志比对），不参与定位。宿主 RequestID 另外用于把同一次调用的 hook 关联到同一份暂存状态。
- **没有 session 概念**：不再有 `session_id`，面板、详情、脚本和数据库都不再按 session + request 读取。
- **SQLite 按 `capture_id` 固定分片**：`writer_count = N` 时，数据目录内固定为 N 个分片库，映射算法固定为 `shard = sha256(capture_id 的 16 字节)[0:8] 按大端 uint64 % N`，写入与按 ID 读取都走同一映射，汇总与正文始终落在**同一个库的同一个事务**里。
- **读取跨分片合并**：列表按 `started_at DESC, capture_id DESC` 全局排序。`capture_id` 唯一，所以这个顺序是全域全序，合并结果确定；每片最多读 `offset + limit` 行，再合并切片，正确性不依赖某个分片持有连续区间。筛选选项（Key、模型）也按分片去重合并。
- **跨分片查询有窗口上限**：合并无法把 offset 下推到单个库，一页最多物化 `分片数 × (offset + limit)` 条汇总，所以分片数 > 1 时只服务 `offset + limit ≤ 10000`（`storage.MaxSearchWindow`）的页，越界直接拒绝（管理接口 400）；面板会禁用超出窗口的下一页/末页并提示缩窄筛选。单库（`writer_count = 1` 或 PostgreSQL）由数据库处理 offset，深页不额外占内存，不受此限制；按 `capture_id` 查看详情也从不经过窗口。
- **Key 映射跨库**：同一 caller_scope 的调用按 capture_id 分散在所有分片，所以 Key 原文会补全到**每一个**分片；补全会依次尝试全部分片（一片失败不会让其余分片被跳过），失败的分片合并返回错误，该 Key 在后续 usage 或调用写入时仍然可重试，不会静默漏掉某个库。插件不自己重排重试队列。
- **预算不分片放大**：`queue_size`、`max_buffer_bytes`、`max_pending_requests` 都是全局的，不随 `writer_count` 变成 N 倍。
- **PostgreSQL 不分片**：`writer_count` 只决定连接池大小和并发 writer 数，所有数据仍在同一个库里。

## 兼容范围

实现对照官方 **CLIProxyAPI v8.0.29**（commit `5fc580b1d72d87a7bcfe5cfb6f479c1aea5625b3`）：

- C ABI version **1**，RPC schema version **6**。
- 依赖 `RequestID` 和 `request_lifecycle_plugin`，不能仅靠 usage 统计判断输入输出所属调用。
- 较旧宿主可能拒绝注册 schema 6；建议使用上述版本或确认兼容的新版本。不能假设所有 CLIProxyAPI v7 二进制均支持。
- 宿主必须支持 CGO 动态插件，管理接口响应头 `X-CPA-SUPPORT-PLUGIN: 1` 可检查该能力。

参考：[官方插件开发文档](https://help.router-for.me/cn/plugin/development)、[请求生命周期示例](https://github.com/router-for-me/CLIProxyAPI/tree/v8.0.29/examples/plugin/request-lifecycle)、[API 类型定义](https://github.com/router-for-me/CLIProxyAPI/blob/v8.0.29/sdk/pluginapi/types.go)。用户原始 `/cn/plugin/` 链接当前返回 404，具体能力文档仍可访问。

## 构建与安装

需要 Go **1.25+**、C 编译器和 `make`。sqlc 生成结果已包含在源码内，正常构建无需安装 sqlc。本项目不依赖整个 CLIProxyAPI SDK，只实现并标注所需 JSON wire contract。

```sh
make build
```

产物为：

```text
dist/darwin/arm64/cpa-distill.dylib   # 示例：macOS Apple Silicon
dist/linux/amd64/cpa-distill.so      # 示例：在 Linux amd64 构建
```

`make` 按当前 Go target 构建；建议在与宿主匹配的 OS/架构上构建，CGO 跨平台构建需要自行配置对应 C 工具链。Windows 产物为 `.dll`。

复制到 **CLIProxyAPI 的**插件目录，例如：

```sh
mkdir -p /path/to/CLIProxyAPI/plugins/darwin/arm64
cp dist/darwin/arm64/cpa-distill.dylib /path/to/CLIProxyAPI/plugins/darwin/arm64/
```

插件注册要求非空 `GitHubRepository`。默认值来自本项目 module path，仅为元数据，并不表示已发布公开仓库。发布到你自己的仓库时可覆盖：

```sh
CGO_ENABLED=1 go build -buildmode=c-shared \
  -ldflags '-X github.com/mars160/cpa-distill/internal/plugin.Repository=https://github.com/YOUR_ORG/YOUR_REPO' \
  -o cpa-distill.dylib ./cmd/cpa-distill
```

## 配置

将 `config.example.yaml` 中的 `plugins` 配置合并到 CLIProxyAPI 的 `config.yaml`。插件 ID 来自动态库文件名，必须为 `cpa-distill`。

### SQLite

```yaml
plugins:
  enabled: true
  dir: plugins
  configs:
    cpa-distill:
      enabled: true
      driver: sqlite
      dsn: /var/lib/cliproxyapi/cpa-distill-data
      writer_count: 4
```

`dsn` 是**数据目录**，不是数据库文件。目录内固定为：

```text
cpa-distill-data/
  manifest.json     # 布局描述：format / version / writer_count / 分片算法 / 文件模板
  shard-00.db       # 每个 writer 一个分片库，writer_count = 1 时也只有 shard-00.db
  shard-01.db
  ...
```

`manifest.json` 示例：

```json
{
  "format": "cpa-distill/capture-shards",
  "version": 1,
  "writer_count": 4,
  "shard_algorithm": "sha256-be64-mod-n",
  "shard_file": "shard-%02d.db"
}
```

语义与拒绝规则：

- 目录不存在时自动创建；父目录必须存在且可写。相对路径按宿主工作目录解析，建议使用绝对路径。
- **manifest 最后发布**：先建好全部分片库，再原子写入 manifest（同目录临时文件 + 硬链接，不覆盖已存在的 manifest）。因此“磁盘上有 manifest”一定意味着“manifest 声明的分片文件都在”。
- **初始化依赖硬链接**：发布 manifest 用 `os.Link`，因为它在目标已存在时报 `EEXIST`，天然排他。不支持硬链接的文件系统（部分网络或对象存储挂载）会让初始化直接失败；插件**不会**退回 rename 或非排他写入，那会允许已有布局被静默覆盖。遇到这种错误请把数据目录放到支持硬链接的本地文件系统。
- 目录已存在时：**有 manifest 必须完全匹配**（格式、版本、`writer_count`、分片算法、文件模板），且 manifest 声明的每个 `shard-XX.db` 必须存在且是普通文件，否则拒绝打开；**没有 manifest 时目录必须为空**，否则拒绝（初始化中途崩溃留下的半成品目录需要人工挪走，不会自动接管）。
- **分片文件丢失时拒绝，而不是补建空库**：重新建一个空库会静默掩盖丢失的数据。请从备份恢复该文件，或把 `dsn` 指向新目录；插件不会自动删除或重建。
- **manifest 发布是排他的**：同一目录被两个进程同时初始化时，已发布的布局不会被覆盖；`writer_count` 不同的并发初始化会被拒绝。一个数据目录同时只应由一个插件实例使用（不要多进程共享同一数据目录）。
- `dsn` 指向已存在的**文件**（包括旧版的 `xxx.db` 单文件库）会被拒绝；`file:` 开头的 SQLite URI 也会被拒绝，插件自己管理 pragma 与分片文件，不解析 URI。
- **绝不自动删除、迁移、改名或改写**已有目录与分片。`writer_count` 与分片算法对数据目录是终身固定的：换配置请换目录或先手工迁移。
- 每个分片使用 WAL、单连接、5 秒 busy timeout；插件自动建表、索引。不兼容的旧表结构会拒绝打开；本版本**没有自动迁移**，请先备份并手动重建或迁移。
- `:memory:` 是显式支持的特例，仅用于测试/开发：进程内一次性内存分片，不写 manifest、不落盘。

### PostgreSQL

```yaml
plugins:
  enabled: true
  dir: plugins
  configs:
    cpa-distill:
      enabled: true
      driver: postgres
      dsn: "postgres://distill:YOUR_PASSWORD@db.example.com:5432/distill?sslmode=verify-full"
      writer_count: 4
```

数据库需预先创建，账户需有建表、建索引、插入、读取与更新权限。支持 pgx/libpq 格式连接串和对应环境变量。连接池大小至少为 `writer_count`（且不小于 4），所以配置的并发 writer 能真正并行写入同一个库。生产建议 TLS；连接凭证与宿主配置应严格控制访问，管理端可能会展示插件配置。

初始化无法打开数据库时，插件注册失败而不是假装采集正常；模型执行链路不依赖该插件提供 executor/router。配置热更新失败会保留旧存储。切换数据库/采集参数时会丢弃尚未结束的调用，避免记录一半输入或跨库拼接，已入队成功调用在总排空预算内写入旧库，超时丢弃剩余队列。

### 可选配置

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `driver` | `sqlite` | `sqlite` / `postgres` |
| `dsn` | `cpa-distill-data` | SQLite **数据目录**（含 manifest 与 shard 文件）；PG 必填连接串 |
| `writer_count` | `1` | 并发 writer 数，1–64。SQLite 为分片库数量，PG 为连接池大小；队列与内存预算不随之放大 |
| `write_timeout` | `5s` | 后台单次成功调用事务超时，最大 `1m` |
| `drain_timeout` | `5s` | 卸载/切库时整个旧队列的总排空预算，最大 `1m` |
| `max_request_bytes` | `67108864` | 每个调用暂存预算，包含 base64 JSON 与事件开销 |
| `max_buffer_bytes` | `268435456` | 暂存、排队、写入中事件的总预算（全局共享，不乘 `writer_count`） |
| `max_pending_requests` | `1024` | 最大未结束调用数 |
| `queue_size` | `32` | 已成功、等待落库的调用数上限（`writer_count` 个 writer 共享这一个队列） |
| `request_ttl` | `30m` | 未收到结束事件的调用的空闲过期时间 |

`writer_count` 超出 1–64 会被直接拒绝，不会静默截断或取默认值。

超限时**跳过整次调用**，不截断后伪装为完整数据。过期回收在新事件到来时触发，最多每分钟扫描一次；连续有片段的长流会刷新空闲时间。内存预算为保留事件的近似上限，不是进程 RSS 硬上限；宿主/ABI/JSON 的短期拷贝也会消耗内存。

正常卸载/关闭会排空已成功调用的队列；`drain_timeout` 只用于取消在途数据库写入并丢弃剩余队列，避免逐批超时导致关闭/热更新长时间卡住。取消是否生效取决于数据库驱动是否响应 context：插件**无论如何都会等全部 writer 退出、连接关闭后才返回**，`Close` 返回后宿主才可以卸载动态库。若驱动忽略取消，关闭会继续等待，而不是提前返回让仍在跑的数据库调用撞上已卸载的库。未完成调用不落库。

## 记录浏览面板

加载插件后，在 CPA 官方面板的插件资源菜单打开 **cpa-distill → 调用记录**；也可以直接访问：

```text
http://你的CPA地址:端口/v0/resource/plugins/cpa-distill/records
```

- 进入后输入 **CPA 管理密钥**连接（不是客户端模型调用 Key）。默认密钥仅保留在当前页面内存中。勾选 **记住登陆** 后，认证成功才会将密钥保存在此浏览器的 localStorage，下次自动连接；不设过期时间，认证失败（HTTP 401 / 403）或点击 **退出并忘记** 时清除。网络故障与服务端 5xx 不会清除。保存按站点和插件 API 路径隔离，不会读取官方面板的凭证。浏览器禁止存储时退回仅内存登录。
- **记住登陆会把管理密钥原文保存在浏览器中**，并不提供加密保险箱保护；请仅在可信设备上使用。同源页面脚本或浏览器扩展可能读取该凭证，清除站点数据也会删除它。不会将密钥注入公开 HTML、放进 URL 或写入插件配置。
- 按开始/结束时间、客户端 Key、模型联合筛选，支持 20 / 50 / 100 条分页。Key 下拉使用掩码标签与短标识区分；模型可选择实际模型或客户端请求别名。
- **跨分片查询最多浏览前 10000 条**（`offset + limit ≤ 10000`）：分片数 > 1 时面板会禁用超出窗口的下一页/末页并提示缩窄筛选，越界请求由接口返回 400；单库（`writer_count = 1` 或 PostgreSQL）没有这个限制，按 `capture_id` 查看详情也不受限制。
- 点击记录查看原始请求正文、原始响应正文（非流式响应体，或按 hook 到达顺序连接的流式字节）与上游 WebSocket 正文。正文按原始字节展示：合法 UTF-8 显示文本，否则显示十六进制预览；单个预览限制 256 KiB，可下载原始字节（不再是事件 JSON）。流原文保持协议内容，不假装已统一转换为最终聊天文本，也不承诺统一提取。
- 面板显示待处理数量、缓冲用量、写入者与分片数量、跳过与写入失败计数。采集是异步的，新记录可能稍后出现，点击查询刷新。
- **配置修改继续用 CPA 官方插件配置页**，本面板没有另起配置文件或写配置接口。

静态页无需认证，但只包含页面代码；记录、详情、选项和状态全部在宿主管理鉴权路由下，资源路径不能用于读取记录。数据接口为：

```text
POST /v0/management/plugins/cpa-distill/records/search
POST /v0/management/plugins/cpa-distill/records/detail   # 请求体 {"capture_id": "<32 位十六进制>"}
GET  /v0/management/plugins/cpa-distill/records/options
GET  /v0/management/plugins/cpa-distill/status
```

详情只按 `capture_id` 定位：先由 ID 算出分片，再按主键读取，不做全分片扫描。ID 格式不合法会返回 400。列表查询在分片数 > 1 时只服务 `offset + limit ≤ 10000` 的页，越界返回 400（提示缩窄筛选或按 `capture_id` 查看）。

这是 v8.0.29 官方插件管理机制的实际入口；该版本没有把插件动态路由挂到 `/v8/management`，不要直接替换前缀。未修改宿主的旧管理实现。

### 客户端 Key 关联

宿主的 request hook 只提供 `Metadata.caller_scope`，其值为 `SHA256("cli-proxy-api:caller-scope:v1\\x00" + trim(clientPrincipal))`，不是 API Key 原文。插件将它作为 `client_key_id`：

1. 只在 **request.intercept_before** 检查客户端候选请求头，且必须与宿主 caller_scope 哈希匹配，才会采纳原文。后续上游请求头绝不作为客户端 Key。
2. 通过 **usage.handle.APIKey** 学习宿主识别的客户端 principal，使用同样的 caller_scope 关联；usage 的 RequestID 与模型执行 RequestID 不同，不能直接按 ID 拼接。
3. usage 晚到时后台只补全 `client_keys` 中该 scope 缺失的 Key 原文，不新建失败调用或孤立凭证记录。

因此 query 参数鉴权也可通过 usage 补全。若宿主没有提供原文，列表仍可按 caller_scope 分组筛选，显示“原文未识别 · 短ID”；连 caller_scope 也没有则归入 unknown。Key 原文学习受 4096 字节单 Key、1024 条缓存、最多 4 MiB 且不超过总缓冲预算 1/16 的限制；补全同样是有界、非阻塞的尽力采集。

## 数据结构与读取

两种数据库都有 `captures` 成功调用汇总表、`capture_bodies` 原始正文表和 `client_keys` 客户端 Key 表；汇总、正文与 Key 在同一事务中写入，不会出现只写了列表却没有正文的半条记录。不再有 `events` 事件表，也不再按 session + request 关联。

### 调用汇总 `captures`

| 列 | 含义 |
| --- | --- |
| `capture_id` | 主键，16 字节 BLOB/BYTEA（128 bit 随机），与 `capture_bodies` 关联，也是定位记录的唯一键 |
| `request_id` | 宿主 RequestID 原值，仅供排查与日志关联；无唯一约束、无索引，不用于定位 |
| `client_key_id` | 宿主 caller_scope（命名空间 SHA-256）筛选标识；无 scope 时数据库存空串，API 显示 `unknown` |
| `model` | 实际执行模型 |
| `requested_model` | 客户端请求的模型名/别名 |
| `source_format` | 客户端协议格式 |
| `stream` | 是否流式调用 |
| `started_at_us`, `completed_at_us` | UTC Unix 微秒，用于时间筛选和排序 |
| `event_count` | 该调用观察到的 hook 数 |

汇总表**不保存**客户端 Key 原文。列表索引为 `(started_at_us, capture_id)`，Key 筛选索引为 `(client_key_id, started_at_us)`。

### 原始正文 `capture_bodies`

| 列 | 含义 |
| --- | --- |
| `capture_id` | 主键，与 `captures` 关联 |
| `request_body` | 客户端首次 `request.intercept_before.Body` 的原始字节；上游 `request.intercept_after` 正文不保存 |
| `response_body` | 非流式 `response.intercept_after.Body`；流式为 `response.intercept_stream_chunk` 中 `ChunkIndex >= 0` 的分片按 hook 到达顺序连接后的原始字节 |
| `websocket_body` | 上游 `websocket.response_event.Payload` 的原始字节；这是上游事件，不是客户端下游 SSE/响应正文的等价物 |

三种正文均为 SQLite BLOB / PG BYTEA，**按原始字节保存**，不做解码、截断或多协议统一提取。流式原文可能不是纯文本，因此不承诺能统一还原为 assistant 文本。

### 客户端 Key `client_keys`

| 列 | 含义 |
| --- | --- |
| `key_id` | 宿主 caller_scope |
| `client_key` | 客户端 API Key 原文，仅保存在数据库，不通过查询 JSON 返回 |

列表以**调用开始时间**降序排列，同一时间按 `capture_id` 降序；时间范围为 `[开始, 结束)`；模型筛选同时精确匹配实际模型或客户端模型名。分页默认 20 条，每页最大 100 条。面板显示本地时间，查询时会转换为带时区时间。

本版本**没有自动迁移**：旧库（含 `events` 表或 `session_id` 主键的版本）不会被自动改写或删除。升级前请自行备份，并按需手动重建空库或迁移数据，插件不会自动删库。

按 **`capture_id`** 读取正文。外层字段名与官方 Go API 一致，例如 `RequestID`、`Body`、`Outcome`。`[]byte` 字段（`Body`、`OriginalRequest`、`RequestBody`、WebSocket `Payload` 等）按 ABI 约定是 **base64**，不是已经解析的 messages。

输入：

- `request.intercept_before.Body`：凭证选择前的输入，写入 `request_body`；
- `request.intercept_after.Body`：执行阶段输入，不写入正文表。

输出：

- 非流式：`response.intercept_after.Body` 写入 `response_body`；
- 流式：`response.intercept_stream_chunk` 中 `ChunkIndex >= 0` 的 `Body` 解码后按 hook 到达顺序连接为 `response_body`；`ChunkIndex == -1` 为初始化，不是内容。SSE、工具调用、reasoning 和 usage 片段均保持原格式，不只抽取文本；
- 上游 WebSocket：`websocket.response_event.Payload` 写入 `websocket_body`，这是上游事件，**不是客户端下游 SSE 的完全等价物**；
- 末尾 `request.complete` 表示该调用整体成功。此前可能有上游失败尝试，插件不另行收集失败响应正文。

SQLite 读取示例（`writer_count > 1` 时记录分散在多个分片，需逐个库查询；`request_id` 是宿主原值，可直接与宿主日志比对）：

```python
import pathlib
import sqlite3

root = pathlib.Path("cpa-distill-data")
for shard in sorted(root.glob("shard-*.db")):
    with sqlite3.connect(shard) as conn:
        rows = conn.execute(
            "SELECT capture_id, request_id FROM captures ORDER BY started_at_us DESC LIMIT 10"
        ).fetchall()
        for capture_id, request_id in rows:
            # capture_id 存的是 16 字节 BLOB；对外表示为 32 位十六进制。
            print(shard.name, capture_id.hex(), request_id)
            row = conn.execute(
                "SELECT request_body, response_body, websocket_body FROM capture_bodies WHERE capture_id=?",
                (capture_id,),
            ).fetchone()
            request_body, response_body, websocket_body = row
            print((request_body or b"").decode("utf-8", errors="replace"))
            print((response_body or b"").decode("utf-8", errors="replace"))
```

也可以直接用面板或管理接口按 `capture_id` 读取，无需自己算分片。

这里采集的是**宿主插件 hook 可见的模型执行输入输出**，并非抓包：不保证覆盖在插件之前被拒绝的 HTTP 请求、所有内部重试细节、宿主所有非模型端点，或其他插件在本插件之后改写的数据。不承诺完整终端用户 HTTP 会话录像。

## 隐私与运维

- 不保存请求头/响应头和完整 `Metadata`，不做上游凭证文件读取，也不输出正文日志。
- **按用户授权，`client_keys.client_key` 保存客户端调用 CPA 的 API Key 原文**。列表、详情 JSON 和筛选选项不返回原文，使用宿主 caller_scope 标识关联及掩码标签展示。这不是数据库加密：数据库文件和备份必须作为凭证资产保护。无法从宿主上下文取得 Key 的调用会归入“未知 Key”，不会猜测或用上游凭证替代。
- **模型正文可能包含密钥、私人对话、工具输出、图片/音频 base64 等敏感信息**。去掉 headers 不等于对正文脱敏；不要把数据库当普通日志公开。
- SQLite 数据目录权限建议 `0700`，其中的 `manifest.json` 与 `shard-XX.db` 建议 `0600`；实际创建权限受宿主 umask 影响，请由部署侧设置（插件创建目录时使用 `0750`）。PG 使用专用低权限账户，限制网络入口。
- 当前不提供自动删除、脱敏、加密和保留期策略。请部署数据库备份、容量监控、磁盘加密、访问授权和定期清理。
- 记录查询路由由宿主管理鉴权保护；静态页面本身不包含记录或配置。面板中的管理密钥只保存在当前页面内存，退出或刷新后需要重新输入。不要公开管理密钥或让不受信任的人访问管理接口。

## 开发与验证

```sh
make test       # 所有 Go 测试 + race detector，PG 无 DSN 时明确 skip
make smoke      # 构建真实动态库，以 ctypes 调用 C ABI，校验 SQLite 分片布局与流式还原
make generate   # 使用固定 sqlc v1.31.1 重新生成 SQLite / PG 类型安全查询
make panel-test # Node.js 18+，无第三方依赖的 DOM 模拟测试（不等于真实浏览器测试）
make check      # race + 动态库 ABI smoke + 前端 DOM 模拟测试
```

存储层基准（`writer_count` 为 1 与多分片两种布局都有覆盖）：

```sh
go test ./internal/storage -run '^$' -bench 'BenchmarkSQLite(AppendCapture|SearchRecords)' -benchmem
go test ./internal/plugin  -run '^$' -bench 'BenchmarkPlugin(EndToEnd|EndToEndSharded|Fragmented)' -benchmem
```

`BenchmarkPluginEndToEnd` 用默认单 writer 布局，`BenchmarkPluginEndToEndSharded` 让 `writer_count` 等于并发度；两者都会逐条核对落库正文并断言零丢弃、零写失败。

PG 实库验证（使用专用测试库）：

```sh
CPA_DISTILL_TEST_POSTGRES_DSN='postgres://user:pass@localhost:5432/distill_test?sslmode=disable' \
  go test -race -v ./internal/storage -run TestPostgresIntegration
```

测试会创建表，并清理自己的测试记录，不应指向生产库。仓库另附 `.github/workflows/test.yml`：Linux/macOS 测试与动态库 smoke，以及使用 PostgreSQL 17 service 的实库测试和 sqlc 生成一致性校验；需推送到 GitHub 后才会执行，不能视为本地已通过。本机没有 PostgreSQL 实库，因此 PG 路径只做了编译、schema 与 SQL 审查，未实测。

本地已验证 C ABI 中的管理路由声明、静态页、带时间/Key/模型条件的分页查询与详情，但尚未完成真实 CPA 服务中的管理鉴权/iframe 浏览器联调；DOM 模拟测试不等于真实浏览器。尝试下载 headless Chromium 时超时，因此没有宣称真实浏览器验收通过。

真实部署还应启动匹配版本 CLIProxyAPI，通过管理密钥请求 `GET /v0/management/plugins`，确认 `registered` 与 `effective_enabled` 均为 `true`，再用自己的模型发起普通/流式成功与失败请求进行端到端验收。C ABI smoke 不等于实际 provider 联调。

## 目录

```text
cmd/cpa-distill/       C ABI 导出与内存边界
internal/plugin/      成功筛选、暂存、非阻塞队列、writer 池和生命周期
internal/config/      YAML 配置校验
internal/panel/       内嵌记录浏览面板及只读查询 API
internal/storage/     分片布局与 manifest、跨分片合并查询、事务与数据库适配，内嵌建表 SQL
internal/storage/*db/ sqlc 生成代码
sql/queries/          共用查询定义
sqlc.yaml             两种方言的 sqlc 配置
scripts/smoke.py      真实动态库 ABI 冒烟测试
```
