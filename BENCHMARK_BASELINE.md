# SQLite / plugin 性能基线（BENCHMARK_BASELINE.md）

本文件是 `cpa-distill` **当前版本**（逐事件写入 `events` 表 + `captures` 摘要表 + client key 延迟回填）
的可重复性能基线。基线只做测量与记录，**未修改任何生产代码或既有测试**。

新增文件（仅这两个 + 本文件）：

- `internal/storage/sqlite_benchmark_test.go`
- `internal/plugin/plugin_benchmark_test.go`

用途：后续把"逐事件存储"改为"请求/响应原始二进制 + 摘要/正文分表 + key 独立表"之后，
用**完全相同的逻辑负载**重跑这两组 benchmark，与本文件逐项对比。

---

## 1. 环境参数

| 项 | 值 |
| --- | --- |
| 主机 | Apple M3，8 核，16 GiB RAM |
| 操作系统 | macOS 27.0.1（build 26A434），darwin/arm64 |
| Go | go1.27.3 darwin/arm64（`go.mod` 声明 `go 1.25.0`） |
| SQLite 驱动 | modernc.org/sqlite v1.38.2（纯 Go 实现，与 CGO 无关；`CGO_ENABLED=1`） |
| 数据库 | 每个 sub-benchmark 在 `b.TempDir()` 下建**真实临时文件**，并断言 `PRAGMA journal_mode = wal` |
| 连接池 | `SetMaxOpenConns(1)` / `SetMaxIdleConns(1)`（与生产一致，写操作串行） |
| PRAGMA | `busy_timeout=5000`、`journal_mode=WAL`、`synchronous=NORMAL` |
| 其他 | `GOFLAGS` 为空，`GOMAXPROCS` 未设置（默认 8） |
| 测量次数 | `-count=3`，本文表格取 3 次**中位数**，原始输出见第 9 节 |

## 2. 复现命令

```sh
# 存储层
go test ./internal/storage -run '^$' -bench BenchmarkSQLite -benchmem -count=3

# 插件端到端（Hook + 真实 SQLite 落库）与 Hook 空 sink 开销
go test ./internal/plugin -run '^$' -bench BenchmarkPlugin -benchmem -count=3
```

本次基线耗时：存储层 `ok ... 112.210s`，插件层 `ok ... 96.221s`。

按需求，本次**未包含**逐事件 `AppendEvent` / `AppendEventConcurrent` 系列（payload size sweep），
只保留与目标方案直接相关的 `AppendBatch` / `AppendCapture` / `AppendCaptureDeferred` 与两个读路径。

## 3. 固定逻辑负载

负载是确定性的（固定 payload、固定事件规模、固定时间戳、固定 key 池），两次运行写入完全相同的数据。

### 3.1 每请求字节负载（plain 与 stream 完全一致）

| 事件 | plain | stream |
| --- | --- | --- |
| `request.intercept_before` | 正文 2 KiB | 正文 2 KiB |
| `request.intercept_after` | 正文 2 KiB | 正文 2 KiB |
| `response.intercept_after` | 正文 24 KiB（完整响应体） | 正文 24 B（marker） |
| `response.intercept_stream_chunk` | - | 8 × 正文 3069 B = 24 KiB |
| `request.complete` | 22 B | 22 B |
| **每请求正文合计** | **28,694 B** | **28,694 B** |
| 事件数 | 4 | 12 |
| 并发度 | 1 / 2 / 4 / 8 | 1 / 2 / 4 / 8 |

即"正常"与"多 chunk 流式"两种形态**承载的正文字节数完全相同**（28,694 B/请求），
差别只在响应体是"一个事件整体承载"还是"8 个 chunk 拆分承载"。
流式 chunk 取 3069 B 正是为了让 24 B marker + 8 个 chunk 恰好等于 24 KiB。

插件 benchmark 另加：每请求 1 次 `usage.handle`（携带 8 个固定 key 之一），
模拟真实宿主流量并触发 key 解析路径；32 请求/轮，key 基数与并发度无关。

### 3.2 落库字节与"真实 Body 信封"

- **插件 benchmark 使用真实 Hook 信封**：正文以 base64 字符串放在 `Body` / `Chunk` 字段里，
  `Metadata.caller_scope` 与 `Authorization` 头也按真实契约提供（`Headers` 必须 marshal 成 JSON 数组）。
- 实测发现（本次基线新增的校验）：插件落库的 payload 是 Hook JSON **去掉 `Metadata` 与 `Headers` 后
  按 key 排序重新 marshal** 的形式，因此落库字节小于宿主发送的信封字节。
  32 请求的实际落库字节被逐 kind 精确校验（见第 5 节），测量值如下：

| 场景 | 宿主发送 `sent-B/req` | 实际落库 `stored-B/req` | 每请求正文 |
| --- | --- | --- | --- |
| plain | 38,905 B | 38,380 B | 28,694 B |
| stream | 39,945 B | 38,660 B | 28,694 B |

  stream 比 plain 多 280 B（+0.7%）落库字节，原因是 8 个 chunk 事件各自带一份 `RequestID` 与字段名，
  属于协议固有开销，不是负载不等。
- **存储层 benchmark 写入的是裸正文字节**（不做 base64、不带 JSON 信封），因此它描述的是
  **保守（偏乐观）的存储布局对比**：真实场景每事件还要多付 base64 膨胀（约 4/3）与 JSON 信封开销。
  存储层数字用于横向比较"不同表结构/索引方案"的相对差异，**不可当作生产实际写入字节量**。

### 3.3 容量配置

插件 benchmark 刻意放大队列与缓冲，使 32 请求的固定轮次不可能因容量被跳过：
`queue_size=4096`、`max_pending_requests=4096`、`max_buffer_bytes=1 GiB`、`max_request_bytes=64 MiB`、
`write_timeout=30s`、`drain_timeout=30s`。
生产默认 `queue_size=32` 在此负载下必然丢请求 —— 这正是第 5 节断言要排除的假吞吐来源。

## 4. 指标语义

### 4.1 存储层（`internal/storage/sqlite_benchmark_test.go`）

- `BenchmarkSQLiteAppendBatch`：一个请求的 4（plain）/12（stream）个事件在**一个事务**内写入，无摘要行。
- `BenchmarkSQLiteAppendCapture`：**生产写路径** —— 摘要行 + 该请求全部事件同一事务，含 client key 回填 UPDATE；
  每个操作用独立 scope，回填保持为有界索引探测。
- `BenchmarkSQLiteAppendCaptureDeferred`：client key 未知时写摘要（key 列为空、不回填）。
- `BenchmarkSQLiteRecordDetail`：读 1 条摘要 + 12 个事件（详情页），预置 200 条摘要。
- `BenchmarkSQLiteSearchRecords`：读记录列表 1 页（含 `COUNT(*)`），20 条/页，预置 200 条摘要。

`ns/op` = 一次"请求级"操作（1 批事件 / 1 次读）的耗时；`MB/s` 由 `b.SetBytes(28,694 B)` 得出；
`B/op`、`allocs/op` 为该操作的平均分配。并发 1/2/4/8 由多个 goroutine 同时发起操作，全部计入计时窗口。

### 4.2 插件（`internal/plugin/plugin_benchmark_test.go`）

每轮固定 32 个请求，`ns/op` 是**整轮**耗时（Hook + 落库等待），不是单请求。

| 指标 | 含义 |
| --- | --- |
| `ns/op` | 一整轮（32 请求）的 Hook + drain 墙钟时间 |
| `MB/s` | 整轮落库字节吞吐（`SetBytes = stored-B/req × 32`） |
| `hook-ns/req` | Hook 阶段墙钟时间 ÷ 32；并发 c 时 Hook 阶段有 c 个 goroutine 并行，故这是**聚合视角**，不是单次 Hook 延迟 |
| `drain-ns/req` | Hook 结束到全部落库完成的时间 ÷ 32（后台 writer 的真实落库吞吐） |
| `persisted-req/s` / `events/s` | 32 ÷（Hook + drain），端到端**已落库**请求/秒、事件/秒 |
| `sent-B/req` / `stored-B/req` | 宿主发送的信封字节数 / 实测落库 payload 字节数（每请求） |
| `B/op`、`allocs/op` | 整轮平均分配（含后台 writer goroutine，Go 基准按进程 MemStats 统计） |

`BenchmarkPluginHookOverhead` 使用**空 sink**（实现 sink 接口、不做任何 I/O），
只报告 `hook-ns/req` 与 `noop-drain-ns/req`：**它不是落库吞吐数字**，
用于把 Hook 自身成本从 SQLite 落库成本中拆出来。

所有信封在计时开始前构建完毕，计时窗口内只有 `Plugin.Handle` 与后台 writer，不含 benchmark 侧的 JSON 组装。

## 5. "不因丢弃而虚报吞吐"的保证

每一轮结束时（任一断言失败即 `b.Fatalf`）：

1. `p.dropped` 增量必须为 **0** —— 没有请求因缓冲、队列、过期或非法事件被跳过；
2. `p.writeFailures` 增量必须为 **0** —— 没有落库失败；
3. 公开 API `Store.SearchRecords` 回读：摘要行数**恰好等于** 32；
4. 所有摘要 `EventCount` 之和**恰好等于** 32 × 每请求事件数（plain 128，stream 384）；
5. 逐 kind 回读 `RecordDetail` 并累加 payload 长度，必须与按事件类型精确计算的落库字节数**完全相等**
   （失败时打印 want/got 逐 kind 对照，便于定位）；
6. 每轮结束前后台 writer 必须完全空闲（`p.bytes == 0` 且 `len(p.pending) == 0`）；
7. `BenchmarkPluginHookOverhead` 另断言空 sink 恰好收到 32 次 `AppendCapture` 与 32 次 `ResolveClientKey`，
   证明 Hook 全路径确实被执行（而不是被短路）。

因此第 6.2 节的 `persisted-req/s`、`events/s` 全部对应**已确认落库**的工作量。

## 6. 结果（3 次中位数）

### 6.1 存储层

| benchmark | ns/op | MB/s | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkSQLiteAppendBatch/plain4/concurrency=1` | 244.8us | 117 | 5.3KB | 125 |
| `BenchmarkSQLiteAppendBatch/plain4/concurrency=2` | 260.5us | 110 | 5.4KB | 128 |
| `BenchmarkSQLiteAppendBatch/plain4/concurrency=4` | 283.8us | 101 | 5.4KB | 127 |
| `BenchmarkSQLiteAppendBatch/plain4/concurrency=8` | 312.3us | 92 | 5.4KB | 127 |
| `BenchmarkSQLiteAppendBatch/stream12/concurrency=1` | 406.5us | 71 | 11.8KB | 349 |
| `BenchmarkSQLiteAppendBatch/stream12/concurrency=2` | 433.8us | 66 | 11.9KB | 352 |
| `BenchmarkSQLiteAppendBatch/stream12/concurrency=4` | 470.0us | 61 | 11.9KB | 351 |
| `BenchmarkSQLiteAppendBatch/stream12/concurrency=8` | 516.9us | 56 | 11.9KB | 351 |
| `BenchmarkSQLiteAppendCapture/plain4/concurrency=1` | 310.3us | 92 | 6.5KB | 171 |
| `BenchmarkSQLiteAppendCapture/plain4/concurrency=2` | 330.5us | 87 | 6.7KB | 173 |
| `BenchmarkSQLiteAppendCapture/plain4/concurrency=4` | 355.9us | 81 | 6.7KB | 173 |
| `BenchmarkSQLiteAppendCapture/plain4/concurrency=8` | 388.9us | 74 | 6.7KB | 173 |
| `BenchmarkSQLiteAppendCapture/stream12/concurrency=1` | 472.0us | 61 | 13.1KB | 395 |
| `BenchmarkSQLiteAppendCapture/stream12/concurrency=2` | 501.9us | 57 | 13.2KB | 397 |
| `BenchmarkSQLiteAppendCapture/stream12/concurrency=4` | 540.1us | 53 | 13.1KB | 397 |
| `BenchmarkSQLiteAppendCapture/stream12/concurrency=8` | 592.6us | 48 | 13.1KB | 396 |
| `BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=1` | 463.5us | 62 | 12.8KB | 381 |
| `BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=2` | 494.7us | 58 | 12.9KB | 383 |
| `BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=4` | 534.5us | 54 | 12.9KB | 383 |
| `BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=8` | 587.0us | 49 | 12.8KB | 382 |
| `BenchmarkSQLiteRecordDetail` | 54.0us | - | 69.3KB | 310 |
| `BenchmarkSQLiteSearchRecords` | 149.2us | - | 22.0KB | 481 |

### 6.2 插件端到端（Hook + 真实 SQLite）

| benchmark | ns/op | MB/s | B/op | allocs/op | hook-ns/req | drain-ns/req | persisted-req/s | events/s | sent-B/req | stored-B/req |
|---|---|---|---|---|---|---|---|---|---|---|
| `BenchmarkPluginEndToEnd/plain/concurrency=1` | 8.32ms | 148 | 3.66MB | 13,942 | 72.5us | 187.3us | 3,846 | 15,384 | 38,905 | 38,380 |
| `BenchmarkPluginEndToEnd/plain/concurrency=2` | 8.31ms | 148 | 3.78MB | 13,997 | 43.4us | 216.4us | 3,850 | 15,400 | 38,905 | 38,380 |
| `BenchmarkPluginEndToEnd/plain/concurrency=4` | 8.48ms | 145 | 3.91MB | 14,030 | 38.5us | 226.4us | 3,776 | 15,103 | 38,905 | 38,380 |
| `BenchmarkPluginEndToEnd/plain/concurrency=8` | 8.61ms | 143 | 3.95MB | 14,029 | 38.3us | 231.4us | 3,718 | 14,871 | 38,905 | 38,380 |
| `BenchmarkPluginEndToEnd/stream/concurrency=1` | 15.21ms | 81 | 4.09MB | 34,746 | 97.9us | 377.2us | 2,105 | 25,257 | 39,945 | 38,660 |
| `BenchmarkPluginEndToEnd/stream/concurrency=2` | 15.35ms | 81 | 4.13MB | 34,830 | 65.0us | 414.2us | 2,084 | 25,014 | 39,945 | 38,660 |
| `BenchmarkPluginEndToEnd/stream/concurrency=4` | 15.51ms | 80 | 4.13MB | 34,845 | 60.2us | 424.6us | 2,064 | 24,767 | 39,945 | 38,660 |
| `BenchmarkPluginEndToEnd/stream/concurrency=8` | 15.69ms | 79 | 4.14MB | 34,857 | 59.5us | 430.3us | 2,040 | 24,481 | 39,945 | 38,660 |

### 6.3 插件 Hook 空 sink 开销（非落库吞吐）

| benchmark | ns/op | MB/s | B/op | allocs/op | hook-ns/req | noop-drain-ns/req |
|---|---|---|---|---|---|---|
| `BenchmarkPluginHookOverhead/plain/concurrency=1` | 2.41ms | 510 | 3.40MB | 7,846 | 71.5us | 3.7us |
| `BenchmarkPluginHookOverhead/plain/concurrency=2` | 1.43ms | 859 | 3.45MB | 7,867 | 40.8us | 3.9us |
| `BenchmarkPluginHookOverhead/plain/concurrency=4` | 1.07ms | 1149 | 3.56MB | 7,891 | 29.4us | 3.9us |
| `BenchmarkPluginHookOverhead/plain/concurrency=8` | 1.09ms | 1128 | 3.62MB | 7,903 | 30.1us | 3.9us |
| `BenchmarkPluginHookOverhead/stream/concurrency=1` | 3.21ms | 386 | 3.62MB | 21,482 | 96.3us | 3.9us |
| `BenchmarkPluginHookOverhead/stream/concurrency=2` | 2.14ms | 578 | 3.63MB | 21,512 | 63.0us | 4.0us |
| `BenchmarkPluginHookOverhead/stream/concurrency=4` | 1.79ms | 693 | 3.63MB | 21,528 | 51.8us | 3.9us |
| `BenchmarkPluginHookOverhead/stream/concurrency=8` | 1.78ms | 693 | 3.64MB | 21,541 | 51.7us | 3.9us |

## 7. 关键观察

1. **落库是端到端瓶颈，Hook 不是**：plain 并发 8 时 `hook-ns/req` 38.3us 对 `drain-ns/req` 231.4us，
   drain 占端到端 **86%**；stream 并发 8 时 59.5us 对 430.3us，drain 占 **88%**。
2. **Hook 成本与 writer 无关（异步生效）**：真实 SQLite 场景的 `hook-ns/req`（plain c1 72.5us、c4 38.5us）
   与空 sink 场景（71.5us / 29.4us）基本一致，说明后台落库慢不会拖慢 Hook；
   Hook 并发提升带来的收益来自 goroutine 并行（c1→c4 约 2.5×），c4→c8 基本饱和（30us 左右）。
3. **相同字节负载下，流式拆 chunk 更贵但事件吞吐更高**：两者每请求正文同为 28,694 B，
   plain 端到端 3,718–3,850 req/s（14.9k–15.4k events/s），stream 2,040–2,105 req/s（24.5k–25.3k events/s）。
   流式请求数吞吐约为 plain 的 55%，但事件吞吐是 1.6×；每请求固定开销（摘要行、事务、调度）被摊薄。
4. **摘要行 + key 回填的增量成本约 65us/请求**：`AppendCapture` 比同规模 `AppendBatch`
   多 65.5us（plain c1 310.3 vs 244.8）、65.5us（stream c1 472.0 vs 406.5）。
   其中回填 UPDATE 本身很小：`AppendCaptureDeferred`（不回填）比 `AppendCapture` 快约 8.5us（463.5 vs 472.0）。
5. **单连接池下并发没有收益，反而略微变差**：`AppendCapture` plain 并发 1→8 由 310.3us 升到 388.9us（-20% 吞吐），
   并发越高争用越明显。当前 SQLite 引擎的实际写入能力由单个连接串行决定。
6. **读路径远低于写路径**：详情读 54.0us、列表页读 149.2us；写路径 310–593us。
   后续"摘要/正文分表"若把正文从摘要行拆开，应重点盯住第 6.1 节写路径与第 6.2 节 `drain-ns/req`。
7. **落库 payload 会被裁剪**：`Metadata` 与 `Headers` 不落库，且落库 JSON 是重新 marshal 的排序形式
   （plain 每请求 38,380 B vs 发送 38,905 B）。这是当前实现的事实，改造正文存储方案时需要一并考虑
   （例如"请求/响应原始二进制"是否要保留裁剪后的语义）。
8. 全部轮次满足第 5 节七项断言（drops=0、write_failures=0、摘要 32 行、事件 128/384、
   逐 kind 落库字节精确匹配、writer 空闲、空 sink 批次数精确），因此第 6.2 节的吞吐未被丢弃请求虚报。

## 8. 重构后如何复用

- **负载参数必须保持不变**才能对比：每请求正文 28,694 B（2 KiB + 2 KiB + 24 KiB 响应体）、
  plain 4 事件 / stream 8 chunk、并发 1/2/4/8、每轮 32 请求、key 池 8、每请求 1 次 `usage.handle`、
  容量配置同上。
- 存储层 benchmark 只用**导出 API**（`AppendBatch` / `AppendCapture` / `RecordDetail` / `SearchRecords`）。
  若重构改变写入 API（例如新增"请求/响应原始二进制"专用方法），只需替换 benchmark 中的写入调用，
  负载与断言口径不变。
- 插件 benchmark 的验证依赖三处实现细节，重构时需同步调整：
  `p.dropped` / `p.writeFailures` 计数器、把 `p.current.sink` 断言为 `*storage.Store` 后回读、
  以及"落库 payload = 去掉 Metadata/Headers 后重排 marshal"的字节模型（`benchStoredEnvelope`）。
  这些断言是"不虚报吞吐"的核心，不要删除；若裁剪语义变化，应显式更新 `benchStoredEnvelope`
  而不是放宽为范围比较。
- 读路径与写路径分别对比；分表后"摘要读"与"正文读"可能成为两个查询，建议各自新增 benchmark，
  但保持 200 条预置、12 事件/条、20 条/页的口径。

---

## 9. 原始输出

### 9.1 存储层（`go test ./internal/storage -run '^$' -bench BenchmarkSQLite -benchmem -count=3`）

```text
goos: darwin
goarch: arm64
pkg: github.com/mars160/cpa-distill/internal/storage
cpu: Apple M3
BenchmarkSQLiteAppendBatch/plain4/concurrency=1-8         	    4596	    250689 ns/op	 114.46 MB/s	    5284 B/op	     125 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=1-8         	    5564	    243739 ns/op	 117.73 MB/s	    5261 B/op	     125 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=1-8         	    6362	    244797 ns/op	 117.22 MB/s	    5263 B/op	     125 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=2-8         	    6380	    260474 ns/op	 110.16 MB/s	    5413 B/op	     128 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=2-8         	    6270	    259975 ns/op	 110.38 MB/s	    5404 B/op	     128 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=2-8         	    6337	    260740 ns/op	 110.05 MB/s	    5402 B/op	     128 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=4-8         	    6276	    282551 ns/op	 101.56 MB/s	    5392 B/op	     127 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=4-8         	    6274	    284288 ns/op	 100.94 MB/s	    5388 B/op	     127 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=4-8         	    6214	    283805 ns/op	 101.11 MB/s	    5390 B/op	     127 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=8-8         	    6235	    313080 ns/op	  91.65 MB/s	    5377 B/op	     127 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=8-8         	    6300	    311856 ns/op	  92.01 MB/s	    5377 B/op	     127 allocs/op
BenchmarkSQLiteAppendBatch/plain4/concurrency=8-8         	    6319	    312323 ns/op	  91.88 MB/s	    5380 B/op	     127 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=1-8       	    3684	    407725 ns/op	  70.38 MB/s	   11788 B/op	     349 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=1-8       	    3720	    406504 ns/op	  70.59 MB/s	   11782 B/op	     349 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=1-8       	    3740	    406518 ns/op	  70.59 MB/s	   11790 B/op	     349 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=2-8       	    3283	    434314 ns/op	  66.07 MB/s	   11901 B/op	     352 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=2-8       	    3295	    433759 ns/op	  66.15 MB/s	   11902 B/op	     352 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=2-8       	    3265	    432927 ns/op	  66.28 MB/s	   11893 B/op	     351 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=4-8       	    3634	    482403 ns/op	  59.48 MB/s	   11866 B/op	     351 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=4-8       	    3564	    465699 ns/op	  61.62 MB/s	   11862 B/op	     351 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=4-8       	    3584	    469956 ns/op	  61.06 MB/s	   11864 B/op	     351 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=8-8       	    3613	    518610 ns/op	  55.33 MB/s	   11862 B/op	     351 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=8-8       	    3613	    514174 ns/op	  55.81 MB/s	   11863 B/op	     351 allocs/op
BenchmarkSQLiteAppendBatch/stream12/concurrency=8-8       	    3622	    516860 ns/op	  55.52 MB/s	   11860 B/op	     351 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=1-8       	    4784	    310481 ns/op	  92.42 MB/s	    6536 B/op	     171 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=1-8       	    4797	    309572 ns/op	  92.69 MB/s	    6535 B/op	     171 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=1-8       	    4759	    310274 ns/op	  92.48 MB/s	    6537 B/op	     171 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=2-8       	    4802	    330534 ns/op	  86.81 MB/s	    6666 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=2-8       	    4767	    331157 ns/op	  86.65 MB/s	    6673 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=2-8       	    4747	    327623 ns/op	  87.59 MB/s	    6670 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=4-8       	    4748	    355855 ns/op	  80.64 MB/s	    6655 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=4-8       	    4632	    354483 ns/op	  80.95 MB/s	    6652 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=4-8       	    4689	    357429 ns/op	  80.28 MB/s	    6658 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=8-8       	    4741	    390957 ns/op	  73.40 MB/s	    6650 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=8-8       	    4746	    388879 ns/op	  73.79 MB/s	    6647 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/plain4/concurrency=8-8       	    4756	    387590 ns/op	  74.03 MB/s	    6652 B/op	     173 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=1-8     	    3099	    478851 ns/op	  59.92 MB/s	   13047 B/op	     394 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=1-8     	    3117	    471981 ns/op	  60.80 MB/s	   13056 B/op	     395 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=1-8     	    3135	    468640 ns/op	  61.23 MB/s	   13053 B/op	     395 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=2-8     	    3022	    501739 ns/op	  57.19 MB/s	   13163 B/op	     397 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=2-8     	    3028	    503068 ns/op	  57.04 MB/s	   13165 B/op	     397 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=2-8     	    3025	    501895 ns/op	  57.17 MB/s	   13165 B/op	     397 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=4-8     	    3007	    540104 ns/op	  53.13 MB/s	   13139 B/op	     397 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=4-8     	    3034	    541219 ns/op	  53.02 MB/s	   13134 B/op	     397 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=4-8     	    3027	    538915 ns/op	  53.25 MB/s	   13138 B/op	     397 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=8-8     	    3020	    588884 ns/op	  48.73 MB/s	   13134 B/op	     396 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=8-8     	    3018	    596248 ns/op	  48.13 MB/s	   13124 B/op	     396 allocs/op
BenchmarkSQLiteAppendCapture/stream12/concurrency=8-8     	    3015	    592566 ns/op	  48.43 MB/s	   13131 B/op	     396 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=1-8         	    3141	    463530 ns/op	  61.91 MB/s	   12773 B/op	     381 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=1-8         	    3189	    466018 ns/op	  61.57 MB/s	   12778 B/op	     381 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=1-8         	    3150	    462247 ns/op	  62.08 MB/s	   12772 B/op	     381 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=2-8         	    3073	    491641 ns/op	  58.37 MB/s	   12893 B/op	     383 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=2-8         	    3078	    494669 ns/op	  58.01 MB/s	   12885 B/op	     383 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=2-8         	    3026	    726148 ns/op	  39.52 MB/s	   12892 B/op	     383 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=4-8         	    3068	    558290 ns/op	  51.40 MB/s	   12857 B/op	     383 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=4-8         	    3049	    531322 ns/op	  54.01 MB/s	   12856 B/op	     383 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=4-8         	    2964	    534509 ns/op	  53.68 MB/s	   12852 B/op	     383 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=8-8         	    3096	    582026 ns/op	  49.30 MB/s	   12846 B/op	     382 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=8-8         	    3050	    591722 ns/op	  48.49 MB/s	   12846 B/op	     382 allocs/op
BenchmarkSQLiteAppendCaptureDeferred/stream12/concurrency=8-8         	    3042	    587014 ns/op	  48.88 MB/s	   12847 B/op	     382 allocs/op
BenchmarkSQLiteRecordDetail-8                                         	   22216	     53957 ns/op	   69314 B/op	     310 allocs/op
BenchmarkSQLiteRecordDetail-8                                         	   22119	     54196 ns/op	   69313 B/op	     310 allocs/op
BenchmarkSQLiteRecordDetail-8                                         	   21726	     53912 ns/op	   69314 B/op	     310 allocs/op
BenchmarkSQLiteSearchRecords-8                                        	    7870	    149153 ns/op	   21976 B/op	     481 allocs/op
BenchmarkSQLiteSearchRecords-8                                        	    7838	    149175 ns/op	   21976 B/op	     481 allocs/op
BenchmarkSQLiteSearchRecords-8                                        	    7861	    149226 ns/op	   21976 B/op	     481 allocs/op
PASS
ok  	github.com/mars160/cpa-distill/internal/storage	112.210s
```

### 9.2 插件（`go test ./internal/plugin -run '^$' -bench BenchmarkPlugin -benchmem -count=3`）

```text
goos: darwin
goarch: arm64
pkg: github.com/mars160/cpa-distill/internal/plugin
cpu: Apple M3
BenchmarkPluginEndToEnd/plain/concurrency=1-8         	     142	   8342964 ns/op	 147.21 MB/s	    188325 drain-ns/req	     15347 events/s	     72312 hook-ns/req	      3837 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3656741 B/op	   13943 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=1-8         	     141	   8307957 ns/op	 147.83 MB/s	    187044 drain-ns/req	     15412 events/s	     72502 hook-ns/req	      3853 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3658894 B/op	   13942 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=1-8         	     144	   8322686 ns/op	 147.57 MB/s	    187339 drain-ns/req	     15384 events/s	     72663 hook-ns/req	      3846 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3655797 B/op	   13942 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=2-8         	     142	   8345404 ns/op	 147.17 MB/s	    217297 drain-ns/req	     15343 events/s	     43412 hook-ns/req	      3836 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3775345 B/op	   13997 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=2-8         	     144	   8304214 ns/op	 147.90 MB/s	    216448 drain-ns/req	     15419 events/s	     42976 hook-ns/req	      3855 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3774359 B/op	   13997 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=2-8         	     142	   8314512 ns/op	 147.71 MB/s	    216159 drain-ns/req	     15400 events/s	     43587 hook-ns/req	      3850 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3778952 B/op	   13998 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=4-8         	     141	   8478165 ns/op	 144.86 MB/s	    226497 drain-ns/req	     15103 events/s	     38346 hook-ns/req	      3776 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3910739 B/op	   14027 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=4-8         	     140	   8480245 ns/op	 144.83 MB/s	    226190 drain-ns/req	     15100 events/s	     38718 hook-ns/req	      3775 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3912838 B/op	   14032 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=4-8         	     140	   8477857 ns/op	 144.87 MB/s	    226368 drain-ns/req	     15104 events/s	     38464 hook-ns/req	      3776 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3908338 B/op	   14030 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=8-8         	     139	   8899807 ns/op	 138.00 MB/s	    239508 drain-ns/req	     14389 events/s	     38486 hook-ns/req	      3597 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3957587 B/op	   14035 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=8-8         	     139	   8611692 ns/op	 142.62 MB/s	    230734 drain-ns/req	     14871 events/s	     38254 hook-ns/req	      3718 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3946549 B/op	   14026 allocs/op
BenchmarkPluginEndToEnd/plain/concurrency=8-8         	     139	   8610631 ns/op	 142.63 MB/s	    231421 drain-ns/req	     14872 events/s	     37538 hook-ns/req	      3718 persisted-req/s	     38905 sent-B/req	     38380 stored-B/req	 3945741 B/op	   14029 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=1-8        	      79	  15205941 ns/op	  81.36 MB/s	    377165 drain-ns/req	     25257 events/s	     97943 hook-ns/req	      2105 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4089276 B/op	   34747 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=1-8        	      78	  15196856 ns/op	  81.41 MB/s	    376549 drain-ns/req	     25272 events/s	     98279 hook-ns/req	      2106 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4088363 B/op	   34746 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=1-8        	      79	  15211330 ns/op	  81.33 MB/s	    378023 drain-ns/req	     25248 events/s	     97257 hook-ns/req	      2104 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4087357 B/op	   34746 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=2-8        	      76	  15337396 ns/op	  80.66 MB/s	    414188 drain-ns/req	     25041 events/s	     65023 hook-ns/req	      2087 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4127197 B/op	   34830 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=2-8        	      78	  15356314 ns/op	  80.56 MB/s	    415423 drain-ns/req	     25011 events/s	     64376 hook-ns/req	      2084 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4125003 B/op	   34824 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=2-8        	      78	  15354236 ns/op	  80.57 MB/s	    414163 drain-ns/req	     25014 events/s	     65570 hook-ns/req	      2084 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4127086 B/op	   34831 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=4-8        	      78	  15494795 ns/op	  79.84 MB/s	    423625 drain-ns/req	     24788 events/s	     60489 hook-ns/req	      2066 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4132448 B/op	   34845 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=4-8        	      78	  15517005 ns/op	  79.73 MB/s	    424558 drain-ns/req	     24752 events/s	     60249 hook-ns/req	      2063 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4135977 B/op	   34846 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=4-8        	      75	  15507665 ns/op	  79.77 MB/s	    424860 drain-ns/req	     24767 events/s	     59657 hook-ns/req	      2064 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4130116 B/op	   34839 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=8-8        	      76	  15689194 ns/op	  78.85 MB/s	    430334 drain-ns/req	     24481 events/s	     59835 hook-ns/req	      2040 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4135977 B/op	   34857 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=8-8        	      70	  15714516 ns/op	  78.72 MB/s	    431502 drain-ns/req	     24442 events/s	     59456 hook-ns/req	      2037 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4135225 B/op	   34860 allocs/op
BenchmarkPluginEndToEnd/stream/concurrency=8-8        	      75	  15661216 ns/op	  78.99 MB/s	    430155 drain-ns/req	     24525 events/s	     59142 hook-ns/req	      2044 persisted-req/s	     39945 sent-B/req	     38660 stored-B/req	 4133051 B/op	   34856 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=1-8     	     496	   2404630 ns/op	 510.75 MB/s	     71385 hook-ns/req	      3723 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3397534 B/op	    7848 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=1-8     	     499	   2411865 ns/op	 509.22 MB/s	     71451 hook-ns/req	      3885 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3394820 B/op	    7846 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=1-8     	     493	   2407951 ns/op	 510.04 MB/s	     71506 hook-ns/req	      3706 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3396139 B/op	    7846 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=2-8     	     836	   1431361 ns/op	 858.04 MB/s	     40792 hook-ns/req	      3892 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3452261 B/op	    7868 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=2-8     	     831	   1429327 ns/op	 859.26 MB/s	     40769 hook-ns/req	      3853 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3452787 B/op	    7867 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=2-8     	     836	   1426980 ns/op	 860.67 MB/s	     40691 hook-ns/req	      3858 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3451760 B/op	    7867 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=4-8     	    1129	   1064248 ns/op	1154.02 MB/s	     29345 hook-ns/req	      3858 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3561562 B/op	    7891 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=4-8     	    1119	   1069568 ns/op	1148.28 MB/s	     29485 hook-ns/req	      3883 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3562339 B/op	    7890 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=4-8     	    1106	   1069341 ns/op	1148.52 MB/s	     29446 hook-ns/req	      3912 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3563733 B/op	    7892 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=8-8     	    1119	   1112225 ns/op	1104.24 MB/s	     30798 hook-ns/req	      3887 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3616187 B/op	    7903 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=8-8     	    1101	   1082130 ns/op	1134.95 MB/s	     29861 hook-ns/req	      3885 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3617768 B/op	    7902 allocs/op
BenchmarkPluginHookOverhead/plain/concurrency=8-8     	    1114	   1089055 ns/op	1127.73 MB/s	     30070 hook-ns/req	      3893 noop-drain-ns/req	     38905 sent-B/req	     38380 stored-B/req	 3622292 B/op	    7904 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=1-8    	     372	   3210292 ns/op	 385.36 MB/s	     96352 hook-ns/req	      3933 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3618000 B/op	   21482 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=1-8    	     372	   3207660 ns/op	 385.68 MB/s	     96307 hook-ns/req	      3897 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3617587 B/op	   21481 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=1-8    	     375	   3199411 ns/op	 386.67 MB/s	     96069 hook-ns/req	      3878 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3617867 B/op	   21482 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=2-8    	     553	   2146572 ns/op	 576.32 MB/s	     63074 hook-ns/req	      3964 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3629007 B/op	   21512 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=2-8    	     558	   2141222 ns/op	 577.76 MB/s	     62913 hook-ns/req	      3957 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3630419 B/op	   21515 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=2-8    	     555	   2141486 ns/op	 577.69 MB/s	     62959 hook-ns/req	      3920 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3628713 B/op	   21512 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=4-8    	     675	   1785461 ns/op	 692.89 MB/s	     51834 hook-ns/req	      3909 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3635149 B/op	   21528 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=4-8    	     670	   1775263 ns/op	 696.87 MB/s	     51521 hook-ns/req	      3904 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3634532 B/op	   21527 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=4-8    	     674	   1792401 ns/op	 690.20 MB/s	     52014 hook-ns/req	      3942 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3634737 B/op	   21528 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=8-8    	     680	   1784227 ns/op	 693.36 MB/s	     51743 hook-ns/req	      3948 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3636177 B/op	   21540 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=8-8    	     672	   1781684 ns/op	 694.35 MB/s	     51681 hook-ns/req	      3931 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3636223 B/op	   21541 allocs/op
BenchmarkPluginHookOverhead/stream/concurrency=8-8    	     676	   1787712 ns/op	 692.01 MB/s	     51837 hook-ns/req	      3963 noop-drain-ns/req	     39945 sent-B/req	     38660 stored-B/req	 3636486 B/op	   21541 allocs/op
PASS
ok  	github.com/mars160/cpa-distill/internal/plugin	96.221s
```
