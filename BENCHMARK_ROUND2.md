# 第二轮性能优化对比

对照版本：`67cc2d6`（已采用原始正文分表），不是最初的事件存储版本。

## 方法

同机 Apple M3 / Go 1.27.3 / SQLite WAL 文件，单writer、单连接，三次中位数。普通及8-chunk流式使用原有负载；新增128/1024-chunk负载分别固定响应为24KiB/1MiB。每轮32请求，逐字节验证正文、0丢弃、0写入失败；不包含宿主序列化、C ABI及网络模型耗时。常规基准默认benchtime，分片扩展使用`-benchtime=3x -count=3`，后者是短轮次趋势测试，不能把小幅差异当成稳定结论。

本轮实现：分片引用暂存、后台精确分配合并；摘要解析移到锁外；成功持久化Key后跳过后续capture和usage的重复Key写入。容量计数增量维护，避免逐chunk遍历造成平方复杂度。无schema改动、无批量提交/压缩/连接池调整。

## 常规端到端吞吐（请求/秒）

|类型|并发|之前|之后|变化|
|---|---:|---:|---:|---:|
|plain|1|6704|7235|+7.9%|
|plain|2|6528|6921|+6.0%|
|plain|4|6327|6693|+5.8%|
|plain|8|6324|6742|+6.6%|
|stream|1|6481|6924|+6.8%|
|stream|2|6370|6962|+9.3%|
|stream|4|6132|6826|+11.3%|
|stream|8|5893|6140|+4.2%|

## 常规流式内存与hook（每轮32请求）

|并发|分配字节之前/之后|hook墙钟μs/请求之前/之后|
|---|---:|---:|
|1|5.33MB / 3.26MB|74.8 / 64.7|
|8|5.35MB / 3.27MB|54.2 / 36.9|

分配字节是累计分配量，不是峰值RSS；并发下hook墙钟/请求是聚合指标，不是p99延迟。

## 扩展分片负载

|响应/分片/并发|吞吐之前/之后(req/s)|分配字节之前/之后(每轮MB)|hook μs/请求之前/之后|
|---|---:|---:|---:|
|response=24KiB/chunks=128/concurrency=1|4108.0 / 4480.0|8.16 / 5.60|236.2 / 216.0|
|response=24KiB/chunks=128/concurrency=8|4440.0 / 5304.0|8.22 / 5.63|189.5 / 148.1|
|response=24KiB/chunks=1024/concurrency=1|855.8 / 860.7|23.24 / 22.33|1161.2 / 1154.5|
|response=24KiB/chunks=1024/concurrency=8|1252.0 / 1279.0|23.37 / 22.44|777.6 / 762.4|
|response=1024KiB/chunks=128/concurrency=1|296.6 / 299.3|272.71 / 125.53|2175.2 / 1928.6|
|response=1024KiB/chunks=128/concurrency=8|284.0 / 283.0|272.79 / 125.56|1096.0 / 619.1|
|response=1024KiB/chunks=1024/concurrency=1|294.4 / 265.4|268.66 / 136.63|3124.4 / 3064.7|
|response=1024KiB/chunks=1024/concurrency=8|275.2 / 272.2|268.78 / 136.73|1723.5 / 1412.9|

## 边界与复现

- 65536个非空分片引用上限；引用数组容量、分片底层容量计费；后台合并先检查全局可用预算，不够则整次跳过而不阻塞。预算是采集缓冲限制，不是进程RSS限制。
- noop sink的Key校验调整为“独立upsert + capture携带Key”总共8次，因为Key现在可能在capture事务内首次存储；不是取消验证。
- 原始输出在`benchmarks/round2/`；mutex-before/after是1MiB/1024分片/并发8、5轮的采样profile，受调度与profile开销影响，不能直接作为时延承诺。

```sh
go test ./internal/plugin -run '^$' -bench 'BenchmarkPlugin(EndToEnd|HookOverhead)$' -benchmem -count=3
go test ./internal/plugin -run '^$' -bench BenchmarkPluginFragmented -benchmem -benchtime=3x -count=3
```

测试：race全套、vet、ABI SQLite smoke及面板smoke。PostgreSQL接口/存储层未改，本地无DSN，仍未实库验证。
