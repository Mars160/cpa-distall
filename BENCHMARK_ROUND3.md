# capture_id 与 writer_count 最终验证

## 实现

- 插件生成128位随机capture_id，数据库以16字节BLOB/BYTEA作为主键，API为32位十六进制。
- 删除session_id；宿主request_id原样留在摘要中，无唯一约束、无索引。
- writer_count默认1，范围1–64；SQLite按capture_id稳定哈希分库，PG单库多writer。
- SQLite DSN现在是数据目录，不是数据库文件。manifest固定布局/分片数；缺片、旧库或布局不匹配拒绝打开，不迁移、不删除。
- 共享一个有界写队列及全局缓冲预算；队列并非每片各一份。各SQLite分片单连接，多worker通过分片连接实现并发。
- 摘要正文同库同事务，晚到Key扇出各片；部分失败保留后续usage/capture重试资格，不声称主动重试。

## 最终版基准

Apple M3、Go 1.27.3、SQLite WAL。每项三次取中位数；每轮32请求，逐字节验证正文，无丢弃或写入失败。仅测插件hook与后台落库，不含宿主序列化、C ABI和网络模型耗时。

以下是同一最终构建下单writer与分片的比较，不是与最初事件存储版本的比较。源码包含上一轮未提交的内存优化；HEAD `67cc2d6` 不包含那轮改动。

|负载|请求并发|单writer req/s|writer数=并发 req/s|变化|
|---|---:|---:|---:|---:|
|plain|1|6689|7197|+7.6%|
|plain|2|7023|7573|+7.8%|
|plain|4|6833|8296|+21.4%|
|plain|8|6784|8502|+25.3%|
|stream|1|7006|7003|-0.0%|
|stream|2|6989|7588|+8.6%|
|stream|4|6840|7916|+15.7%|
|stream|8|6689|8368|+25.1%|

并发1两列实际上配置相同，数值差异反映采样/运行顺序噪声，不是分片收益。不能据此保证线性扩展或生产容量。默认仍为1，4可作为部署后压测起点。

```sh
go test ./internal/plugin -run '^$' -bench BenchmarkPluginEndToEnd -benchmem -count=3
```

最终原始输出：`benchmarks/round3/plugin-final.txt`。本目录其他早期结果来自中间构建（曾存request_id摘要，最终已恢复原文），仅保留追溯，不代表交付版性能。

## 验证及限制

- 中央重新执行race全套、vet、ABI SQLite smoke、panel smoke；面板73/73通过。
- PG本地无DSN，实库测试跳过；没有PG性能结论。
- 多库列表每片取offset+limit再归并；窗口最多10000条，单库仍下推LIMIT/OFFSET。多分片最坏内存与分片数乘窗口有关，深页仍比首屏重；面板提示缩窄筛选。
- manifest发布需要支持硬链接的文件系统。初始化竞争失败可能留额外空文件，不会自动清理；布局不匹配拒绝而不是自动修改。
- drain预算取消ctx后仍等待writer退出，驱动不响应取消可能延迟关闭；不能在goroutine仍运行时返回并允许宿主卸载动态库。
- 无旧库迁移。改变SQLite writer_count需要新目录或另行离线重分布；PG调整writer_count不改变数据布局。
