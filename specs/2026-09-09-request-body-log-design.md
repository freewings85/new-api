# 请求正文日志（Request Body Log）设计

日期：2026-09-09

## 1. 目标

把每一次中转请求的完整输入和输出落到本地文件，按用户分目录、按天和大小切分，供事后按 request_id 查找。

非目标（本期不做）：

- 不截断、不脱敏正文。
- 不做后台查看页面，查找靠文件系统命令。
- 不做跨节点汇总，每个节点只写自己的本地目录。以后若改用共享存储，文件名必须加节点名，否则多节点会写坏同一个文件。
- 除 OpenAI Chat Completions 外的格式，本期只记录输入，输出留空并标记。成功记录只出自 OpenAI Chat 结算路径，音频、实时、任务、Midjourney 等中转本期只会产生 `status=error` 的错误记录。
- 只捕获 `Content-Type: application/json` 的请求正文（缺省 Content-Type 按 JSON 处理）；multipart 上传等其它媒体类型，以及主程序已落到磁盘缓存的超大正文，都只记 `input_skipped` 和 `input_size`，不读入内存。捕获失败记 `input_missing`。这是取舍不是截断：正文要么完整记录，要么整条跳过并说明原因。

## 2. 配置

全部走环境变量，写入 `.env.example` 供参考。默认启用，目录为相对工作目录的 `data/requests`（本地 `go run` 时在仓库根目录下，容器内 `WORKDIR` 为 `/data`，因此实际路径为 `/data/data/requests`，部署时显式设为 `/data/requests` 并挂卷）。

| 变量 | 默认 | 说明 |
|---|---|---|
| `REQUEST_LOG_DIR` | `data/requests` | 根目录。设为 `off` 关闭整个功能 |
| `REQUEST_LOG_MAX_SIZE_MB` | 256 | 单文件超过此大小后轮转 |
| `REQUEST_LOG_RETENTION_DAYS` | 30 | 超过天数的文件由清理任务删除 |
| `REQUEST_LOG_QUEUE_SIZE` | 10000 | 内存队列长度，满则丢弃 |
| `REQUEST_LOG_QUEUE_BYTES_MB` | 256 | 内存队列中正文字节上限，超出则整条丢弃 |

启动时若目录不存在则创建；不可写时打印错误并把功能关闭，不阻止服务启动。`data/` 已在 .gitignore 中。
`REQUEST_LOG_MAX_SIZE_MB`、`REQUEST_LOG_QUEUE_SIZE`、`REQUEST_LOG_QUEUE_BYTES_MB` 留空或填 0 表示使用默认值。目录 0700、文件 0600，正文里可能有敏感数据。

## 3. 文件布局

```
<DIR>/<user_id>/2026-09-09.jsonl          当前正在写（唯一的"活文件"）
<DIR>/<user_id>/2026-09-09.1.jsonl.gz     关闭的分段：超大小或跨天轮转出的第 1 个
<DIR>/<user_id>/2026-09-09.2.jsonl.gz
<DIR>/<user_id>/2026-09-08.3.jsonl.gz     昨天最后一个分段
```

- 日期取记录时间的本地日期（`TZ`）。
- 轮转：写完一条后若累计字节数 ≥ 上限，或记录日期与当前文件不同，关闭当前文件，改名为 `<date>.<n>.jsonl` 后交给压缩器，再打开记录日期对应的活文件。活文件路径永远不交给压缩器：记录到达顺序不保证按时间递增，晚到的前一天记录只是重新打开一个新的活文件，每个关闭的分段都有自己的序号，不会互相覆盖。
- 压缩由单个后台 goroutine 串行执行，待压缩队列最多 1024 个分段；队列满时分段暂留为未压缩的 `.n.jsonl`，由维护任务补压缩。
- 维护任务（启动时一次，之后每天一次，在消费 goroutine 上执行）：删除超过保留期的文件；把过去日期的活文件（空闲关闭或重启遗留）改名为分段并压缩；把未压缩的 `.n.jsonl` 分段补交压缩。当天的活文件和仍被打开的文件不碰。
- 打开活文件时若末尾不是换行（崩溃或写失败留下的半条记录），先补一个换行，保证后续记录仍是合法的 JSONL；写入返回部分成功时先截断回写入前的长度。
- 进程启动时若当天文件已存在（重启场景），以追加方式继续写，初始计数取文件当前大小。
- 轮转序号 `n` 取该用户目录中同日期已存在的最大序号加一，重启后不会覆盖已有的 `.n.jsonl.gz`。

## 4. 记录格式

JSON Lines，一行一个对象，字段顺序固定，一次 `write` 写完整行：

```json
{"time":"2026-09-09T17:04:50+08:00","request_id":"2026090909…","user_id":1,"username":"chenzifei","token_name":"test","model":"deepseek-v4-flash-xiexin","upstream_model":"deepseek-v4-flash","channel_id":1,"stream":false,"status":"ok","prompt_tokens":84,"completion_tokens":35,"use_time_ms":1200,"input":{...},"output":{...}}
```

- `input`：客户端发来的原始请求体，按原样嵌入为 JSON；不是合法 JSON 时作为字符串存入。
- `output`：非流式为上游响应体 JSON；流式为拼接后的全文，存为 `{"text":"…"}`。全文与计费统计使用同一份拼接结果，因此包含模型输出的思考内容以及工具调用的名称和参数（按流中出现的顺序拼接），是完整的流式输出而非仅正文。本期不支持的格式为 `null` 并附 `"output_missing":true`。
- `status`：`ok` 或 `error`；错误时增加 `"error":"<上游错误信息>"`，`output` 为 `null`。
- 时间含时区，便于跨节点对齐。

## 5. 组件

### 5.1 `pkg/bodylog`：滚动写入器（纯逻辑，可单测）

- `Writer`：持有配置、`chan Record` 队列、一个消费 goroutine、`map[int]*appender`。
- `Enqueue(rec)`：非阻塞投递；队列满则丢弃并累加 `dropped` 计数，每分钟最多打一条警告日志。
- 消费 goroutine：逐条取出，序列化成一行，找到或创建该用户的 appender，写入，检查轮转。
- `appender`：一个打开的文件句柄、当前日期、已写字节数、最后写入时间。
- 句柄回收：每分钟扫描，关闭 5 分钟未写的 appender；同时打开数超过 1024 时按最久未用关闭。关闭不触发压缩，下次写入按"当天文件已存在"逻辑续写。
- 压缩：`gzip` 读原文件写 `.gz`，成功后删除原文件；失败保留原文件并记日志。
- 清理：每天一次，遍历所有用户目录，删除文件名日期早于保留期的文件，删除空目录。
- `Close()`：关闭队列，写完剩余记录，关闭所有句柄。服务优雅退出时调用。

原子性来自单消费 goroutine 加整行单次写入；轮转只发生在两条记录之间。

### 5.2 `relay/common/relay_info.go`：新增字段

- `ResponseBody []byte`：非流式响应原始字节。
- `ResponseText string`：流式拼接全文。
- 由各格式的 handler 在返回前填充。本期只在 `relay/channel/openai/relay-openai.go` 的 `OaiStreamHandler`（`responseTextBuilder.String()`）和 `OpenaiHandler`（`responseBody`）两处赋值。

### 5.3 `service/body_log.go`：挂钩

- `RecordRequestBodyLog(c, relayInfo, status, errMsg, usage)`：从 `common.GetRequestBody(c)` 取请求体，从 `relayInfo` 取响应和元数据，组装 `Record` 后 `Enqueue`。
- 调用点：
  - 成功：`service/quota.go` 中 `model.RecordConsumeLog(...)` 之后。
  - 失败：`controller/relay.go` 第 407 行附近，`model.RecordErrorLog(...)` 之后，受 `ERROR_LOG_ENABLED` 同一开关控制。
- 只在 `user_id` 已知（已通过令牌鉴权）时记录；鉴权失败等更早阶段的错误不记录。
- `logs.other` 的 `admin_info` 中增加 `"body_file":"<user_id>/2026-09-09.jsonl"`（仅管理员可见）。文件名只由 user_id 和记录时间推导，不依赖写入器状态，因此可以在写消费日志之前算出；轮转后实际文件可能变为 `.n.jsonl.gz`，查找时按日期通配。

### 5.4 生命周期

- `main.go` 启动阶段按环境变量初始化 `bodylog.Writer`，`REQUEST_LOG_DIR=off` 时为 no-op。
- 优雅退出时在 HTTP 服务器关闭后调用 `Shutdown(ctx)`，与 HTTP 服务器共用同一个 `SHUTDOWN_TIMEOUT_SECONDS` 截止时间：先等队列排空并关闭文件，再等压缩队列清空；超时则记日志放弃，进程照常退出。
- `Enqueue` 与关闭之间用读写锁隔离：已通过关闭检查的投递一定会被排空写盘，不会出现"返回成功却无人消费"。

## 6. 错误处理

| 情况 | 处理 |
|---|---|
| 队列满 | 丢弃该条，计数，限频告警 |
| 打开或写文件失败 | 记日志，丢弃该条，下一条重试打开 |
| 压缩失败 | 保留未压缩文件，记日志 |
| 进程崩溃 | 丢失队列中未写出的记录，已写出的行完整 |
| 目录不可写 | 启动时关闭功能并记错误 |

写路径上任何错误都不影响请求本身的响应和计费。

## 7. 查找方式

```bash
# 已知 user_id 和日期
zgrep -h '"request_id":"2026090909…"' <DIR>/<user_id>/2026-09-09*.jsonl*
```

`logs` 表的 `user_id`、`created_at`、`request_id` 和 `other.body_file` 提供全部定位信息。

## 8. 测试

验收：开发完成后经 web 容器调用 `http://localhost:8080/v1/chat/completions`（模型 `deepseek-v4-flash-xiexin`，流式与非流式各一次），确认 `data/requests/<user_id>/<日期>.jsonl` 中出现对应 request_id 的完整记录，且 `logs.other.body_file` 指向该文件。

`pkg/bodylog` 单元测试，用临时目录，不依赖数据库：

- 一条记录一行，多条并发 `Enqueue` 后文件内容可逐行解析且无交错。
- 超过大小上限后轮转为 `.1.jsonl.gz`，新记录写入新文件；日期变化后旧文件被压缩。
- 队列满时丢弃并计数，不阻塞。
- 空闲 appender 被关闭后再次写入能续写同一文件。
- 清理任务删除超期文件，保留未超期文件。
- `Close()` 后所有已入队记录均落盘。

挂钩层测试：用 `httptest` 构造 gin 上下文和 `RelayInfo`，验证 `Record` 的字段映射和 `input` 原样嵌入、非法 JSON 转字符串。

## 9. 部署

- `docker-compose.local.yml` 和 `deploy/aliyun/docker-compose.yml` 给 server 容器挂卷 `./requests:/data/requests`，环境变量 `REQUEST_LOG_DIR=/data/requests`。
- 容量估算：每请求约 5KB 压缩后（无图片），日 10 万请求约 500MB/天，30 天约 15GB；带图片的请求按 base64 原样计入，需单独评估。
