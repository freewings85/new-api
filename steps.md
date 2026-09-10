# new-api 本地开发与部署记录

## 1. 开发环境（WSL Ubuntu，VS Code 用 "Connect to WSL using Distro" 选 Ubuntu）

| 组件 | 位置 / 版本 |
|---|---|
| Go | /usr/local/go，1.26.1，`GOPROXY=https://goproxy.cn,direct` |
| gopls / dlv | ~/go/bin，由 VS Code Go 扩展安装 |
| Node / npm | WSL 内 Node 24 / npm 11（前端只用 npm，不用 bun） |
| Docker | WSL 内 docker-ce，镜像加速见 /etc/docker/daemon.json |
| VS Code 扩展 | golang.go、oxc.oxc-vscode、bradlc.vscode-tailwindcss、ms-azuretools.vscode-docker |

本地基础设施复用 langfuse 容器（网络 `langfuse_default`）：

| 服务 | 宿主机地址 | 凭据 |
|---|---|---|
| PostgreSQL | 127.0.0.1:5433 | postgres / postgres，库 `new_api` |
| Redis | 127.0.0.1:6380 | 密码 myredissecret，db 5 |

`.vscode/launch.json` 的第一个配置连接以上两者，并设 `TRUSTED_PROXIES=172.16.0.0/12`、`NO_PROXY=*`。

## 2. 镜像构建（前后端分离）

| 文件 | 作用 |
|---|---|
| deploy/server.Dockerfile | 只编译 Go 后端，web/dist 用占位页；内置 goproxy.cn 与阿里云 apt 源 |
| deploy/web.Dockerfile | node:24-alpine + npm 编译前端，nginx 托管静态文件并反代后端 |
| deploy/web.nginx.conf.template | 路由：/api /v1 /v1beta /mj /pg /dashboard/billing /*/mj 转后端，其余返回 index.html；/static 长缓存 |
| deploy/web.nginx.proxy.inc | 反代公共配置：流式不缓冲、WebSocket、900s 超时、X-Forwarded-For |
| deploy/resolve-tag.sh | tag 规则：VERSION 文件 > 参数 > 提示输入；两者都有且不一致则报错 |
| deploy/build-server-image.sh [tag] | 产出 `new-api-server:<tag>` |
| deploy/build-web-image.sh [tag] | 产出 `new-api-web:<tag>` |

```bash
deploy/build-server-image.sh dev
deploy/build-web-image.sh dev
```

正式发版：`echo v1.0.0-rc.36-custom.1 > VERSION` 后不带参数运行；推 ACR 用 `docker tag` + `docker push`。

## 3. 本地运行（docker-compose.local.yml，web 对外 8080）

```bash
deploy/local-up.sh dev                              # 全容器：web + server
docker compose -f docker-compose.local.yml down     # 停止

# 调试模式：后端在 VS Code 里 F5 启动（3000），web 容器指向宿主机
docker compose -f docker-compose.local.yml stop server
IMAGE_TAG=dev SERVER_UPSTREAM=host.docker.internal:3000 \
  docker compose -f docker-compose.local.yml up -d --no-deps web
```

浏览器统一用 http://localhost:8080（Turnstile 站点填的是 `localhost`，不要用 127.0.0.1）。

## 4. Git 工作流

| 远程 / 分支 | 说明 |
|---|---|
| `official` | https://github.com/QuantumNous/new-api.git，只读 |
| `origin` | git@github.com:freewings85/new-api.git |
| `main` | 与 official/main 完全一致，只做快进 |
| `custom` | 自己的改动（deploy/、docker-compose.local.yml、.gitattributes） |

```bash
git fetch official
git checkout main && git merge --ff-only official/main && git push origin main
git checkout custom && git merge main && git push origin custom
git diff main..custom --stat          # 查看自己的改动
```

## 5. 管理员账号

- 首次启动走 `/setup` 安装向导创建管理员，本地库里是 `chenzifei`（role 100）。
- 密码是 bcrypt 单向哈希（`common.Password2Hash`），不可解密；忘记时用 bcrypt 生成新哈希写回 `users.password`。
- 另一种方式：`make reset-setup` 删除 setups 记录和 root 用户后重跑向导。

## 6. Turnstile（Cloudflare 人机验证）

- 申请：https://dash.cloudflare.com → Turnstile → 添加站点，Hostname 填线上域名和 `localhost`，模式 Managed。
- 配置：系统设置 → 身份验证 → 机器人保护，填站点密钥和密钥，打开开关。
- 存在 `options` 表；刷新页面后两个密钥框为空是正常的，后端不回传以 Token/Secret/Key 结尾的选项。
- 只作用于 5 个接口：登录、注册、发验证码、重置密码、签到。中间件 `middleware/turnstile-check.go`。
- 从本机直连实测 api.js 1~1.8s、siteverify 0.5~1.5s；走代理会慢到 3~6s。

## 7. 注册入口

- 登录页没有注册链接是因为安装向导勾了 **自用模式**（`SelfUseModeEnabled=true`）。
- 关闭位置：系统设置 → 运维 → 系统行为 → 自用模式。
- 自用模式还会允许未配价格的模型被调用，对外服务前必须关闭。

## 8. 涉及的数据表与缓存

| 表 / key | 用途 | 今天涉及的字段或值 |
|---|---|---|
| `users` | 用户 | id、username、role（100=超级管理员）、status、password（bcrypt） |
| `options` | 系统选项，后台"系统设置"读写，多节点自动同步 | TurnstileCheckEnabled、TurnstileSiteKey、TurnstileSecretKey、SelfUseModeEnabled、RegisterEnabled |
| `setups` | 安装向导完成记录 | initialized_at |
| `logs` | 每次请求一行的使用日志，唯一持续增长的表 | ip 字段来自可信代理链解析出的客户端 IP |
| Redis `rateLimit:v2:ip:<GA|GW|CT>:<ip>` | 按 IP 限流计数 | GA 全局 API、GW 全局网页、CT 关键操作；本地显示 172.19.0.1 是 Docker 网关 |

配置分两层：环境变量（进程级，`.env` 文件由 `main.go` 自动加载）和 `options` 表（业务开关，后台页面修改）。

## 9. 生产部署（deploy/aliyun/）

- 每台 ECS：web + server 两个容器，ALB 指向 80 端口，健康检查 `/api/status`。
- `.env` 三台一致，只有 `NODE_NAME` 不同；`TRUSTED_PROXIES` 要同时包含 Docker 网段和 ALB 所在 VPC 网段，否则所有用户会被算成同一个 IP 限流。
- 升级逐台进行：ALB 摘流量 → 改镜像 tag → `docker compose pull && up -d` → 健康后挂回。

## 10. 接入上游模型（以 DeepSeek 官方为例）

顺序：渠道 → 测试 → 定价 → 令牌 → 调用 → 看日志。模型页和供应商页只是展示用的元数据，可选。

| 步骤 | 位置 | 要点 |
|---|---|---|
| 建渠道 | 管理员 → 渠道 → 添加 | 类型 DeepSeek，地址留空用默认 `https://api.deepseek.com`，填 API Key，分组 `default`；"获取模型"可拉取上游当前模型名 |
| 模型映射（可选） | 渠道编辑页 | **原始模型 = 对外给用户的名字，替换模型 = 上游官方名**。渠道的模型列表要填对外名字，否则路由不到该渠道 |
| 测试 | 渠道列表 → 测试 | 真实请求上游，用模型列表第一个名字，会经过映射 |
| 定价 | 系统设置 → 计费与支付 → 模型定价 | 按**对外名字**配价（计费用用户请求的名字）；未配价在自用模式下扣费为 0，关闭自用模式后直接拒绝 |
| 货币 | 计费与支付 → 货币与展示 | 内部永远按美元记账；额度展示类型改 CNY 并填汇率后，定价页才出现人民币选项，录入时按汇率换成美元存储 |
| 令牌 | 管理员 → 令牌 → 新建 | 得到 `sk-` 开头的 key |
| 调用 | `POST /v1/chat/completions` | `Authorization: Bearer sk-xxx`，model 填对外名字 |
| 核对 | 使用日志 | 看模型名、token 数、扣费 |

涉及的表：`channels`（渠道，含 models、model_mapping、group、key）、`abilities`（渠道×模型×分组的路由索引，由渠道保存时生成）、`tokens`（令牌）、`logs`（使用日志）；价格存 `options` 表。

## 11. 请求正文日志

- 每次中转请求的完整输入输出写到 `REQUEST_LOG_DIR`（默认 `data/requests`，`off` 关闭）下 `<user_id>/<日期>.jsonl`；超 256MB 或跨天都轮转为 `<日期>.n.jsonl.gz`（单个压缩 goroutine 串行处理），保留 30 天，启动时和每天各做一次维护：删过期、补压缩遗留的未压缩分段。
- 查找：`logs.other.admin_info.body_file` 给出文件，`zgrep '"request_id":"…"' data/requests/<user_id>/<日期>*.jsonl*`。
- 本期只有 OpenAI Chat Completions 记录输出，其它格式 `output_missing=true`；流式的 `output.text` 是与计费共用的拼接全文，包含思考内容和工具调用的名称与参数。
- 本期记录范围：只捕获 `Content-Type: application/json` 的请求正文（缺省 Content-Type 按 JSON 处理），multipart 上传等其它类型只记 `input_skipped`（媒体类型）和 `input_size`；被主程序落到磁盘缓存的超大正文同样跳过（`input_skipped="disk-cached body"`），不再读回内存；成功记录只出自 OpenAI Chat 结算路径，音频、实时、任务、Midjourney 等中转目前只会产生 `status=error` 的错误记录。正文本身不截断、不脱敏。
- 错误请求（`ERROR_LOG_ENABLED=true` 时）记为 `status=error`，带上游错误信息，无 output。
- 容器里目录是 `/data/requests`，compose 已挂卷；本地 `go run` 在仓库根目录 `data/requests`。
- 退出时与 HTTP 服务器共用 `SHUTDOWN_TIMEOUT_SECONDS` 截止时间，超时放弃剩余压缩。
- 外部审查（Codex）的 8 项发现已用回归测试复现并修复，测试在 `pkg/bodylog/review_regressions_test.go`。
- 验收记录（2026-09-09，真实 DeepSeek 上游）：非流式、流式、上游 400 各一条，request_id 与响应头一致，`body_file` 指向正确。
- 设计：`specs/2026-09-09-request-body-log-design.md`；实施计划：`specs/2026-09-09-request-body-log-plan.md`。
