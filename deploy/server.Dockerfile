# new-api 后端镜像：只包含 Go 服务，不包含前端页面。
# 构建上下文必须是仓库根目录：docker build -f deploy/server.Dockerfile -t <repo>:<tag> .

FROM golang:1.26.1-alpine AS builder
ENV GO111MODULE=on CGO_ENABLED=0 GOWORK=off GOOS=linux GOARCH=amd64
ENV GOEXPERIMENT=greenteagc
# 国内网络默认走 goproxy.cn；海外构建可传 --build-arg GOPROXY=https://proxy.golang.org,direct
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
# 程序内显示的版本号，由构建脚本传入
ARG VERSION=dev

WORKDIR /build
ADD go.mod go.sum ./
ADD relaykit/go.mod ./relaykit/go.mod
RUN go mod download

COPY . .
# main.go 通过 go:embed 嵌入 web/dist，后端镜像不带前端，放一个占位页让编译通过。
# 生产环境所有页面请求都由前端容器的 nginx 处理，不会到达这里。
RUN mkdir -p web/dist && \
    echo '<!doctype html><html><head><title>new-api server</title></head><body>new-api server is running. The web UI is served by the web container.</body></html>' > web/dist/index.html
RUN go build -ldflags "-s -w -X 'github.com/QuantumNous/new-api/common.Version=${VERSION}'" -o new-api

FROM debian:bookworm-slim
# 国内网络默认走阿里云 apt 源；海外构建可传 --build-arg APT_MIRROR=deb.debian.org
ARG APT_MIRROR=mirrors.aliyun.com
RUN sed -i "s/deb.debian.org/${APT_MIRROR}/g" /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata wget \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates
COPY --from=builder /build/new-api /
COPY LICENSE NOTICE THIRD-PARTY-LICENSES.md /licenses/
EXPOSE 3000
WORKDIR /data
ENTRYPOINT ["/new-api"]
