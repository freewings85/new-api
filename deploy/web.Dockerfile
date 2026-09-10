# new-api 前端镜像：用 npm 编译 web/ 下的页面，用 nginx 托管静态文件并把后端路径反向代理到 server 容器。
# 构建上下文必须是仓库根目录：docker build -f deploy/web.Dockerfile -t new-api-web:<tag> .

# node 24 自带 npm 11；npm 10 在遇到 package.json 的 overrides 时会报 "edgesOut" 错误
FROM node:24-alpine AS builder
# 国内网络默认走 npmmirror；海外构建可传 --build-arg NPM_REGISTRY=https://registry.npmjs.org/
ARG NPM_REGISTRY=https://registry.npmmirror.com/
# 页面里显示的版本号，由构建脚本传入
ARG VERSION=dev

WORKDIR /build/web
COPY web/package.json web/package-lock.json* ./
# 有 package-lock.json 用 npm ci 精确复现；没有则 npm install。下载缓存跨构建复用。
RUN --mount=type=cache,target=/root/.npm \
    npm config set registry "${NPM_REGISTRY}" && \
    if [ -f package-lock.json ]; then npm ci; else npm install; fi
COPY web ./
RUN DISABLE_ESLINT_PLUGIN='true' VITE_REACT_APP_VERSION=${VERSION} npm run build

FROM nginx:1.27-alpine
COPY --from=builder /build/web/dist /usr/share/nginx/html
# 官方 nginx 镜像启动时会用 envsubst 渲染 templates/*.template 到 conf.d/，
# 所以后端地址可以用环境变量 SERVER_UPSTREAM 指定，默认 server:3000
COPY deploy/web.nginx.conf.template /etc/nginx/templates/default.conf.template
COPY deploy/web.nginx.proxy.inc      /etc/nginx/templates/proxy.inc.template
ENV SERVER_UPSTREAM=server:3000
EXPOSE 80
