# 反向代理到后端的公共配置，被 default.conf 里的各个 location include。
proxy_pass         http://$backend_upstream;
proxy_http_version 1.1;
proxy_set_header   Host              $host;
proxy_set_header   X-Real-IP         $remote_addr;
proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
proxy_set_header   X-Forwarded-Proto $scheme;
# WebSocket（/v1/realtime）
proxy_set_header   Upgrade           $http_upgrade;
proxy_set_header   Connection        $connection_upgrade;
# 流式响应：关闭缓冲，逐块透传；超时要不小于后端 STREAMING_TIMEOUT
proxy_buffering    off;
proxy_cache        off;
proxy_read_timeout 900s;
proxy_send_timeout 900s;
