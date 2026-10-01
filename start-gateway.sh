#!/bin/sh
# 启动免费模型容灾网关（默认端口 8799）
# 用法：sh start-gateway.sh
# 可用环境变量：GATEWAY_CONFIG / GATEWAY_SECRETS / GATEWAY_PORT(未用，改配置)
BASE="$(cd "$(dirname "$0")" && pwd)"
NODE="${NODE:-node}"

if pgrep -f "[g]ateway.mjs" >/dev/null 2>&1; then
  echo "网关已在运行（PID $(pgrep -f '[g]ateway.mjs' | head -1)）"
  exit 0
fi

cd "$BASE" || exit 1
nohup "$NODE" gateway.mjs > gateway.log 2>&1 &
sleep 1
PORT=$(grep -o '"port"[[:space:]]*:[[:space:]]*[0-9]*' "$BASE/gateway-config.json" 2>/dev/null | grep -o '[0-9]*$' || echo 8799)
if curl -s "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
  echo "✅ 网关已启动 http://127.0.0.1:$PORT"
else
  echo "❌ 网关启动失败，看日志：$BASE/gateway.log"
  exit 1
fi
