#!/bin/sh
# 停止免费模型容灾网关
# 用法：sh stop-gateway.sh
if pkill -f "[g]ateway.mjs"; then
  echo "✅ 网关已停止"
else
  echo "网关未在运行"
fi
