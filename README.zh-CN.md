# DSH 免费网关

**🌐 [English](README.md) | [简体中文](README.zh-CN.md)**

一个 **零依赖 Node.js** 的 OpenAI 兼容故障转移网关，聚合多个免费/低成本 LLM provider，并在它们之间透明地故障转移 —— 支持 **SSE 流断点续传恢复**、按渠道 **冷却**，以及 **缓冲模式**。

为 [DeepSeek Harness (DSH)](https://github.com/fendouai/awesome-deepseek-harness) 移动端 agent 平台打造，但也可作为普通 OpenAI 兼容端点供任意客户端使用。

## 为什么需要它

免费 LLM API 在能用的时候很好用，一旦不能用就很糟：它们会限流、返回配额耗尽错误（状态码还互不一致：402 / 403 / 429），而且最糟的是 —— **会在响应中途切断流**。现有开源网关（LiteLLM、one-api、gpt-load……）能处理路由与重试，但没有一个能续传被中断的 SSE 流。本网关可以。

## 功能特性

- 🔀 **模型组路由** —— 一个逻辑模型 ID 映射到一组有序的 `(channel, model)` 回退项；失败时网关自动沿列表向下尝试。
- 🔁 **SSE 流续传** —— 两种恢复模式：
  - **模式 A（续写）**：要求模型从中断点继续，并把已生成的输出作为上下文传入。
  - **模式 B（重新生成）**：重新发送完整问题（配合缓冲模式使用，此时客户端从未看到部分输出）。
- 🧊 **缓冲模式** —— 在向客户端刷出之前先缓冲完整的上游响应；如果上游在流中途挂掉，客户端什么都看不到，网关会干净地重新生成 —— 完全透明。
- 🧊 **渠道冷却** —— 失败的渠道在 `cooldownMs` 内被排除，之后自动重试。
- 🩹 **可恢复状态码** —— 可配置列表（默认包含 `402, 403, 429, 500, 502, 503, 504`），使配额耗尽的响应触发故障转移而非直接中止。
- 📦 **零依赖** —— 纯 Node.js `http`/`https` + `readline`，无需 npm install、无需数据库、无需 Docker。

## 架构

```
任意 OpenAI 兼容客户端（DSH 引擎、脚本……）
      │  POST http://127.0.0.1:8799/v1/chat/completions  (model=glm-5.3-flashx)
      ▼
┌───────────────────────────────────────┐
│              gateway.mjs              │
│  · 模型组路由（按配置顺序）           │
│  · 渠道健康（冷却 / 失败计数）        │
│  · 请求级重试（429/5xx）              │
│  · SSE 流续传（A 续写 / B 重新生成）  │
│  · 缓冲模式（可选，防中断）           │
└───────────────────────────────────────┘
      │  按模型组挑选健康渠道
      ▼
   channel 1 (modelscope) ──► channel 2 (ark) ──► ... ──► channel N (paid fallback)
```

## 快速开始

```bash
# 1. 获取代码
git clone https://github.com/HaoJieJin/dsh-free-gateway.git
cd dsh-free-gateway

# 2. 配置
cp gateway-config.example.json gateway-config.json
cp gateway-secrets.example.json gateway-secrets.json
#    编辑 gateway-secrets.json —— 填入各渠道所需的 API key
#    （可选）编辑 gateway-config.json —— 渠道、模型组、端口

# 3. 启动
node gateway.mjs
# 或：sh start-gateway.sh   （后台运行，日志写入 gateway.log）

# 4. 验证
curl http://127.0.0.1:8799/health
# => {"ok":true,...}

# 5. 像使用任意 OpenAI 兼容端点一样使用它
curl http://127.0.0.1:8799/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-5.3-flashx","messages":[{"role":"user","content":"Hello!"}]}'
```

## 配置

所有配置都保存在 `gateway-config.json` 中（参见 `gateway-config.example.json`）。

| 键 | 默认值 | 说明 |
|---|---|---|
| `port` | `8799` | HTTP 监听端口 |
| `bufferMode` | `true` | 在向客户端刷出之前缓冲完整的上游响应 |
| `recoverMode` | `"B"` | `"A"` = 从中断点续写，`"B"` = 完整重新生成 |
| `maxRetries` | `3` | 每个请求的最大故障转移尝试次数 |
| `retryIntervalMs` | `1000` | 故障转移尝试之间的延迟 |
| `upstreamTimeoutMs` | `300000` | 上游请求超时 |
| `cooldownMs` | `60000` | 渠道失败后的冷却时间 |
| `recoverableStatusCodes` | `[402,403,429,500,502,503,504]` | 触发故障转移的状态码 |
| `prompts` | — | 模式 A/B 恢复提示词模板（`{{previous_output}}`、`{{question}}`） |
| `channels` | — | `{id, baseUrl, apiKey, note}` 列表 |
| `modelGroups` | — | 逻辑模型 ID → 有序 `[{channel, model}, ...]` 回退项 的映射 |

密钥**不会**存放在配置中：`gateway-config.json` 可以引用明文 `apiKey` 值，但推荐做法是把它们放在 `gateway-secrets.json`（启动时加载；按名称解析密钥）。示例文件使用占位符。

```bash
# 通过环境变量指定备用配置路径
GATEWAY_CONFIG=/path/to/gateway-config.json GATEWAY_SECRETS=/path/to/secrets.json node gateway.mjs
```

## 说明

- 需要 **Node.js 18+** —— 仅使用内置的 `node:http`/`node:https`；已在 Node 26 上测试。
- API key 归你所有；本项目不会回连任何服务器，也不包含遥测。
- 示例配置包含作者手机上使用的渠道列表（Modelscope、Volcengine Ark、SenseNova、阿里云百炼、腾讯 TokenHub、云知声、智谱、小米、DeepSeek）。请删除你没有对应 key 的渠道。

## License

MIT © 2026 HaoJieJin
