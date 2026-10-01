# DSH Free Gateway

A **zero-dependency Node.js** OpenAI-compatible fallback gateway that aggregates multiple free/cheap LLM providers and transparently fails over between them — with **SSE stream-resume recovery**, per-channel **cooldown**, and **buffered mode**.

Built for the [DeepSeek Harness (DSH)](https://github.com/fendouai/awesome-deepseek-harness) mobile agent platform, but works as a plain OpenAI-compatible endpoint for any client.

## Why

Free LLM APIs are great until they aren't: they rate-limit, return quota-exhausted errors with inconsistent status codes (402 / 403 / 429), and — worst of all — **cut the stream mid-response**. Existing open-source gateways (LiteLLM, one-api, gpt-load, ...) handle routing and retries, but none of them resume an interrupted SSE stream. This gateway does.

## Features

- 🔀 **Model-group routing** — one logical model ID maps to an ordered list of `(channel, model)` fallbacks; on failure the gateway walks down the list automatically.
- 🔁 **SSE stream resume** — two recovery modes:
  - **Mode A (continue)**: asks the model to continue from the break point, passing the already-generated output as context.
  - **Mode B (regenerate)**: re-asks the full question (used with buffered mode, where the client never saw partial output).
- 🧊 **Buffered mode** — buffer the full upstream response before flushing to the client; if the upstream dies mid-stream, the client sees nothing and a clean regeneration happens — completely transparent.
- 🧊 **Channel cooldown** — a channel that fails is excluded for `cooldownMs`, then automatically retried.
- 🩹 **Recoverable status codes** — configurable list (default includes `402, 403, 429, 500, 502, 503, 504`) so quota-exhausted responses trigger failover instead of aborting.
- 📦 **Zero dependencies** — pure Node.js `http`/`https` + `readline`, no npm install, no database, no Docker.

## Architecture

```
Any OpenAI-compatible client (DSH engine, scripts, ...)
      │  POST http://127.0.0.1:8799/v1/chat/completions  (model=glm-5.3-flashx)
      ▼
┌─────────────────────────────────────────────┐
│            gateway.mjs                    │
│  · model-group routing (config order)      │
│  · channel health (cooldown/fail counts)   │
│  · request-level retry (429/5xx)          │
│  · SSE stream resume (A continue / B regen)│
│  · buffered mode (optional, fail-proof)    │
└─────────────────────────────────────────────┘
      │  picks healthy channel per model group
      ▼
   channel 1 (modelscope) ──► channel 2 (ark) ──► ... ──► channel N (paid fallback)
```

## Quick Start

```bash
# 1. Get the code
git clone https://github.com/HaoJieJin/dsh-free-gateway.git
cd dsh-free-gateway

# 2. Configure
cp gateway-config.example.json gateway-config.json
cp gateway-secrets.example.json gateway-secrets.json
#    edit gateway-secrets.json — fill in the API keys your channels need
#    (optionally edit gateway-config.json — channels, model groups, port)

# 3. Start
node gateway.mjs
# or: sh start-gateway.sh   (runs in background, logs to gateway.log)

# 4. Verify
curl http://127.0.0.1:8799/health
# => {"ok":true,...}

# 5. Use it like any OpenAI-compatible endpoint
curl http://127.0.0.1:8799/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-5.3-flashx","messages":[{"role":"user","content":"Hello!"}]}'
```

## Configuration

All configuration lives in `gateway-config.json` (see `gateway-config.example.json`).

| Key | Default | Description |
|---|---|---|
| `port` | `8799` | HTTP listen port |
| `bufferMode` | `true` | Buffer full upstream response before flushing to client |
| `recoverMode` | `"B"` | `"A"` = continue from break point, `"B"` = full regenerate |
| `maxRetries` | `3` | Max failover attempts per request |
| `retryIntervalMs` | `1000` | Delay between failover attempts |
| `upstreamTimeoutMs` | `300000` | Upstream request timeout |
| `cooldownMs` | `60000` | Channel cooldown after a failure |
| `recoverableStatusCodes` | `[402,403,429,500,502,503,504]` | Status codes that trigger failover |
| `prompts` | — | Templates for mode A/B recovery prompts (`{{previous_output}}`, `{{question}}`) |
| `channels` | — | List of `{id, baseUrl, apiKey, note}` |
| `modelGroups` | — | Map of logical model ID → ordered `[{channel, model}, ...]` fallbacks |

Secrets are **not** stored in the config: `gateway-config.json` may reference plain `apiKey` values, but the recommended setup keeps them in `gateway-secrets.json` (loaded at startup; the keys are resolved by name). The example files use placeholders.

```bash
# Alternate config paths via env
GATEWAY_CONFIG=/path/to/gateway-config.json GATEWAY_SECRETS=/path/to/secrets.json node gateway.mjs
```

## Notes

- Requires **Node.js 18+** — uses only built-in `node:http`/`node:https`; tested on Node 26.
- API keys are yours; this project never calls home and contains no telemetry.
- The example config includes the channel list used on the author's phone (Modelscope, Volcengine Ark, SenseNova, Aliyun Bailian, Tencent TokenHub, Unisound, Zhipu, Xiaomi, DeepSeek). Remove channels you don't have keys for.

## License

MIT © 2026 HaoJieJin