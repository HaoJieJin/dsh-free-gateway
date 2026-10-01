#!/usr/bin/env node
/**
 * dsh-free-gateway —— 免费模型容灾网关（零依赖 Node.js）
 *
 * 思路借鉴：
 *  - tdn-001/Ai-Gateway：SSE 流式断点恢复（接续生成 A / 完整重生成 B）、缓冲模式、可恢复错误码重试
 *  - ikun5200/uni-api：渠道冷却（失败后排除冷却一段时间，到期自动恢复）
 *  - zk-2025/model-gateway（原目标）：聚合多个免费 LLM，OpenAI 兼容，无感容灾切换
 *
 * 用法：
 *   node gateway.mjs                 # 启动（读取同目录 gateway-config.json + gateway-secrets.json）
 *   GATEWAY_CONFIG=xxx node gateway.mjs
 * 环境变量：
 *   GATEWAY_CONFIG  配置文件路径（默认 ./gateway-config.json）
 *   GATEWAY_SECRETS 密钥文件路径（默认 ./gateway-secrets.json）
 */
import http from 'node:http';
import https from 'node:https';
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import readline from 'node:readline';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = process.env.GATEWAY_CONFIG || join(__dirname, 'gateway-config.json');
const SECRETS_PATH = process.env.GATEWAY_SECRETS || join(__dirname, 'gateway-secrets.json');

// ---------------- 配置加载 ----------------
let config;
try {
  config = JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
} catch (e) {
  console.error(`[gateway] FATAL: cannot read config ${CONFIG_PATH}: ${e.message}`);
  process.exit(1);
}

const secrets = {};
if (existsSync(SECRETS_PATH)) {
  try {
    Object.assign(secrets, JSON.parse(readFileSync(SECRETS_PATH, 'utf8')));
  } catch (e) {
    console.error(`[gateway] WARN: cannot parse secrets ${SECRETS_PATH}: ${e.message}`);
  }
}

const PORT = config.port || 8765;
const MAX_RETRIES = config.maxRetries ?? 3;
const RETRY_INTERVAL_MS = config.retryIntervalMs ?? 1000;
const UPSTREAM_TIMEOUT_MS = config.upstreamTimeoutMs ?? 300000;
const BUFFER_MODE = config.bufferMode ?? true;      // 默认缓冲模式（失败对客户端无感）
const DEFAULT_RECOVER_MODE = config.recoverMode || 'B';
const RECOVERABLE_CODES = config.recoverableStatusCodes ?? [429, 500, 502, 503, 504];
const COOLDOWN_MS = config.cooldownMs ?? 60000;
const PROMPTS = Object.assign(
  {
    mode_a:
      '之前生成到一半连接中断。请忽略你已经看到的不完整输出，根据原始对话上下文，从断点继续生成，不要重复已输出的开头部分。\n已生成内容：\n{{previous_output}}',
    mode_b:
      '之前的回答因网络中断作废。请重新完整回答用户的原始问题，不要提及中断。\n用户问题：\n{{question}}',
  },
  config.prompts || {},
);

// ---------------- 渠道健康管理（按 渠道|模型 独立冷却，同渠道不同模型互不影响） ----------------
const healthMap = new Map(); // `${channelId}|${model}` -> { cooldownUntil, consecutiveFailures }

function healthKey(ch, model) {
  return `${ch.id}|${model || ''}`;
}

function isHealthy(ch, model) {
  const h = healthMap.get(healthKey(ch, model));
  if (!h) return true;
  if (h.cooldownUntil && Date.now() < h.cooldownUntil) return false;
  return true;
}

function markFailure(ch, model, reason) {
  const key = healthKey(ch, model);
  const h = healthMap.get(key) || { cooldownUntil: 0, consecutiveFailures: 0 };
  h.consecutiveFailures += 1;
  if (COOLDOWN_MS > 0) h.cooldownUntil = Date.now() + COOLDOWN_MS;
  healthMap.set(key, h);
  log(`渠道 ${ch.id} 模型 ${model} 失败(${reason}) 连续#${h.consecutiveFailures} 冷却至 ${new Date(h.cooldownUntil).toISOString()}`);
}

function markSuccess(ch, model) {
  const key = healthKey(ch, model);
  const h = healthMap.get(key);
  if (h && h.consecutiveFailures > 0) {
    log(`渠道 ${ch.id} 模型 ${model} 恢复成功，重置失败计数`);
  }
  healthMap.set(key, { cooldownUntil: 0, consecutiveFailures: 0 });
}

function resolveApiKey(ch) {
  if (!ch.apiKey) return '';
  if (typeof ch.apiKey === 'string' && ch.apiKey.startsWith('env:')) {
    return process.env[ch.apiKey.slice(4)] || '';
  }
  if (typeof ch.apiKey === 'string' && ch.apiKey.startsWith('secret:')) {
    return secrets[ch.apiKey.slice(7)] || '';
  }
  return String(ch.apiKey || '');
}

// ---------------- 工具函数 ----------------
function log(...args) {
  console.log(`[gateway ${new Date().toISOString()}]`, ...args);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function readAll(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (c) => chunks.push(c));
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    stream.on('error', reject);
  });
}

function sendRaw(res, status, body, contentType = 'application/json') {
  res.statusCode = status;
  res.setHeader('Content-Type', contentType);
  res.end(body);
}

function sendJson(res, status, obj) {
  sendRaw(res, status, JSON.stringify(obj));
}

function sseHeaders() {
  return {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  };
}

function writeSSELine(res, line) {
  res.write(line + '\n\n');
}

function flushSSELines(res, lines) {
  for (const line of lines) {
    writeSSELine(res, line);
  }
  writeSSELine(res, 'data: [DONE]');
  res.end();
}

function writeRecoveryTerminate(res, errMsg) {
  const err = JSON.stringify({
    error: { message: errMsg || 'recovery failed', type: 'server_error' },
  });
  writeSSELine(res, 'data: ' + err);
  writeSSELine(res, 'data: [DONE]');
  res.end();
}

function extractUserQuestion(messages) {
  if (!Array.isArray(messages)) return '';
  for (const m of messages) {
    if (m && m.role === 'user') {
      if (typeof m.content === 'string') return m.content;
      if (Array.isArray(m.content)) {
        return m.content
          .map((p) => (p && typeof p.text === 'string' ? p.text : ''))
          .join(' ')
          .trim();
      }
    }
  }
  return '';
}

// ---------------- 上游请求 ----------------
function sendUpstream(ch, bodyObj) {
  const base = (ch.baseUrl || '').replace(/\/+$/, '');
  const url = new URL(base + '/chat/completions');
  const mod = url.protocol === 'https:' ? https : http;
  const payload = JSON.stringify(bodyObj);
  const apiKey = resolveApiKey(ch);

  return new Promise((resolve, reject) => {
    const req = mod.request(
      url,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(apiKey ? { Authorization: `Bearer ${apiKey}` } : {}),
          Accept: 'text/event-stream',
          'User-Agent': 'dsh-free-gateway/1.0',
        },
        timeout: UPSTREAM_TIMEOUT_MS,
      },
      (res) => resolve(res),
    );
    req.on('timeout', () => req.destroy(new Error(`upstream timeout after ${UPSTREAM_TIMEOUT_MS}ms`)));
    req.on('error', (err) => reject(err));
    req.write(payload);
    req.end();
  });
}

/**
 * 读取上游 SSE 流，逐行回调（line 是原始 "data: xxx" 行）。
 * 返回累计的 content 文本。
 * 若流未以 [DONE] 正常结束（连接中断/EOF 无 [DONE]），抛出 Error。
 */
async function readSSE(stream, onLine) {
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let fullContent = '';
  let sawDone = false;
  for await (const line of rl) {
    if (!line.startsWith('data: ')) continue;
    const data = line.slice(6);
    if (data === '[DONE]') {
      sawDone = true;
      if (onLine) onLine(line);
      break;
    }
    try {
      const evt = JSON.parse(data);
      const delta = evt?.choices?.[0]?.delta?.content;
      if (typeof delta === 'string') fullContent += delta;
    } catch {
      // 非 JSON 的 data 行（如注释），忽略
    }
    if (onLine) onLine(line);
  }
  if (!sawDone) {
    throw new Error('SSE stream ended without [DONE] (upstream connection dropped)');
  }
  return fullContent;
}

// ---------------- 模型组解析 ----------------
function buildAttempts(model) {
  const groups = config.modelGroups || {};
  if (groups[model]) {
    return groups[model]
      .map((entry) => ({
        channelId: entry.channel,
        model: entry.model || model,
      }))
      .filter((a) => a.channelId && (config.channels || []).some((c) => c.id === a.channelId));
  }
  // 未配置模型组的模型：不支持（避免把付费/未纳入的模型意外路由到免费渠道）
  return [];
}

// ---------------- 恢复请求 ----------------
function buildRecoveryPrompt(mode, session) {
  const tmpl = mode === 'A' ? PROMPTS.mode_a : PROMPTS.mode_b;
  return tmpl
    .replaceAll('{{question}}', session.userQuestion || '')
    .replaceAll('{{previous_output}}', session.previousOutput || '')
    .replaceAll('{{error}}', session.lastError || '')
    .replaceAll('{{model}}', session.model || '');
}

function buildRecoveryMessages(mode, session) {
  const prompt = buildRecoveryPrompt(mode, session);
  if (mode === 'A') {
    return [...(session.originalMessages || []), { role: 'system', content: prompt }];
  }
  return [{ role: 'system', content: prompt }];
}

/**
 * 断流恢复：从 startIdx 开始依次尝试后续渠道，读取完整恢复流（缓冲模式）。
 * 成功 → 把缓存的 SSE 行全部写给客户端，返回 true。
 * 全部失败 → 返回 false。
 */
async function recoverWithRetry(res, body, session, attempts, startIdx, mode, depth) {
  if (depth >= MAX_RETRIES) {
    log(`恢复达到最大重试次数 ${MAX_RETRIES}，放弃`);
    return false;
  }
  for (let i = startIdx; i < attempts.length; i++) {
    const a = attempts[i];
    const ch = (config.channels || []).find((c) => c.id === a.channelId);
    if (!ch) continue;
    if (!isHealthy(ch, a.model)) continue;

    const recoveryBody = {
      ...body,
      model: a.model,
      stream: true,
      messages: buildRecoveryMessages(mode, session),
    };
    let up;
    try {
      up = await sendUpstream(ch, recoveryBody);
    } catch (e) {
      markFailure(ch, a.model, `恢复请求连接失败: ${e.message}`);
      await sleep(RETRY_INTERVAL_MS);
      continue;
    }
    if (RECOVERABLE_CODES.includes(up.statusCode)) {
      markFailure(ch, a.model, `恢复请求 HTTP ${up.statusCode}`);
      up.resume();
      await sleep(RETRY_INTERVAL_MS);
      continue;
    }
    if (up.statusCode !== 200) {
      await readAll(up); // 丢弃错误 body
      await sleep(RETRY_INTERVAL_MS);
      continue;
    }

    const lines = [];
    let broken = false;
    try {
      session.previousOutput = await readSSE(up, (line) => lines.push(line));
    } catch (e) {
      broken = true;
      session.lastError = e.message;
      markFailure(ch, a.model, `恢复流中断: ${e.message}`);
    }
    if (!broken) {
      markSuccess(ch, a.model);
      log(`恢复成功：${ch.id} (${a.model}) 模式=${mode} 深度=${depth}`);
      flushSSELines(res, lines);
      return true;
    }
    // 恢复流也中断 → 继续下一个渠道，深度 +1
    await sleep(RETRY_INTERVAL_MS);
    return recoverWithRetry(res, body, session, attempts, i + 1, mode, depth + 1);
  }
  return false;
}

// ---------------- 主处理逻辑 ----------------
async function handleChatCompletions(req, res) {
  let raw;
  try {
    raw = await readRequestBody(req);
  } catch (e) {
    return sendJson(res, 400, { error: { message: `failed to read body: ${e.message}` } });
  }

  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    return sendJson(res, 400, { error: { message: 'invalid JSON body' } });
  }

  const model = typeof body.model === 'string' ? body.model : '';
  const stream = !!body.stream;

  if (!model) {
    return sendJson(res, 400, { error: { message: 'missing model' } });
  }

  const attempts = buildAttempts(model);
  if (attempts.length === 0) {
    log(`未知模型 ${model}，网关未配置该模型组`);
    return sendJson(res, 502, {
      error: { message: `model "${model}" is not configured in gateway modelGroups` },
    });
  }

  const session = {
    model,
    originalMessages: Array.isArray(body.messages) ? body.messages : [],
    userQuestion: extractUserQuestion(body.messages),
    previousOutput: '',
    lastError: '',
  };

  const statusChain = [];

  // 第一轮：按配置顺序尝试
  for (let i = 0; i < attempts.length; i++) {
    const a = attempts[i];
    const ch = (config.channels || []).find((c) => c.id === a.channelId);
    if (!ch) {
      statusChain.push(`${a.channelId}:no-such-channel`);
      continue;
    }
    if (!isHealthy(ch, a.model)) {
      statusChain.push(`${a.channelId}/${a.model}:cooldown`);
      continue;
    }

    const upstreamBody = { ...body, model: a.model };
    let up;
    try {
      up = await sendUpstream(ch, upstreamBody);
    } catch (e) {
      markFailure(ch, a.model, `连接失败: ${e.message}`);
      statusChain.push(`${a.channelId}/${a.model}:${e.message}`);
      await sleep(RETRY_INTERVAL_MS);
      continue;
    }

    // 非流式
    if (!stream) {
      if (RECOVERABLE_CODES.includes(up.statusCode)) {
        const errText = await readAll(up);
        markFailure(ch, a.model, `HTTP ${up.statusCode}`);
        statusChain.push(`${a.channelId}/${a.model}:HTTP${up.statusCode}`);
        await sleep(RETRY_INTERVAL_MS);
        continue;
      }
      const text = await readAll(up);
      if (up.statusCode >= 400) {
        log(`模型 ${model} 渠道 ${ch.id} 返回不可恢复错误 HTTP ${up.statusCode}`);
        return sendRaw(res, up.statusCode, text);
      }
      markSuccess(ch, a.model);
      log(`OK 非流式 ${model} -> ${ch.id} (${a.model}) 状态链 [${statusChain.join(', ')}]`);
      return sendRaw(res, 200, text);
    }

    // 流式：上游非 200
    if (up.statusCode !== 200) {
      const errText = await readAll(up);
      if (RECOVERABLE_CODES.includes(up.statusCode)) {
        markFailure(ch, a.model, `HTTP ${up.statusCode}`);
        statusChain.push(`${a.channelId}/${a.model}:HTTP${up.statusCode}`);
        await sleep(RETRY_INTERVAL_MS);
        continue;
      }
      log(`模型 ${model} 渠道 ${ch.id} 流式返回错误 HTTP ${up.statusCode}`);
      return sendRaw(res, up.statusCode, errText);
    }

    // 流式 200：开始 SSE
    res.writeHead(200, sseHeaders());
    if (BUFFER_MODE) {
      // —— 缓冲模式：先缓存完整流，成功后再一次性输出，失败对客户端无感 ——
      const lines = [];
      let broken = false;
      try {
        session.previousOutput = await readSSE(up, (line) => lines.push(line));
      } catch (e) {
        broken = true;
        session.lastError = e.message;
        markFailure(ch, a.model, `SSE 中断: ${e.message}`);
      }
      if (!broken) {
        markSuccess(ch, a.model);
        log(`OK 流式(缓冲) ${model} -> ${ch.id} (${a.model}) 状态链 [${statusChain.join(', ')}]`);
        flushSSELines(res, lines);
        return;
      }
      // 断流 → 恢复（缓冲模式强制模式 B：完整重生成）
      log(`模型 ${model} 渠道 ${ch.id} 流中断，进入恢复（缓冲模式→B）`);
      const recovered = await recoverWithRetry(res, body, session, attempts, i + 1, 'B', 0);
      if (!recovered) {
        writeRecoveryTerminate(res, `stream broken and recovery failed: ${session.lastError}`);
      }
      return;
    }

    // —— 非缓冲模式：流式直转，断流时尝试接续生成（模式 A）——
    const clientStream = res;
    let firstOutputSent = false;
    let broken = false;
    try {
      session.previousOutput = await readSSE(up, (line) => {
        firstOutputSent = true;
        writeSSELine(clientStream, line);
      });
    } catch (e) {
      broken = true;
      session.lastError = e.message;
      markFailure(ch, a.model, `SSE 中断: ${e.message}`);
    }
    if (!broken) {
      markSuccess(ch, a.model);
      log(`OK 流式(直转) ${model} -> ${ch.id} (${a.model})`);
      writeSSELine(clientStream, 'data: [DONE]');
      clientStream.end();
      return;
    }
    // 断流恢复：已输出内容客户端已见 → 模式 A 接续；未输出任何内容 → 直接换渠道重试
    log(`模型 ${model} 渠道 ${ch.id} 流中断（已输出=${firstOutputSent}），进入恢复（非缓冲→${firstOutputSent ? 'A' : '重试'}）`);
    if (!firstOutputSent) {
      // 尚未输出：直接重试下一个渠道（首 token 前 failover）
      const recovered = await recoverWithRetry(res, body, session, attempts, i + 1, 'B', 0);
      if (!recovered) {
        writeRecoveryTerminate(res, `stream broken before first token and recovery failed: ${session.lastError}`);
      }
      return;
    }
    const recovered = await recoverWithRetry(res, body, session, attempts, i + 1, DEFAULT_RECOVER_MODE || 'A', 0);
    if (!recovered) {
      writeRecoveryTerminate(res, `stream broken and recovery failed: ${session.lastError}`);
    }
    return;
  }

  // 所有渠道都失败
  log(`模型 ${model} 所有渠道失败 状态链 [${statusChain.join(', ')}]`);
  if (stream && res.headersSent) {
    writeRecoveryTerminate(res, 'all upstream channels failed');
  } else {
    sendJson(res, 502, {
      error: {
        message: `all upstream channels failed for model ${model}`,
        status_chain: statusChain,
      },
    });
  }
}

// ---------------- HTTP 服务 ----------------
function channelHealthSummary(ch) {
  // 按 渠道|模型 独立冷却：渠道「健康」= 没有任何模型处于冷却中
  const prefix = ch.id + '|';
  const entries = [...healthMap.entries()].filter(([k]) => k.startsWith(prefix));
  const cooling = entries.filter(([, h]) => h.cooldownUntil && Date.now() < h.cooldownUntil);
  const maxFail = entries.reduce((m, [, h]) => Math.max(m, h.consecutiveFailures || 0), 0);
  return {
    id: ch.id,
    healthy: cooling.length === 0,
    hasKey: !!resolveApiKey(ch),
    consecutiveFailures: maxFail,
    coolingModels: cooling.map(([k]) => k.slice(prefix.length)),
    modelStates: Object.fromEntries(entries.map(([k, h]) => [k.slice(prefix.length), { ...h }])),
  };
}

const server = http.createServer((req, res) => {
  const url = req.url || '/';
  if (req.method === 'GET' && url === '/health') {
    const channelStatus = (config.channels || []).map((c) => channelHealthSummary(c));
    return sendJson(res, 200, {
      status: 'ok',
      port: PORT,
      bufferMode: BUFFER_MODE,
      modelGroups: Object.keys(config.modelGroups || {}),
      channels: channelStatus,
    });
  }
  if (req.method === 'POST' && (url === '/v1/chat/completions' || url === '/chat/completions')) {
    return handleChatCompletions(req, res).catch((e) => {
      log(`处理请求异常: ${e.stack || e.message}`);
      if (res.headersSent) {
        try { res.end(); } catch { /* ignore */ }
      } else {
        sendJson(res, 500, { error: { message: `internal error: ${e.message}` } });
      }
    });
  }
  sendJson(res, 404, { error: { message: 'not found' } });
});

server.listen(PORT, '127.0.0.1', () => {
  log(`免费模型容灾网关已启动 http://127.0.0.1:${PORT}`);
  log(`模型组: ${Object.keys(config.modelGroups || {}).join(', ')}`);
  for (const ch of config.channels || []) {
    log(`  渠道 ${ch.id} ${ch.baseUrl} key=${resolveApiKey(ch) ? '已配置' : '⚠ 未配置'}`);
  }
});