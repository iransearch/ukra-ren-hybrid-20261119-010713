// HTTP/1.1 XHTTP and raw WebSocket relay. Only Node standard-library streams.
import http from 'node:http';
import https from 'node:https';
import tls from 'node:tls';
import dgram from 'node:dgram';
import fs from 'node:fs';
import os from 'node:os';
import {createHash} from 'node:crypto';

const cfg = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const clustered = process.env.REN_NODE_CLUSTER === '1' && typeof process.send === 'function';
let metricsSending = false, nextAdmission = 0;
const pendingAdmissions = new Map();
const ca = [...tls.rootCertificates, fs.readFileSync(cfg.ca_file, 'utf8')];
// All routes use this immutable trust policy. Share its parsed store per worker,
// while each connection still verifies the configured SNI and certificate chain.
const secureContext = tls.createSecureContext({ca});
const telemetry = dgram.createSocket('udp4');
telemetry.on('error', () => {});
telemetry.unref();
const started = performance.now();
const counters = new Map();
const targets = new Map();
const websocketSockets = new Set();
let serverClosed = false, shutdownFinished = false, shutdownTimer;
let active = 0, peak = 0, unsupportedUpgrades = 0, stopping = false;
let snapshotSequence = 0;
const allowed = new Set(['GET', 'HEAD', 'POST']);
const hopHeaders = new Set(['connection', 'keep-alive', 'proxy-connection',
  'proxy-authenticate', 'proxy-authorization', 'te', 'trailer', 'transfer-encoding', 'upgrade']);

function sendEvent(prefix, body) {
  const data = Buffer.from(prefix + ' ' + JSON.stringify(body));
  if (data.length <= 65507) telemetry.send(data, cfg.metrics_port, '127.0.0.1', () => {});
}

for (const route of cfg.routes) {
  const row = {domain: route.domain, keepalive: true, active: 0, peak: 0,
    accepted_total: 0, completed_total: 0, bytes_in_total: 0, bytes_out_total: 0,
    connect_errors_total: 0, response_errors_total: 0, client_aborts_total: 0,
    origin_5xx_total: 0, frontend_5xx_total: 0, backend_connect_total: 0,
    backend_reuse_total: 0, websocket_active: 0, websocket_accepted_total: 0};
  const agent = new https.Agent({keepAlive: true, maxFreeSockets: 64, timeout: 15000});
  const create = agent.createConnection;
  agent.createConnection = function(options, callback) {
    row.backend_connect_total++;
    return create.call(this, options, callback);
  };
  counters.set(route.domain, row);
  targets.set(route.domain, {...route, row, agent});
}

const renderHostname = String(process.env.RENDER_EXTERNAL_HOSTNAME || '').trim().toLowerCase().replace(/\.$/, '');
const renderTarget = renderHostname.endsWith('.onrender.com') && targets.size === 1
  ? targets.values().next().value : undefined;

function resolveTarget(host) {
  if (cfg.legacy) return targets.values().next().value;
  // Only this service's official hostname can alias an unambiguous destination.
  // Reuse the configured target, including its verified origin SNI and agent.
  return targets.get(host) || (host === renderHostname ? renderTarget : undefined);
}

function metrics() {
  const origins = [...counters.values()];
  const sample = {engine: 'node', build: 'ren-node-4', mode: 'http1-experimental',
    instance: process.env.RENDER_INSTANCE_ID || os.hostname(), generation: 0,
    current_workers: 1, draining_workers: 0, uptime_s: (performance.now() - started) / 1000,
    active, peak, websocket_supported: true, http_status_metrics_available: true,
    unsupported_upgrades_total: unsupportedUpgrades,
    bytes_scope: 'HTTP_payload_body_and_WebSocket_frames', origins};
  for (const key of ['accepted_total', 'completed_total', 'bytes_in_total', 'bytes_out_total',
      'connect_errors_total', 'response_errors_total', 'client_aborts_total',
      'origin_5xx_total', 'frontend_5xx_total', 'backend_connect_total', 'backend_reuse_total',
      'websocket_active', 'websocket_accepted_total']) {
    sample[key] = origins.reduce((sum, row) => sum + row[key], 0);
  }
  if (clustered) {
    if (!metricsSending && process.connected) {
      metricsSending = true;
      process.send({kind: 'ren_metrics', sample}, () => { metricsSending = false; });
    }
    return;
  }
  // Bound datagram size even with 100 long domain names and large cumulative counters.
  const parts = Math.ceil(origins.length / 50);
  const sequence = ++snapshotSequence;
  for (let part = 0; part < parts; part++) {
    sendEvent('REN_NODE_METRICS', {...sample, origins: origins.slice(part * 50, (part + 1) * 50),
      snapshot_sequence: sequence, snapshot_part: part, snapshot_parts: parts});
  }
}

function cleanHeaders(headers) {
  const blocked = new Set(hopHeaders);
  for (const key of String(headers.connection || '').split(',')) blocked.add(key.trim().toLowerCase());
  return Object.fromEntries(Object.entries(headers).filter(([key]) => !blocked.has(key.toLowerCase())));
}

function reply(req, res, status, body) {
  req.resume();
  res.writeHead(status, {'content-type': 'text/plain', 'cache-control': 'no-store'});
  res.end(body);
}

function releaseAdmission(id) {
  if (id && process.connected) process.send({kind: 'release', id}, () => {});
}

function acquireAdmission(req, res) {
  const id = String(++nextAdmission);
  req.pause();
  return new Promise(resolve => {
    let done = false;
    const settle = granted => {
      if (done) return;
      done = true;
      clearTimeout(deadline); pendingAdmissions.delete(id);
      req.off('aborted', cancel); res.off('close', cancel);
      resolve(granted === null ? null : granted ? id : false);
    };
    const cancel = () => { releaseAdmission(id); settle(null); };
    const deadline = setTimeout(() => { releaseAdmission(id); settle(false); }, 2000);
    pendingAdmissions.set(id, settle);
    req.once('aborted', cancel); res.once('close', cancel);
    if (req.aborted || res.destroyed) return cancel();
    if (!process.connected) return settle(false);
    process.send({kind: 'acquire', id}, error => { if (error) settle(false); });
  });
}

const server = http.createServer(async (req, res) => {
  if (!req.url || !req.url.startsWith('/')) return reply(req, res, 400, 'Bad Request');
  if (req.url.split('?', 1)[0] === '/_ren/health') {
    req.resume();
    res.writeHead(200, {'content-type': 'application/json'});
    return res.end('{"status":"ok"}');
  }
  const host = String(req.headers.host || '').toLowerCase().split(':', 1)[0].replace(/\.$/, '');
  const target = resolveTarget(host);
  if (!target) return reply(req, res, 421, 'Unknown domain');
  if (!allowed.has(req.method)) return reply(req, res, 405, 'Allowed methods: GET, HEAD, POST');
  const row = target.row;
  if (stopping || cfg.legacy && !cfg.origin_enabled || !clustered && cfg.max_concurrent && active >= cfg.max_concurrent) {
    row.frontend_5xx_total++;
    return reply(req, res, 503, 'Service Unavailable');
  }
  const admission = clustered && cfg.max_concurrent ? await acquireAdmission(req, res) : undefined;
  if (admission === null) return;
  if (admission === false || stopping || req.aborted || res.destroyed) {
    releaseAdmission(admission);
    if (!res.destroyed) { row.frontend_5xx_total++; reply(req, res, 503, 'Service Unavailable'); }
    return;
  }
  row.accepted_total++; row.active++; active++;
  row.peak = Math.max(row.peak, row.active); peak = Math.max(peak, active);
  let finished = false, cancelled = false, connected = false;
  let uploaded = false, downloaded = false, upstreamResponse;
  const headers = cleanHeaders(req.headers);
  headers.host = target.origin_sni + (target.origin_port === 443 ? '' : ':' + target.origin_port);
  const upstream = https.request({host: target.origin_ip, port: target.origin_port,
    servername: target.origin_sni, method: req.method, path: req.url, headers,
    secureContext, rejectUnauthorized: true, ALPNProtocols: ['http/1.1'], agent: target.agent});
  const connectTimer = setTimeout(() => upstream.destroy(Object.assign(new Error('timeout'),
    {code: 'ETIMEDOUT'})), cfg.connect_timeout * 1000);
  connectTimer.unref();
  function finish(force = false) {
    if (finished || !force && !(uploaded && downloaded)) return;
    finished = true; clearTimeout(connectTimer);
    releaseAdmission(admission);
    row.active--; active--; row.completed_total++;
  }
  function cancel() {
    if (cancelled || finished) return;
    cancelled = true; row.client_aborts_total++;
    upstream.destroy(); upstreamResponse?.destroy(); finish(true);
  }
  req.on('data', chunk => { row.bytes_in_total += chunk.length; });
  req.on('error', cancel);
  req.on('aborted', cancel);
  upstream.once('finish', () => { uploaded = true; finish(); });
  res.once('finish', () => { downloaded = true; finish(); });
  res.once('close', () => { if (!res.writableEnded) cancel(); });
  res.on('error', cancel);
  upstream.once('socket', socket => {
    if (upstream.reusedSocket) {
      row.backend_reuse_total++; connected = true; clearTimeout(connectTimer);
    } else socket.once('secureConnect', () => { connected = true; clearTimeout(connectTimer); });
  });
  upstream.setTimeout(cfg.http_read_timeout * 1000, () =>
    upstream.destroy(Object.assign(new Error('timeout'), {code: 'ETIMEDOUT'})));
  function upstreamFailed(error) {
    if (cancelled || finished) return;
    const phase = connected ? 'response' : 'connect';
    row[connected ? 'response_errors_total' : 'connect_errors_total']++;
    if (cfg.error_logs) sendEvent('REN_NODE_ERROR', {domain: target.domain, phase,
      code: /^[A-Z0-9_]{1,64}$/.test(error.code || '') ? error.code : 'UPSTREAM_ERROR'});
    req.unpipe(upstream); req.resume();
    if (!res.headersSent) { row.frontend_5xx_total++; reply(req, res, 502, 'Bad Gateway'); }
    else res.destroy();
    finish(true);
  }
  upstream.on('error', upstreamFailed);
  upstream.once('upgrade', (_response, socket) => {
    socket.destroy(); upstream.destroy();
    upstreamFailed({code: 'UNSUPPORTED_UPGRADE'});
  });
  upstream.on('response', response => {
    upstreamResponse = response; clearTimeout(connectTimer);
    const status = response.statusCode || 502;
    if (status >= 500) { row.origin_5xx_total++; row.frontend_5xx_total++; }
    const responseHeaders = cleanHeaders(response.headers);
    delete responseHeaders['x-accel-redirect'];
    responseHeaders['x-accel-buffering'] = 'no';
    responseHeaders['cache-control'] = 'no-store';
    res.writeHead(status, responseHeaders);
    res.flushHeaders();
    response.on('data', chunk => { row.bytes_out_total += chunk.length; });
    response.on('error', () => {
      if (!cancelled && !finished) { row.response_errors_total++; res.destroy(); finish(true); }
    });
    response.pipe(res);
  });
  // Native streams handle backpressure. No buffering/retry/path conversion.
  req.pipe(upstream);
});
server.requestTimeout = 0;
server.headersTimeout = 20000;
server.keepAliveTimeout = 15000;
server.setTimeout(cfg.http_read_timeout * 1000, socket => socket.destroy());
function rejectSocket(socket, status) {
  // Upgraded/CONNECT sockets are no longer tracked by server.closeAllConnections().
  socket.end(`HTTP/1.1 ${status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`, () => socket.destroy());
}
server.on('clientError', (_error, socket) => rejectSocket(socket, '400 Bad Request'));
server.on('upgrade', async (req, socket, head) => {
  // Pause before admission/TLS setup: no payload can escape its backpressure chain.
  socket.pause();
  // Observe an empty-buffer FIN even while the handshake is paused. read(0)
  // never consumes early frames or defeats the socket's bounded read buffer.
  const pendingReadable = () => socket.read(0);
  const pendingEnd = () => socket.destroy();
  socket.on('readable', pendingReadable);
  socket.once('end', pendingEnd);
  socket.read(0);
  socket.on('error', () => socket.destroy());
  websocketSockets.add(socket);
  socket.once('close', () => { websocketSockets.delete(socket); queueMicrotask(finishShutdown); });
  if (String(req.headers.upgrade || '').toLowerCase() !== 'websocket') {
    unsupportedUpgrades++;
    return rejectSocket(socket, '501 Not Implemented');
  }
  const key = String(req.headers['sec-websocket-key'] || '');
  if (req.method !== 'GET' || !req.url?.startsWith('/') ||
      req.headers['sec-websocket-version'] !== '13' ||
      !/^[A-Za-z0-9+/]{22}==$/.test(key) || Buffer.from(key, 'base64').length !== 16 ||
      req.headers['transfer-encoding'] || Number(req.headers['content-length'] || 0) !== 0) {
    return rejectSocket(socket, '400 Bad Request');
  }
  const host = String(req.headers.host || '').toLowerCase().split(':', 1)[0].replace(/\.$/, '');
  const target = resolveTarget(host);
  if (!target) return rejectSocket(socket, '421 Misdirected Request');
  const row = target.row;
  const unavailable = () => {
    row.frontend_5xx_total++;
    rejectSocket(socket, '503 Service Unavailable');
  };
  if (stopping || cfg.legacy && !cfg.origin_enabled ||
      !clustered && cfg.max_concurrent && active >= cfg.max_concurrent) return unavailable();
  const admission = clustered && cfg.max_concurrent ? await acquireAdmission(req, socket) : undefined;
  if (admission === null) return;
  if (admission === false || stopping || socket.destroyed) {
    releaseAdmission(admission);
    if (!socket.destroyed) unavailable();
    return;
  }
  row.accepted_total++; row.active++; active++;
  row.peak = Math.max(row.peak, row.active); peak = Math.max(peak, active);
  let finished = false, connected = false, upgraded = false, peer, handshakeTimer;
  const headers = cleanHeaders(req.headers);
  headers.host = target.origin_sni + (target.origin_port === 443 ? '' : ':' + target.origin_port);
  headers.connection = 'Upgrade'; headers.upgrade = 'websocket';
  const upstream = https.request({host: target.origin_ip, port: target.origin_port,
    servername: target.origin_sni, method: 'GET', path: req.url, headers,
    secureContext, rejectUnauthorized: true, ALPNProtocols: ['http/1.1'], agent: target.agent});
  const connectTimer = setTimeout(() => fail('504 Gateway Timeout', 'ETIMEDOUT'), cfg.connect_timeout * 1000);
  connectTimer.unref();
  function finish() {
    if (finished) return;
    finished = true;
    clearTimeout(connectTimer); clearTimeout(handshakeTimer);
    releaseAdmission(admission);
    row.active--; active--; row.completed_total++;
    if (upgraded) row.websocket_active--;
    upstream.destroy(); peer?.destroy();
  }
  function fail(status, code) {
    if (finished) return;
    row[connected ? 'response_errors_total' : 'connect_errors_total']++;
    if (cfg.error_logs) sendEvent('REN_NODE_ERROR', {domain: target.domain,
      phase: connected ? 'response' : 'connect', code});
    if (upgraded) socket.destroy();
    else { row.frontend_5xx_total++; rejectSocket(socket, status); }
    finish();
  }
  socket.once('close', () => { if (!finished && !upgraded) row.client_aborts_total++; finish(); });
  upstream.on('error', error => fail('502 Bad Gateway',
    /^[A-Z0-9_]{1,64}$/.test(error.code || '') ? error.code : 'UPSTREAM_ERROR'));
  upstream.once('socket', originSocket => {
    function tlsReady() {
      if (finished) return;
      connected = true; clearTimeout(connectTimer);
      handshakeTimer = setTimeout(() => fail('504 Gateway Timeout', 'HANDSHAKE_TIMEOUT'),
        cfg.ws_handshake_timeout * 1000);
      handshakeTimer.unref();
    }
    if (upstream.reusedSocket) { row.backend_reuse_total++; tlsReady(); }
    else originSocket.once('secureConnect', tlsReady);
  });
  upstream.once('response', response => {
    if (finished) return response.destroy();
    const status = response.statusCode || 502;
    if (status >= 500) { row.origin_5xx_total++; row.frontend_5xx_total++; }
    // An origin rejection is final; never retry or interpret its body as frames.
    rejectSocket(socket, `${status} ${http.STATUS_CODES[status] || 'Rejected'}`);
    response.destroy(); finish();
  });
  upstream.once('upgrade', (response, originSocket, originHead) => {
    peer = originSocket;
    peer.on('error', () => fail('502 Bad Gateway', 'UPSTREAM_SOCKET_ERROR'));
    if (finished || socket.destroyed) { peer.destroy(); finish(); return; }
    const expected = createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
    if (response.statusCode !== 101 || String(response.headers.upgrade || '').toLowerCase() !== 'websocket' ||
        response.headers['sec-websocket-accept'] !== expected) return fail('502 Bad Gateway', 'INVALID_UPGRADE');
    clearTimeout(connectTimer); clearTimeout(handshakeTimer);
    upgraded = true; row.websocket_active++; row.websocket_accepted_total++;
    peer.pause();
    socket.setNoDelay(true); peer.setNoDelay(true);
    socket.setKeepAlive(true, 30000); peer.setKeepAlive(true, 30000);
    const idle = cfg.tunnel_idle_timeout * 1000;
    socket.setTimeout(idle); peer.setTimeout(idle);
    socket.on('timeout', () => socket.destroy());
    peer.on('timeout', () => { socket.destroy(); finish(); });
    peer.once('close', () => { socket.destroy(); finish(); });
    const responseHeaders = cleanHeaders(response.headers);
    delete responseHeaders['content-length'];
    responseHeaders.connection = 'Upgrade'; responseHeaders.upgrade = 'websocket';
    let handshake = 'HTTP/1.1 101 Switching Protocols\r\n';
    for (const [name, value] of Object.entries(responseHeaders))
      for (const item of Array.isArray(value) ? value : [value]) handshake += `${name}: ${item}\r\n`;
    socket.write(handshake + '\r\n');
    // Restore parser over-read bytes to the streams before connecting pipes.
    if (head.length) socket.unshift(head);
    if (originHead.length) peer.unshift(originHead);
    socket.off('readable', pendingReadable);
    socket.off('end', pendingEnd);
    socket.on('data', data => { row.bytes_in_total += data.length; });
    peer.on('data', data => { row.bytes_out_total += data.length; });
    socket.pipe(peer); peer.pipe(socket);
    socket.resume(); peer.resume();
  });
  upstream.end();
});
server.on('connect', (_req, socket) => rejectSocket(socket, '501 Not Implemented'));
const timer = setInterval(metrics, Math.max(10, cfg.stats_interval * 1000));
timer.unref();
server.listen(cfg.port, '0.0.0.0', metrics);
function stop() {
  if (stopping) return;
  stopping = true;
  server.close(() => { serverClosed = true; finishShutdown(); });
  shutdownTimer = setTimeout(() => {
    server.closeAllConnections();
    for (const socket of websocketSockets) socket.destroy();
    for (const target of targets.values()) target.agent.destroy();
  }, 20000).unref();
}
function finishShutdown() {
  if (!stopping || !serverClosed || websocketSockets.size || shutdownFinished) return;
  shutdownFinished = true; clearTimeout(shutdownTimer);
  clearInterval(timer); metrics();
  for (const target of targets.values()) target.agent.destroy();
  telemetry.close();
  if (clustered && process.connected) process.disconnect();
}
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
if (clustered) {
  process.on('message', message => {
    if (message?.kind === 'grant') {
      const settle = pendingAdmissions.get(message.id);
      if (settle) settle(message.granted);
      else if (message.granted) releaseAdmission(message.id);
    } else if (message?.kind === 'shutdown') stop();
  });
  process.on('disconnect', stop);
}
