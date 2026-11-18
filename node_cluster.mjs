// Primary owns socket distribution, optional admission and telemetry; never payload streams.
import cluster from 'node:cluster';
import dgram from 'node:dgram';
import fs from 'node:fs';
import os from 'node:os';
import {fileURLToPath} from 'node:url';

const cfg = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const started = performance.now();
const telemetry = dgram.createSocket('udp4');
telemetry.on('error', () => {}); telemetry.unref();
const states = new Map(), retired = new Map(), restartTimers = new Set();
const totals = ['accepted_total', 'completed_total', 'bytes_in_total', 'bytes_out_total',
  'connect_errors_total', 'response_errors_total', 'client_aborts_total',
  'origin_5xx_total', 'frontend_5xx_total', 'backend_connect_total', 'backend_reuse_total',
  'websocket_accepted_total'];
for (const r of cfg.routes) retired.set(r.domain, Object.fromEntries(totals.map(k => [k, 0])));
let stopping = false, reservations = 0, restarts = 0, crashes = 0, sequence = 0, shutdownTimer;
let retiredUnsupportedUpgrades = 0;
cluster.schedulingPolicy = cluster.SCHED_RR;
cluster.setupPrimary({exec: fileURLToPath(new URL('./node_runtime.mjs', import.meta.url)),
  args: [process.argv[2]], stdio: ['ignore', 'ignore', 'ignore', 'ipc']});

function emit(sample) {
  const parts = Math.ceil(sample.origins.length / 50);
  const id = ++sequence;
  for (let part = 0; part < parts; part++) {
    const data = Buffer.from('REN_NODE_METRICS ' + JSON.stringify({...sample,
      origins: sample.origins.slice(part * 50, (part + 1) * 50),
      snapshot_sequence: id, snapshot_part: part, snapshot_parts: parts}));
    if (data.length <= 65507) telemetry.send(data, cfg.metrics_port, '127.0.0.1', () => {});
  }
}

function metrics() {
  const live = [...states.values()];
  const complete = live.length === cfg.workers && live.every(s => s.latest);
  const origins = cfg.routes.map(route => {
    const rows = live.map(s => s.latest?.origins.find(r => r.domain === route.domain));
    const row = {domain: route.domain, keepalive: true,
      active: complete ? rows.reduce((sum, r) => sum + (r?.active || 0), 0) : null,
      websocket_active: complete ? rows.reduce((sum, r) => sum + (r?.websocket_active || 0), 0) : null,
      peak: null};
    for (const key of totals) row[key] = complete ? retired.get(route.domain)[key] +
      rows.reduce((sum, r) => sum + (r?.[key] || 0), 0) : null;
    return row;
  });
  const sample = {engine: 'node', build: 'ren-node-cluster-3', mode: 'http1-cluster-experimental',
    instance: process.env.RENDER_INSTANCE_ID || os.hostname(), generation: 0,
    uptime_s: (performance.now() - started) / 1000, current_workers: live.filter(s => s.ready).length,
    target_workers: cfg.workers, draining_workers: stopping ? live.length : 0,
    worker_pids: live.map(s => s.worker.process.pid), workers: live.map(s => ({pid: s.worker.process.pid,
      ready: s.ready, active: s.latest?.active ?? null, accepted_total: s.latest?.accepted_total ?? null,
      sample_age_s: s.at === null ? null : (performance.now() - s.at) / 1000})),
    worker_restarts_total: restarts, worker_crashes_total: crashes,
    counters_incomplete: crashes > 0 || !complete, samples_complete: complete,
    active: complete ? origins.reduce((sum, r) => sum + r.active, 0) : null,
    active_scope: 'sum_of_latest_worker_samples', peak: null,
    peak_scope: 'unavailable_across_workers', admitted_active: cfg.max_concurrent ? reservations : null,
    websocket_supported: true,
    websocket_active: complete ? origins.reduce((sum, r) => sum + r.websocket_active, 0) : null,
    unsupported_upgrades_total: complete ? retiredUnsupportedUpgrades +
      live.reduce((sum, s) => sum + s.latest.unsupported_upgrades_total, 0) : null,
    http_status_metrics_available: true,
    bytes_scope: 'HTTP_payload_body_and_WebSocket_frames', origins};
  for (const key of totals) sample[key] = complete ? origins.reduce((sum, r) => sum + r[key], 0) : null;
  emit(sample);
}

function readiness() {
  if (!stopping && [...states.values()].filter(s => s.ready).length === cfg.workers)
    fs.writeFileSync(cfg.ready_file, 'ready\n', {mode: 0o600});
  else { try { fs.unlinkSync(cfg.ready_file); } catch (error) { if (error.code !== 'ENOENT') throw error; } }
}

function release(state, id) {
  if (state.tokens.delete(id)) reservations--;
}

function fork() {
  if (stopping) return;
  const worker = cluster.fork({REN_NODE_CLUSTER: '1'});
  const state = {worker, ready: false, latest: null, at: null, tokens: new Set()};
  states.set(worker.id, state);
  worker.on('error', () => worker.kill());
  worker.on('listening', () => { state.ready = true; readiness(); metrics(); });
  worker.on('message', message => {
    if (message?.kind === 'ren_metrics') {
      state.latest = message.sample; state.at = performance.now();
    } else if (message?.kind === 'acquire' && cfg.max_concurrent && typeof message.id === 'string') {
      const granted = !stopping && reservations < cfg.max_concurrent;
      if (granted && !state.tokens.has(message.id)) { state.tokens.add(message.id); reservations++; }
      worker.send({kind: 'grant', id: message.id, granted}, error => { if (error) release(state, message.id); });
    } else if (message?.kind === 'release') release(state, message.id);
  });
  worker.on('exit', () => {
    reservations -= state.tokens.size;
    state.tokens.clear();
    retiredUnsupportedUpgrades += state.latest?.unsupported_upgrades_total || 0;
    if (state.latest) for (const row of state.latest.origins) {
      const saved = retired.get(row.domain);
      if (saved) for (const key of totals) saved[key] += row[key];
    }
    states.delete(worker.id); readiness();
    if (stopping) {
      if (!states.size) finish();
    } else {
      crashes++;
      const delay = Math.min(5000, 250 * 2 ** Math.min(crashes - 1, 4));
      const timer = setTimeout(() => { restartTimers.delete(timer); restarts++; fork(); }, delay);
      restartTimers.add(timer);
    }
  });
}

function finish() {
  clearInterval(timer); clearTimeout(shutdownTimer);
  metrics(); telemetry.close();
}

function stop() {
  if (stopping) return;
  stopping = true;
  for (const timer of restartTimers) clearTimeout(timer);
  restartTimers.clear(); readiness();
  for (const {worker} of states.values()) {
    if (worker.isConnected()) worker.send({kind: 'shutdown'}, () => {});
    else worker.kill('SIGTERM');
  }
  if (!states.size) return finish();
  shutdownTimer = setTimeout(() => {
    for (const {worker} of states.values()) worker.kill('SIGKILL');
  }, 21500);
}

const timer = setInterval(metrics, Math.max(10, cfg.stats_interval * 1000));
timer.unref();
process.on('SIGTERM', stop); process.on('SIGINT', stop);
for (let i = 0; i < cfg.workers; i++) fork();
