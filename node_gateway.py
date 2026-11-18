#!/usr/bin/env python3
"""Validate configuration and supervise the experimental Node HTTP/1.1 relay."""
from dataclasses import asdict
import json
import logging
import math
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from main import load_settings, Route, process_tree, process_metrics, cgroup_metrics

LOG = logging.getLogger('ren_control')


def node_workers(env):
    value = str(env.get('NODE_WORKERS', 'auto')).strip().lower()
    if value != 'auto':
        if not value.isdecimal() or not 1 <= int(value) <= 32:
            raise ValueError('NODE_WORKERS must be auto or 1..32')
        return int(value)
    try:
        count = len(os.sched_getaffinity(0))
    except AttributeError:
        count = os.cpu_count() or 1
    v2_root = Path('/sys/fs/cgroup')
    v1_roots = [Path('/sys/fs/cgroup/cpu'), Path('/sys/fs/cgroup/cpu,cpuacct')]
    candidates = {(v2_root, 2), *((root, 1) for root in v1_roots)}
    try:
        membership = Path('/proc/self/cgroup').read_text().splitlines()
    except OSError:
        membership = []
    for line in membership:
        try:
            kind, controllers, path = line.split(':', 2)
        except ValueError:
            continue
        relative = Path(path.lstrip('/'))
        if '..' in relative.parts:
            continue
        roots = [(v2_root, 2)] if kind == '0' and not controllers else (
            [(root, 1) for root in v1_roots] if 'cpu' in controllers.split(',') else [])
        for root, version in roots:
            current = root / relative
            while True:
                candidates.add((current, version))
                if current == root:
                    break
                current = current.parent
    for root, version in candidates:
        try:
            if version == 2:
                quota, period = (root / 'cpu.max').read_text().split()
                if quota == 'max':
                    continue
                quota, period = int(quota), int(period)
            else:
                quota = int((root / 'cpu.cfs_quota_us').read_text())
                period = int((root / 'cpu.cfs_period_us').read_text())
            if quota > 0 and period > 0:
                count = min(count, math.ceil(quota / period))
        except (OSError, ValueError):
            pass
    return max(1, min(count, 8))


class Controller:
    def __init__(self, env=None):
        self.env = dict(os.environ if env is None else env)
        self.settings = load_settings(self.env)
        self.worker_count = node_workers(self.env)
        if self.settings.max_concurrent > 2147483647:
            raise ValueError('Node MAX_CONCURRENT must be 0..2147483647')
        self.run_dir = Path(tempfile.mkdtemp(prefix='ren-node-'))
        self.events = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.events.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.events.bind(('127.0.0.1', 0))
        self.events.setblocking(False)
        self.process = None
        self.snapshot = None
        self.received_at = None
        self.pending_sequence = -1
        self.pending_count = 0
        self.pending_parts = {}
        self.stopping = False

    def start(self):
        s = self.settings
        ca = self.run_dir / 'origin-ca.pem'
        shutil.copyfile(s.ca_file, ca)
        ready = self.run_dir / 'cluster-ready'
        config = {**asdict(s), 'ca_file': str(ca), 'metrics_port': self.events.getsockname()[1],
                  'workers': self.worker_count, 'ready_file': str(ready)}
        config['routes'] = [asdict(r) for r in s.routes] or [asdict(Route('_', s.origin_ip, s.origin_sni, s.origin_port))]
        config['legacy'] = not bool(s.routes)
        path = self.run_dir / 'settings.json'
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        self.process = subprocess.Popen([self.env.get('NODE_BIN', 'node'),
            str(Path(__file__).with_name('node_runtime.mjs' if self.worker_count == 1 else 'node_cluster.mjs')), str(path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        for _ in range(500):
            if self.process.poll() is not None:
                raise RuntimeError('Node relay exited during startup')
            try:
                if self.worker_count > 1 and not ready.exists():
                    time.sleep(.02)
                    continue
                with socket.create_connection(('127.0.0.1', s.port), timeout=.05):
                    LOG.info('REN_ENGINE engine=node mode=http1-experimental websocket=true workers=%d', self.worker_count)
                    return
            except OSError:
                time.sleep(.02)
        raise RuntimeError('Node relay listener readiness timed out')

    def read_events(self):
        if self.events.fileno() < 0:
            return
        for _ in range(64):
            try:
                data = self.events.recv(65535)
            except BlockingIOError:
                break
            try:
                prefix, body = data.split(b' ', 1)
                event = json.loads(body)
                if not isinstance(event, dict):
                    continue
                if prefix == b'REN_NODE_METRICS' and event.get('engine') == 'node':
                    self.metrics_chunk(event)
                elif prefix == b'REN_NODE_ERROR' and self.settings.error_logs:
                    LOG.warning('REN_ORIGIN_ERROR engine=node domain=%s phase=%s code=%s',
                                event['domain'], event['phase'], event['code'])
            except (ValueError, KeyError, TypeError):
                continue

    def metrics_chunk(self, event):
        sequence, part, count = (event.get(k) for k in
                                 ('snapshot_sequence', 'snapshot_part', 'snapshot_parts'))
        if (any(type(v) is not int for v in (sequence, part, count)) or
                not 1 <= count <= 2 or not 0 <= part < count or sequence < self.pending_sequence or
                not isinstance(event.get('origins'), list) or len(event['origins']) > 50):
            return
        if sequence > self.pending_sequence:
            self.pending_sequence, self.pending_count = sequence, count
            self.pending_parts = {}
        if count != self.pending_count:
            return
        self.pending_parts[part] = event
        if len(self.pending_parts) == count:
            sample = dict(self.pending_parts[0])
            sample.pop('snapshot_part')
            sample.pop('snapshot_parts')
            sample['origins'] = [row for index in range(count)
                                 for row in self.pending_parts[index]['origins']]
            self.snapshot = sample
            self.received_at = time.monotonic()

    def log_metrics(self):
        self.read_events()
        sample = dict(self.snapshot or {'engine': 'node', 'mode': 'http1-experimental',
                                       'active': None, 'origins': []})
        sample.update(process_metrics(process_tree(os.getpid())))
        sample.update(cgroup_metrics())
        sample['metrics_age_s'] = None if self.received_at is None else round(time.monotonic() - self.received_at, 3)
        sample['telemetry_delivery'] = 'cumulative_snapshots_over_loopback_udp'
        LOG.info('REN_METRICS %s', json.dumps(sample, separators=(',', ':')))

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGTERM)
                try:
                    self.process.wait(timeout=23)
                except subprocess.TimeoutExpired:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait()
            # A dead primary may leave live workers in its process group.
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process = None
        self.events.close()
        shutil.rmtree(self.run_dir, ignore_errors=True)

    def run(self):
        def stop_requested(*_):
            self.stopping = True
        signal.signal(signal.SIGTERM, stop_requested)
        signal.signal(signal.SIGINT, stop_requested)
        try:
            self.start()
            next_metrics = 0
            while not self.stopping:
                if self.process.poll() is not None:
                    raise RuntimeError('Node relay process exited')
                self.read_events()
                if time.monotonic() >= next_metrics:
                    self.log_metrics()
                    next_metrics = time.monotonic() + self.settings.stats_interval
                time.sleep(.05)
        finally:
            self.stop()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
    c = None
    try:
        c = Controller()
        c.run()
        return 0
    except Exception as exc:
        LOG.error('REN_FATAL %s', str(exc))
        return 1
    finally:
        if c is not None:
            c.stop()


if __name__ == '__main__':
    raise SystemExit(main())
