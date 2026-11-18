#!/usr/bin/env python3
"""Nginx data plane; Python validates settings and reports completed-request metrics."""
import argparse
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

from main import (Route, load_settings, quoted, fd_capacity, process_tree,
                  process_metrics, cgroup_metrics)

LOG = logging.getLogger('ren_control')
BUILD = 'ren-nginx-1'


def render_config(s, root, event_port):
    duration = lambda v: str(max(1, math.ceil(v * 1000))) + 'ms'
    health = '''location = /_ren/health {
            access_log off;
            default_type application/json;
            return 200 '{"status":"ok"}';
        }'''
    limit_zone = ('map $host $ren_limit_key { default "all"; }\n'
                  '    limit_conn_zone $ren_limit_key zone=ren_admission:1m;') if s.max_concurrent else ''
    limit = f'limit_conn ren_admission {s.max_concurrent};\n            limit_conn_status 503;' if s.max_concurrent else ''
    config = f'''worker_processes {s.threads};
pid {quoted(root / 'nginx.pid')};
error_log /dev/null crit;
worker_shutdown_timeout 20s;
events {{ worker_connections {min(65536, fd_capacity()[0])}; }}
http {{
    server_tokens off;
    server_names_hash_bucket_size 512;
    client_max_body_size 0;
    client_body_timeout {duration(s.http_read_timeout)};
    client_header_timeout 20s;
    send_timeout {duration(s.tunnel_idle_timeout)};
    tcp_nodelay on;
    merge_slashes off;
    keepalive_timeout 15s;
    client_body_temp_path {quoted(root / 'body')};
    proxy_temp_path {quoted(root / 'proxy')};
    fastcgi_temp_path {quoted(root / 'fastcgi')};
    uwsgi_temp_path {quoted(root / 'uwsgi')};
    scgi_temp_path {quoted(root / 'scgi')};
    log_format ren escape=json 'REN_NGINX_EVENT {{"domain":"$server_name","status":$status,"bytes_in":$request_length,"bytes_out":$bytes_sent,"upstream_status":"$upstream_status"}}';
    access_log syslog:server=127.0.0.1:{event_port},facility=local0,severity=info,nohostname,tag=ren ren;
    {limit_zone}
'''
    routes = s.routes or (Route('_', s.origin_ip, s.origin_sni, s.origin_port),)
    if s.routes:
        config += f'''    server {{
        listen 0.0.0.0:{s.port} default_server;
        server_name _;
        {health}
        location / {{ return 421; }}
    }}
'''
    for i, r in enumerate(routes, 1):
        authority = r.origin_sni + (f':{r.origin_port}' if r.origin_port != 443 else '')
        disabled = not s.routes and not s.origin_enabled
        # Nginx keepalive caches are per upstream group, not per wire protocol.
        # Never let proxy_pass (H1) borrow a cached grpc_pass (H2) socket.
        config += f'''    upstream origin_{i}_http {{
        server {r.origin_ip}:{r.origin_port};
        keepalive 64;
    }}
    upstream origin_{i}_grpc {{
        server {r.origin_ip}:{r.origin_port};
        keepalive 64;
    }}
    server {{
        listen 0.0.0.0:{s.port};
        server_name {r.domain};
        {health}
'''
        if disabled:
            config += '        location / { return 503; }\n    }\n'
            continue
        # Named internal locations preserve the original method, URI and body.
        config += f'''        location / {{
            error_page 418 = @websocket;
            error_page 419 = @xhttp_stream;
            if ($http_upgrade ~* "^websocket$") {{ return 418; }}
            if ($http_content_type ~* "^application/grpc([+][a-z0-9._-]+)?([[:space:]]*;|[[:space:]]*$)") {{ return 419; }}
'''
        for websocket in (False, True):
            if websocket:
                config += '        location @websocket {\n'
            config += f'''            {limit}
            proxy_pass https://origin_{i}_http;
            proxy_http_version 1.1;
            proxy_set_header Host {authority};
            proxy_set_header Connection {'"upgrade"' if websocket else '""'};
            proxy_set_header Upgrade {'$http_upgrade' if websocket else '""'};
            proxy_buffering off;
            proxy_request_buffering off;
            proxy_ignore_headers X-Accel-Redirect X-Accel-Buffering X-Accel-Limit-Rate X-Accel-Charset X-Accel-Expires;
            proxy_cache off;
            proxy_next_upstream off;
            proxy_connect_timeout {duration(s.connect_timeout)};
            proxy_read_timeout {duration(s.tunnel_idle_timeout if websocket else s.http_read_timeout)};
            proxy_send_timeout {duration(s.tunnel_idle_timeout if websocket else s.http_read_timeout)};
            proxy_ssl_server_name on;
            proxy_ssl_name {r.origin_sni};
            proxy_ssl_verify on;
            proxy_ssl_verify_depth 5;
            proxy_ssl_trusted_certificate {quoted(root / 'origin-ca.pem')};
            proxy_ssl_protocols TLSv1.2 TLSv1.3;
        }}
'''
        config += f'''        location @xhttp_stream {{
            {limit}
            grpc_pass grpcs://origin_{i}_grpc;
            grpc_set_header Host {authority};
            grpc_set_header Connection "";
            grpc_next_upstream off;
            grpc_ignore_headers X-Accel-Redirect X-Accel-Charset;
            grpc_connect_timeout {duration(s.connect_timeout)};
            grpc_read_timeout {duration(s.http_read_timeout)};
            grpc_send_timeout {duration(s.http_read_timeout)};
            grpc_socket_keepalive on;
            grpc_ssl_server_name on;
            grpc_ssl_name {r.origin_sni};
            grpc_ssl_verify on;
            grpc_ssl_verify_depth 5;
            grpc_ssl_trusted_certificate {quoted(root / 'origin-ca.pem')};
            grpc_ssl_protocols TLSv1.2 TLSv1.3;
        }}
    }}
'''
    return config + '}\n'


class Controller:
    def __init__(self, env=None):
        self.env = dict(os.environ if env is None else env)
        self.settings = load_settings(self.env)
        if self.settings.max_concurrent > 65535:
            raise ValueError('Nginx MAX_CONCURRENT must be 0..65535')
        self.binary = self.env.get('NGINX_BIN', 'nginx')
        self.run_dir = Path(tempfile.mkdtemp(prefix='ren-nginx-'))
        self.config = self.run_dir / 'nginx.conf'
        self.process = None
        self.started = time.monotonic()
        self.event_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.event_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.event_socket.bind(('127.0.0.1', 0))
        self.event_socket.setblocking(False)
        self.telemetry_parse_errors = 0
        self.stopping = False
        self.counters = {}

    def validate(self):
        shutil.copyfile(self.settings.ca_file, self.run_dir / 'origin-ca.pem')
        for name in ('body', 'proxy', 'fastcgi', 'uwsgi', 'scgi'):
            (self.run_dir / name).mkdir(exist_ok=True)
        self.config.write_text(render_config(self.settings, self.run_dir, self.event_socket.getsockname()[1]))
        result = subprocess.run([self.binary, '-t', '-p', str(self.run_dir), '-c', str(self.config)],
                                capture_output=True, timeout=10)
        if result.returncode:
            raise RuntimeError('Nginx configuration validation failed')

    def start(self):
        self.validate()
        self.process = subprocess.Popen([self.binary, '-p', str(self.run_dir), '-c', str(self.config),
                                         '-g', 'daemon off;'], stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL, start_new_session=True)
        for _ in range(100):
            if self.process.poll() is not None:
                raise RuntimeError('Nginx exited during startup')
            try:
                with socket.create_connection(('127.0.0.1', self.settings.port), timeout=.1):
                    break
            except OSError:
                time.sleep(.02)
        else:
            raise RuntimeError('Nginx listener readiness timed out')
        LOG.info('REN_ENGINE engine=nginx build=%s error_logs=%s', BUILD, self.settings.error_logs)

    def read_events(self):
        if self.event_socket.fileno() < 0:
            return
        # Nginx uses nonblocking UDP syslog. Slow/paused diagnostics may lose
        # observations, but can never backpressure a data-plane worker.
        for _ in range(4096):
            try:
                data = self.event_socket.recv(8192)
            except BlockingIOError:
                break
            _, marker, body = data.partition(b'REN_NGINX_EVENT ')
            if not marker:
                continue
            try:
                event = json.loads(body)
                domain = event['domain']
                row = self.counters.setdefault(domain, {'completed_requests_total': 0,
                    'completed_5xx_total': 0, 'bytes_in_completed_total': 0, 'bytes_out_completed_total': 0})
                row['completed_requests_total'] += 1
                row['completed_5xx_total'] += int(500 <= event['status'] <= 599)
                row['bytes_in_completed_total'] += event['bytes_in']
                row['bytes_out_completed_total'] += event['bytes_out']
                if self.settings.error_logs and event['status'] >= 400:
                    LOG.warning('REN_ORIGIN_ERROR engine=nginx domain=%s status=%d upstream_status=%s',
                                domain, event['status'], event['upstream_status'])
            except (ValueError, KeyError, TypeError):
                self.telemetry_parse_errors += 1

    def log_metrics(self):
        self.read_events()
        routes = self.settings.routes or (Route('_', '', '', 443),)
        origins = [{'domain': r.domain, **self.counters.get(r.domain, {
            'completed_requests_total': 0, 'completed_5xx_total': 0,
            'bytes_in_completed_total': 0, 'bytes_out_completed_total': 0})} for r in routes]
        sample = {'engine': 'nginx', 'mode': 'http', 'build': BUILD,
            'instance': os.getenv('RENDER_INSTANCE_ID') or socket.gethostname(), 'generation': 0,
            'uptime_s': round(time.monotonic() - self.started, 1), 'metrics_scope': 'completed_requests_only',
            'active': None, 'peak': None, 'connect_errors_total': None, 'backend_reuse_total': None,
            'response_errors_total': None, 'origin_5xx_total': None,
            'http_status_metrics_available': False, 'telemetry_dropped_total': None,
            'telemetry_delivery': 'best_effort_nonblocking_syslog', 'telemetry_loss_possible': True,
            'telemetry_parse_errors_total': self.telemetry_parse_errors,
            'origins': origins}
        sample.update(process_metrics(process_tree(os.getpid())))
        sample.update(cgroup_metrics())
        LOG.info('REN_METRICS %s', json.dumps(sample, separators=(',', ':')))

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGQUIT)
                # Consume best-effort events while workers finish.
                deadline = time.monotonic() + 20
                while self.process.poll() is None and time.monotonic() < deadline:
                    self.read_events()
                    time.sleep(.02)
                if self.process.poll() is None:
                    os.killpg(self.process.pid, signal.SIGTERM)
                    try:
                        self.process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        os.killpg(self.process.pid, signal.SIGKILL)
                        self.process.wait()
        self.event_socket.close()
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
                    raise RuntimeError('Nginx process exited')
                self.read_events()
                if time.monotonic() >= next_metrics:
                    self.log_metrics()
                    next_metrics = time.monotonic() + self.settings.stats_interval
                time.sleep(.05)
        finally:
            self.stop()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    controller = None
    try:
        controller = Controller()
        if args.check:
            controller.validate()
            print('REN Nginx configuration valid')
        else:
            controller.run()
        return 0
    except Exception as exc:
        LOG.error('REN_FATAL %s', str(exc))
        return 1
    finally:
        if controller is not None:
            controller.stop()


if __name__ == '__main__':
    raise SystemExit(main())
