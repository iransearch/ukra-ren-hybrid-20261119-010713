Nginx completion events use nonblocking loopback UDP syslog rather than a pipe to Python. Delivery is best effort, so counters describe observed completions and may undercount under load; telemetry_loss_possible=true, telemetry_dropped_total=null. Slow diagnostics must never block data-plane workers. HTTP/1.1 and gRPC/H2 use independent upstream keepalive pools. Origin X-Accel instructions cannot internally replay requests or re-enable buffering. MAX_CONCURRENT accepts 0..65535 in Nginx.

# Proxy engine at service creation

Windows CREATE NEW asks `Proxy engine: haproxy, nginx or node [haproxy]`. HAProxy uses Dockerfile; Nginx uses Dockerfile.nginx and nginx_gateway.py; experimental Node uses Dockerfile.node, node_gateway.py and node_runtime.mjs. PROXY_ENGINE is creation metadata and never rewritten by UPDATE. Updates infer/cross-check the existing Dockerfile and engine marker. Old services retain HAProxy. HAProxy mode selection is hidden/denied for the other engines.

Nginx routes each HTTP Host to its own verified-TLS origin. Ordinary HTTP and packet-up use unbuffered HTTP/1.1; WebSocket upgrades are supported. XHTTP's default application/grpc upload header selects grpc_pass over HTTP/2/TLS, including duplex stream-one. The origin must support h2 for that path. Disabling that header changes classification. No request replay or cross-origin fallback is enabled. The Docker image is pinned to the official nginx:1.28.3-alpine multi-platform digest; the HAProxy Dockerfile remains unchanged.

Nginx emits sanitized completed-request counters only. Long-lived traffic is absent until completion. Request-length bytes omit WebSocket upload frames. Active connections, peak, TCP/TLS errors, response-transfer errors and reuse are null/unsupported, not zero. Raw Nginx error logs are suppressed because they can expose URIs, headers and client identifiers; safe status-only error events remain opt-in. Environment changes require deployment; file hot reload is not provided for Nginx.

Local TLS/streaming tests do not prove live Xray/Render throughput. Do not attribute counter resets or completed-request-only counts to speed improvements.

## Experimental Node.js XHTTP relay

Select `node` only when creating a new service. Start live comparisons with XHTTP packet-up. This engine relays GET/HEAD/POST over verified HTTPS with HTTP/1.1 to origins; it does not support WebSocket, raw TCP or HTTP/2-only origins. It uses direct configured IP, verified SNI and rewritten Host, unchanged path/query and native streaming backpressure in both directions. Per-origin keepalive pools are independent. No retry, redirect following or cross-origin fallback is enabled. Client cancellation aborts its upstream. Python supervises and validates configuration; payload bytes pass only through Node. The Dockerfile uses the pinned official Node 24 Alpine image; no Windows Node installation is needed.

REN_METRICS contains cumulative per-origin request, cancellation, connect/error, active/peak and actual TLS connection/reuse counters. Bytes count HTTP payload bodies observed by the relay, not on-wire bytes. Snapshots use bounded chunks over nonblocking loopback UDP; only complete matching sequences publish, and a lost sample recovers at the next complete cumulative snapshot. Check metrics_age_s before interpreting stale samples. Counters reset at process restart and are instance-local. Sanitized opt-in errors contain domain/phase/code only. MAX_CONCURRENT=0 remains unlimited; positive values cap active requests across all origins. No live speed advantage is claimed.

### Node multicore cluster

Node build ren-node-2 / ren-node-cluster-2 shares one pre-parsed tls.SecureContext per worker across origin connections. It retains public+custom CA roots, explicit verified SNI, rejectUnauthorized=true and independent per-route agents. Passing secureContext instead of raw ca on every request avoids duplicate parsed trust stores and large CA-based agent keys. No admission/rate cap was added. This applies to direct and cluster workers; local200-stream RSS measurements are not production capacity guarantees.

NODE_WORKERS defaults to auto: affinity/cgroup CPUv2 or common CPUv1 quota, minimum1 and automatic maximum8. Explicit values1..32 are accepted. One worker runs the original direct runtime; two or more use node_cluster.mjs with standard-library cluster round-robin distribution of new frontend TCP connections. Existing keepalive connections stay with their worker. Python supervision, direct TLS streaming, routes and HTTP/1.1 semantics remain the same. The manager handles socket distribution, periodic telemetry and optional admission messages, never streaming bodies. Each worker owns a separate origin keepalive pool, so idle64 sockets is per-origin per-worker.

MAX_CONCURRENT=0 has no per-request admission IPC or manual cap. A positive value applies to all workers in one instance; tokens are released at completion/cancellation and reclaimed after worker death. Health is exempt. A failed worker is replaced with bounded exponential backoff; its active streams can drop and are not replayed. Readiness requires every configured worker to listen. Shutdown drains up to20seconds, then forces cleanup; supervisor also kills the process group if the primary dies.

Cluster metrics sum latest per-worker cumulative samples and last observed counters from exited workers. worker_pids/workers/current_workers/target_workers/restarts and per-worker sample_age_s expose freshness. active_scope=sum_of_latest_worker_samples; a cross-worker peak is unavailable/null, never a sum of individual peaks. Missing worker samples produce null counters with samples_complete=false. After a crash counters_incomplete=true persists because unsampled tail traffic may be lost. unsupported_upgrades_total is currently absent in cluster aggregates, not zero; rejection behavior remains enforced. When enabled, admitted_active is the primary's exact reservation count; it differs from sampled active requests. More workers consume additional RAM. Set NODE_WORKERS=1 and deploy for rollback; installer UPDATE code-only applies the new default to existing Node services without changing plan/instance count or asking for engine/worker selection. This does not establish production user capacity.

# Hybrid mode (gateway v7)

Set `HAPROXY_MODE=hybrid`, or choose installer UPDATE > option 7 > hybrid. The TCP frontend preserves new WebSocket upgrades on raw TCP backends and uses HAProxy switch-mode to process other HTTP requests on HTTP backends. This removes the TCP trial's WebSocket-only rejection for XHTTP. HTTP paths/query/body stream without full-body buffering, and origin Host is rewritten to SNI/port. Origin HTTP backends negotiate h2 or HTTP/1.1; raw WebSocket uses HTTP/1.1 and preserves Host. Both enforce TLS certificate and hostname verification. Render remains HTTPS ingress with TLS termination, not public raw TCP.

After a connection switches to HTTP processing, later requests (including a later WebSocket upgrade) remain in HTTP processing and route by each request's Host. A WebSocket connection whose first request is Upgrade retains the raw TCP path. HTTP backend read timeout defaults to 120s; raw TCP idle timeout defaults to 1h. Retries remain zero.

Per-origin REN_METRICS contains separate `http` and `websocket` records. Aggregate traffic/connection counters include both. Mixed HTTP 5xx/response-error/reuse counters and aggregate peak are null; use the HTTP record only and heed `http_status_metrics_scope=http_backends_only`. TCP sessions and HTTP requests are different counting units. Counters reset on deploy/reload and are scoped to instance/generation. HTTP keep-alive applies only to the HTTP leg. Unknown HTTP hosts return 421; unknown raw WebSocket hosts close. Health remains public.

The optional hybrid admission cap reserves one shared tracker across threads and both legs. HTTP transactions release their tracker at completion, including reused frontend connections. The cap is worker/generation-local; draining workers during reload are separate. `http_status_metrics_available=false` describes aggregate completeness, not the availability of nested HTTP counts.

Existing modes `http` and WebSocket-only `tcp` are unchanged. Roll back with the same installer menu. Preparing this package does not change any live service. Local tests use HAProxy, verified TLS origins and incremental streaming; actual Xray client/Render ingress testing remains required.

The historical sections below describe earlier versions; their HTTP-only keep-alive statements do not apply to raw TCP.

# Default keep-alive for all origins

HAProxy defaults use `option http-keep-alive` and `http-reuse safe` for every origin, including legacy single-origin mode. No country selection or enable flag is required. The obsolete ORIGIN_KEEPALIVE_DOMAINS variable is ignored even if an old deployment saved a value. New origins inherit the same policy. In HTTP mode, prior timeout, HTTP/1.1 routing and retry behavior is preserved; retries remain zero. WebSockets remain dedicated after Upgrade.

Update the gateway code and deploy to apply this version. Installer option6 only toggles ORIGIN_ERROR_LOGS; it no longer asks for a mapping. Error logs exclude URI, credentials, headers and client IP. Per-origin metrics report keepalive=true for every route. Counters reset on deploy/reload; compare same-instance deltas over equal windows.

# Multiple independent domain mappings

The Windows installer supports multiple X-UI origins in one Render service. `ORIGIN_ROUTES_JSON` is an authoritative JSON array of objects with exactly `domain`, `origin_ip`, `origin_sni`, `origin_port`. Set it as a Render environment variable, never commit live configuration. Example schema (replace the documentation address before use):

```json
[{"domain":"de.example.com","origin_ip":"203.0.113.10","origin_sni":"origin.example.com","origin_port":2083}]
```

Each HTTP Host routes to its own TLS-verified origin. Host matching is case-insensitive and ignores the optional port. Backend Host is rewritten to origin SNI plus its nonstandard port. Unknown hosts return 421; the gateway health path remains public. HTTP streams and WebSockets retain the selected backend. Each origin has an independent port; `PORT` is one internal HTTP listener behind Render TLS termination and does not expose another public port.

At most 100 routes / 64 KiB. Invalid or duplicate domains fail startup/config validation. When this key is absent, legacy `ORIGIN_*` behavior remains supported as described below. With the key present, stale legacy origin values are ignored. The route table is read at startup; environment changes require deployment. The installer registers all domains on the selected service and deploys after the env update. Route removal keeps Render/DNS registrations but stops forwarding to that origin. Existing sessions follow the platform deployment lifecycle; no cross-origin fallback is performed.

The following documentation describes the legacy control and deployment architecture; the multi-origin mode supersedes single-backend configuration examples when ORIGIN_ROUTES_JSON is set.

# REN Hybrid — Python مدیریت، HAProxy انتقال داده

این بسته جایگزین آزمایشی gateway تک-origin است. HAProxy مستقیماً روی PORT گوش می‌دهد؛
Python هیچ درخواست یا پیام کاربر را دریافت یا رله نمی‌کند. Python فقط تنظیمات، فرآیندها
و آمار را مدیریت می‌کند. HAProxy متن HTTP را به origin مشخص‌شده با TLS معتبر می‌فرستد.
پس از Upgrade، WebSocket به شکل جریان بایت عبور می‌کند.

## راه‌اندازی روی Render

1. محتویات همین پوشه را در یک مخزن آزمایشی قرار دهید؛ Dockerfile باید در ریشه باشد.
2. یک **Web Service جدید** با Language برابر **Docker** در همان region سرویس فعلی بسازید.
3. Dockerfile Path را `./Dockerfile` بگذارید. Docker Command را خالی بگذارید؛ ENTRYPOINT
   داخل image برابر `python3 /app/main.py` است. دستور `pip install` لازم نیست.
4. همان متغیرهای `ORIGIN_IP`، `ORIGIN_SNI` و `ORIGIN_PORT` سرویس فعلی را وارد کنید.
5. Health Check Path برابر `/_ren/health` باشد. این endpoint زنده‌بودن HAProxy را می‌سنجد؛
   سالم‌بودن origin را تضمین نمی‌کند و حتی با origin غیرفعال باید 200 بدهد.
6. `MAX_CONCURRENT=0` بگذارید یا متغیر را حذف کنید: محدودیت عددی برنامه غیرفعال است.
   متغیر قدیمی با مقدار 2000 یا 8000 را کپی نکنید مگر اینکه واقعاً سقف بخواهید.
7. پس از Deploy، پیام `REN Hybrid online build=ren-hybrid-1` و خطوط `REN_METRICS` را ببینید.
8. ابتدا با URL آزمایشی اتصال واقعی کلاینت را بررسی کنید. انتقال دامنه/ترافیک اصلی را بعد
   از تست سرعت، آپلود، دانلود و پایداری انجام دهید. نسخه فعلی را برای بازگشت نگه دارید.

این بسته روی Render منتشر نشده و تنظیمات سرویس فعلی را تغییر نداده است.

## متغیرها

| متغیر | پیش‌فرض | معنی |
|---|---|---|
| ORIGIN_IP | الزامی | IPv4 عمومی و ثابت origin؛ برای اتصال DNS استفاده نمی‌شود |
| ORIGIN_SNI | الزامی | نام گواهی origin؛ SNI، verifyhost و Host از این نام ساخته می‌شوند |
| ORIGIN_PORT | 443 | پورت TLS origin |
| ORIGIN_ENABLED | true | با false اتصال جدید به origin رد می‌شود؛ تونل‌های قبلی ادامه دارند |
| PORT | 10000 | پورت HTTP داخلی رندر |
| MAX_CONCURRENT | 0 | صفر یعنی بدون سقف پذیرش در برنامه؛ مقدار مثبت سقف اختیاری backend است |
| CONNECT_TIMEOUT | 12 | ثانیه؛ اتصال به origin |
| WS_HANDSHAKE_TIMEOUT | 20 | ثانیه؛ انتظار پاسخ Upgrade |
| HTTP_READ_TIMEOUT | 120 | ثانیه؛ سکوت خواندن/نوشتن HTTP |
| TUNNEL_IDLE_TIMEOUT | 3600 | ثانیه؛ سکوت کامل تونل پس از Upgrade؛ نه حداکثر عمر اتصال |
| STATS_INTERVAL | 10 | فاصله لاگ آمار، ثانیه |
| HAPROXY_THREADS | تشخیص CPU، حداکثر 8 | تعداد threadهای پردازش HAProxy؛ قابل تنظیم متناسب با پلن |
| ORIGIN_CA_FILE | فایل همراه بسته | CA مورد اعتماد؛ مثلاً /etc/secrets/custom-ca.pem |
| ORIGIN_CONFIG_FILE | خالی | مسیر فایل JSON اختیاری برای تغییر زنده origin |
| CONFIG_POLL_INTERVAL | 5 | فاصله بررسی فایل JSON، ثانیه |
| ALLOW_PRIVATE_ORIGIN | false | فقط برای تست محلی یا شبکه داخلی؛ در اینترنت لازم نیست |

«بدون سقف برنامه» به معنی نامحدودبودن ظرفیت سیستم نیست. HAProxy سقف socket خود را از
بودجه واقعی فایل‌های باز فرآیند، با مقداری رزرو، محاسبه می‌کند. سقف RAM، CPU، شبکه و origin
همچنان برقرارند. مقدار `resource_capacity` در لاگ سقف مشتق‌شده از FD است و ظرفیت امن
حافظه یا تضمین تعداد اتصال نیست. نرم‌افزار محدودیت FD نامحدود را کاهش نمی‌دهد.

## تفاوت‌های عمدی با نسخه aiohttp

- هیچ وابستگی runtime به aiohttp و هیچ monkey patch روی TLS در Python وجود ندارد.
- SSL_READ_BUFFER و WS_MAX_MSG قبلی استفاده نمی‌شوند؛ HAProxy پیام WebSocket را کامل
  بازسازی نمی‌کند و محدودیت 1MiB پیام در gateway وجود ندارد. محدودیت دو endpoint باقی است.
- HAProxy heartbeat وب‌سوکت تولید نمی‌کند. کلاینت/origin باید ping/pong مناسب داشته باشند.
  تونل بدون داده به مدت TUNNEL_IDLE_TIMEOUT بسته می‌شود؛ این با heartbeat=60 قبلی یکسان نیست.
- HTTP بدون Upgrade بعد از HTTP_READ_TIMEOUT سکوت قطع می‌شود؛ برای SSE مقدار مناسب انتخاب کنید.
- خطای اتصال TLS معمولاً 503 و timeout پاسخ origin معمولاً 504 است؛ معادل دقیق 502 قبلی نیست.
- Host به ORIGIN_SNI و پورت origin بازنویسی می‌شود. مسیر و query بدون بازنویسی برنامه‌ای عبور می‌کنند.
- استانداردهای HTTP و framing را HAProxy مدیریت می‌کند؛ گواهی و نام سرور همیشه بررسی می‌شوند.
- درخواست نامعتبر HTTP ممکن است طبق قوانین HAProxy رد شود؛ سازگاری کلاینت واقعی باید تست شود.
- مذاکره فشرده‌سازی WebSocket بین دو endpoint عبور می‌کند؛ HAProxy خودش پیام را فشرده یا باز نمی‌کند.

## تغییر زنده تنظیمات

فایل `origin.example.json` شکل ورودی را نشان می‌دهد. فقط چهار کلید ORIGIN_IP/PORT/SNI/ENABLED
در JSON پذیرفته می‌شوند. فایل JSON بر متغیرهای همان origin اولویت دارد. فایل جدید را
در همان filesystem با نوشتن فایل موقت و rename اتمی جایگزین کنید.

تغییر ORIGIN_ENABLED با سوکت Runtime API داخلی اعمال و در کانفیگ ماندگار می‌شود.
تغییر IP، پورت یا SNI ابتدا با `haproxy -c` بررسی شده، سپس graceful reload می‌شود.
تونل‌های قبلی روی worker قدیمی می‌مانند؛ اتصال‌های جدید به origin جدید می‌روند.
تا تخلیه worker قبلی، reload ساختاری بعدی عقب می‌افتد تا نسل‌های قدیمی جمع نشوند.
آخرین فایل معتبر پس از تغییر نامعتبر حفظ می‌شود. شکست تأییدشده reload باعث بازگردانی فایل
قبلی می‌شود و تونل سالم قطع نمی‌شود. اگر نتیجه reload نامشخص بماند، pending_reload در لاگ
true می‌شود و کنترلر تا روشن‌شدن نتیجه تغییر ساختاری دیگری اعمال نمی‌کند. CA، پورت frontend و timeoutها در این
نسخه با Deploy تغییر می‌کنند. تغییر env در Render خودبه‌خود فایل زنده را عوض نمی‌کند.

مدیریت فقط با Unix socket با دسترسی مالک فرآیند است؛ هیچ پورت یا API مدیریتی عمومی نیست.
روی Render، فایل‌های محلی پس از جایگزینی instance پایدار نیستند. برای تنظیمات ثابت از env
استفاده کنید؛ قابلیت فایل زنده فقط با منبعی که فایل را واقعاً به‌روزرسانی کند مفید است.
برای نسخه اول GitHub polling یا پنل مدیریت اضافه نشده است.

## لاگ و تفسیر

هر instance مقدار instance و pid جدا دارد. لاگ شامل بدنه، رمز یا آدرس کاربران نیست.

- `active`: اتصال backend فعال در worker فعلی به‌علاوه worker در حال تخلیه؛ نه تعداد افراد.
- `active_current` و `draining_connections`: تفکیک اتصال‌ها قبل و بعد از reload.
- `peak`: پیک worker فعلی از زمان شروع همان نسل، نه پیک هر 10 ثانیه.
- `rss_sum_mb`: جمع RSS کنترلر، master و workerها؛ صفحات مشترک ممکن است چندبار شمرده شوند.
- `cgroup_mb/cgroup_limit_mb`: مصرف/سقف کانتینر در صورت دسترسی؛ null یعنی در دسترس نیست.
- `fds_sum`: مجموع FD فرآیندها؛ `processes/current_workers/draining_workers` برای تشخیص تجمع.
- `bytes_in_total/bytes_out_total`: آمار HAProxy شامل سربار HTTP؛ صورتحساب ترافیک کاربر نیست.
- `connect_errors_total/response_errors_total/queued`: خطا و صف backend.
- `frontend_5xx_total/origin_5xx_total`: همه 5xxها؛ دقیقاً شمارنده رد ظرفیت نیستند.
- `generation`: با reload بالا می‌رود؛ شمارنده‌های total worker جدید از نو شروع می‌شوند.

در shutdown، کنترلر تا 20 ثانیه به جریان‌های موجود فرصت می‌دهد و سپس فرآیندها را می‌بندد.
Deploy یا حذف instance رندر می‌تواند اتصال‌های موجود را قطع کند؛ reload داخلی HAProxy
جایگزین قابلیت اتصال مجدد در کلاینت نیست.

## تست محلی

Runtime فقط Python استاندارد و HAProxy 3.2.26 می‌خواهد. برای اجرای تست‌ها:

```sh
python3 -m pip install -r requirements-dev.txt
HAPROXY_BIN=/path/to/haproxy python3 -m unittest discover -s tests -v
```

تست‌ها یک origin TLS موقت محلی می‌سازند. به openssl CLI نیاز دارند. اگر sandbox ساخت AF_UNIX
را ممنوع کند، فقط در تست transport مدیریت با TCP روی loopback جایگزین می‌شود؛ کد production
همچنان Unix socket استفاده می‌کند. image و استقرار نهایی باید روی محیط Docker/Render تست شوند.

ریشه‌های عمومی Origin CA از مستندات رسمی Cloudflare دریافت شده‌اند:
https://developers.cloudflare.com/ssl/static/origin_ca_rsa_root.pem
https://developers.cloudflare.com/ssl/static/origin_ca_ecc_root.pem

## Node engine WebSocket support (ren-node-3 / ren-node-cluster-3)

Node supports HTTP/1.1 XHTTP and WebSocket on the same listener and domain mappings. TLS trust is shared per worker; origin certificates/SNI remain verified. Raw socket pipes preserve frames, binary, fragmentation, ping/pong, subprotocol early data and extension negotiation, using native backpressure without parsing/recompressing payloads. The gateway does not generate WebSocket heartbeats.

CONNECT_TIMEOUT covers TCP/TLS; WS_HANDSHAKE_TIMEOUT starts after verified TLS; TUNNEL_IDLE_TIMEOUT applies after 101. Existing HTTP_READ_TIMEOUT applies to HTTP. MAX_CONCURRENT is shared across HTTP and WebSocket, including cluster workers, and defaults to unlimited (0). Active counts include handshakes and HTTP; websocket_active counts established tunnels. bytes_scope is HTTP_payload_body_and_WebSocket_frames, excluding HTTP/TLS headers. SIGTERM allows 20 seconds to drain, then closes upgraded and ordinary sockets.

Update the existing Node service with gateway-code-only. No new service or engine change is needed. Local TLS tests do not establish production Xray compatibility or Render capacity.

## Official Render hostname (ren-node-4)

With exactly one configured destination, the Node engine also accepts this service's exact official hostname from Render's `RENDER_EXTERNAL_HOSTNAME` environment variable for HTTP and WebSocket. It reuses the configured origin IP, TLS port, verified SNI and connection agent. No custom-domain registration of the `.onrender.com` hostname is needed.

Unrelated hostnames remain rejected. With multiple configured destinations, use their explicit custom hostnames; the official Render hostname does not select a default destination. Legacy single-origin routing is unchanged. Do not replace the X-UI origin certificate hostname with the Render hostname.
