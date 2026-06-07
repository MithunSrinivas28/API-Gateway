# Nexus - Adaptive Redis Proxy for High-Traffic Resilience

> A distributed backend system built from first principles.

---

## What Is This?

Every major e-commerce platform has the same failure story. Traffic spikes. 
90% of requests hammer the same 20 cache keys. One key expires mid-spike and 
12,000 requests hit the database simultaneously. The database falls over. The 
sale page goes blank.

This is not a scaling problem. This is a cache intelligence problem.

Nexus is a TCP-based Redis proxy that sits transparently between your 
application and Redis — no code changes required. It collapses thousands of 
concurrent cache misses into a single database call. It detects keys under 
heavy load and extends their TTL automatically. It invalidates related keys 
atomically. It keeps the hottest data warm exactly when traffic is at its peak.

Built in Node.js using raw TCP sockets, a hand-rolled RESP protocol parser, 
a YAML policy engine, Redis Lua scripting, Kafka for event-driven automation, 
and a full observability stack with OpenTelemetry, Prometheus, and Grafana.

---

## System Architecture

```
Client
  │
  ▼
┌──────────────────────────────────────────────────────────────┐
│                  API Gateway  (Port 3000)                    │
│                                                              │
│  onRequest   → OTel Root Span (SERVER kind)                  │
│              → Token Bucket Rate Limiter                     │
│              → JWT Verifier (HS256)                          │
│              → Kafka: gateway.rate_limit.hit                 │
│              → Prometheus: rate limit counter                │
│                                                              │
│  preHandler  → Header Injection                              │
│                                                              │
│  Proxy       → Load Balancer (RR / LC / WRR)                │
│              → Circuit Breaker (opossum)                     │
│              → Kafka: gateway.circuit.opened                 │
│              → OTel Child Span (CLIENT kind) + traceparent   │
│              → Retry Engine (withRetry)                      │
│              → Request Transformer                           │
│              → /api/* → Upstream Pool                        │
│                                                              │
│  onResponse  → Response Transformer                          │
│              → OTel Root Span closed (status + code)        │
│              → Prometheus: request counter + histogram       │
│              → Telemetry Logger                              │
│              → Kafka: gateway.request.completed              │
│                                                              │
│  Discovery   → services.json poller (5s)                    │
│                                                              │
│  Endpoints   → /metrics  (Prometheus scrape target)         │
│              → /health   /telemetry  /lb-stats  /cb-stats    │
└──────────────┬──────────────────┬───────────────────────────┘
               │                  │
               ▼                  ▼
     ┌──────────────┐   ┌──────────────┐
     │  Mock 3001   │   │  Mock 3002   │
     │  /products   │   │  /products   │
     │  /health     │   │  /health     │
     └──────────────┘   └──────────────┘

┌──────────────────────────────────────────────────────────────┐
│              Cache Proxy  (Port 6380)                        │
│                                                              │
│  TCP Server  → RESP Parser                                   │
│              → Policy Engine (YAML)                          │
│              → Singleflight Guard                            │
│              → Hot Key Detector                              │
│              → Tag Invalidation (Lua)                        │
│              → RESP Serializer                               │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
                 Redis  (Port 6379)

┌──────────────────────────────────────────────────────────────┐
│            Kafka Broker  (Port 9092)                         │
│                                                              │
│  Topics:                                                     │
│    gateway.rate_limit.hit                                    │
│    gateway.circuit.opened                                    │
│    gateway.request.completed                                 │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│           Consumer Service  (nexus-consumer)                 │
│                                                              │
│  rate_limit.hit   → IP hit counter + auto-block              │
│  circuit.opened   → upstream failure alert                   │
│  request.completed→ latency spike detection                  │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│              Observability Stack                             │
│                                                              │
│  Jaeger   (Port 16686) ← OTel spans via OTLP/HTTP (4318)    │
│  Prometheus (Port 9090) ← scrapes /metrics every 15s        │
│  Grafana  (Port 3004)  ← queries Prometheus, dashboards      │
└──────────────────────────────────────────────────────────────┘
```

Each layer has one job and one job only. The gateway enforces policy and routing. The proxy manages cache intelligence. Redis stores data. Kafka carries events. The consumer acts on them. The observability stack records everything without touching the hot path.

---

## Phase 1 — API Gateway

The gateway is the public face of the system. Every request passes through it. Nothing reaches the upstream without the gateway's approval.

### Token Bucket Rate Limiting

Built from scratch. No library. Each IP gets a bucket with **10 tokens** that refills at **2 tokens per second**. The math on every request:

```
tokens = min(capacity, tokens + (Δt × refillRate))
```

This runs in **O(1)** — a single Map lookup and a subtraction. It fires at `onRequest`, the earliest possible lifecycle point in Fastify. A rejected request never touches the router, the proxy, or the upstream. It's dead before it costs anything.

Real numbers from a stress test:
```
429 response → 0.38ms   ← killed at onRequest
200 response → 5–7ms    ← full round-trip
```

### JWT Authentication (HS256)

The `/auth/token` endpoint validates `client_id` + `client_secret` and returns a signed JWT. Every downstream request carries it as `Authorization: Bearer <token>`. Verification is **pure CPU** — recomputing HMAC-SHA256 costs nanoseconds and requires zero I/O. No session store. No database lookup. Scales linearly with CPU.

### Request Telemetry

Every request — whether it succeeds, gets rate-limited, or fails auth — is logged with full context at `onResponse`. That hook fires after the reply is flushed, so latency and status codes are finalized before they're recorded.

```
GET /api/products → 200 → 6.2ms  → client: gateway-client → ip: 127.0.0.1
POST /auth/token  → 401 → 0.9ms  → client: anonymous
GET /api/products → 429 → 0.38ms → client: anonymous → rate limited
```

Inspect everything live at `GET /telemetry`.

### Request Lifecycle

```
Incoming Request
      │
      ▼
 onRequest    → [1] Rate limit check  → 429 if exhausted
              → [2] JWT verification  → 401 if invalid
      │
      ▼
 preHandler   → Inject X-Forwarded-For + X-Gateway-Timestamp
      │
      ▼
 handler      → Proxy to upstream
      │
      ▼
 onResponse   → Record telemetry (latency now finalized)
```

The ordering is intentional. Authentication and rate limiting fire before routing — rejected requests burn zero upstream resources.

---

## Phase 2 — Intelligent Cache Proxy

The cache proxy is a **TCP server** that sits between the gateway and Redis. It speaks RESP — the same binary protocol Redis itself speaks — which means any Redis client can point at it transparently.

This isn't a caching layer bolted onto HTTP. It operates at the protocol level. It reads raw bytes off a socket, parses them into commands, applies intelligent policy, and either responds from cache or forwards to real Redis.

### Why TCP, Not HTTP?

Redis uses RESP (REdis Serialization Protocol) over raw TCP. There's no HTTP envelope, no headers, no JSON. To proxy Redis transparently, you must speak the same protocol. The `net` module gives exactly that — a raw socket you read bytes from and write bytes to.

### RESP Protocol Parser

TCP has no concept of message boundaries. `SET foo bar` might arrive as 3 separate `data` events. The parser maintains an internal byte buffer, accumulates incoming chunks, and only emits a complete command when every required byte has arrived.

It handles all 5 RESP types:

| Prefix | Type          | Example                             |
|--------|---------------|-------------------------------------|
| `+`    | Simple String | `+OK\r\n`                           |
| `-`    | Error         | `-ERR unknown command\r\n`          |
| `:`    | Integer       | `:1000\r\n`                         |
| `$`    | Bulk String   | `$6\r\nfoobar\r\n`                  |
| `*`    | Array         | `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n` |

Bulk strings are read by byte count, not by scanning for `\r\n`. This makes the parser binary-safe — a value can contain newlines (like a JSON blob) and the parser handles it correctly.

### YAML Policy Engine

Caching rules are declared in a YAML file, not hardcoded. Change which commands get cached, their TTL, and whether caching is enabled — without touching a single line of proxy code.

```yaml
policies:
  - match: GET
    ttl: 60
    enabled: true
  - match: SET
    ttl: 0
    enabled: false
```

### Singleflight Request Deduplication

Without this, a cache miss under high traffic creates a **thundering herd** — every concurrent request for the same uncached key fires its own Redis call simultaneously.

Singleflight collapses all duplicate in-flight requests for the same key into one. The first request fires. Everyone else waits on the same promise. One Redis call. One result returned to all waiters.

```
100 concurrent GET requests for "product:1" (cache miss)
→ Without singleflight: 100 Redis calls
→ With singleflight:      1 Redis call, 100 responses
```

### Tag-Based Cache Invalidation

Keys are grouped under tags. Invalidating a tag deletes all associated keys in a single atomic operation using a Lua script inside Redis.

**Why Lua?** Lua scripts run atomically inside Redis. No other command can interrupt mid-execution. This prevents partial invalidation — the scenario where some keys get deleted and others don't under concurrent requests.

```
tagKey('products', 'product:1')
tagKey('products', 'product:2')

invalidateTag('products')
→ product:1 deleted
→ product:2 deleted
→ tag:products deleted
```

### Hot Key Detection + TTL Adjustment

The proxy tracks how many times each key is accessed. When a key crosses a hit threshold, it's considered "hot" and its TTL is automatically extended.

Hot keys expire at the worst possible time — when they're being hit the hardest. Detecting and extending their TTL keeps the most valuable data warm without any manual intervention. Cold keys expire naturally. Hot keys stay cached.

```
product:1 → hit 10 times → TTL was 60s → TTL becomes 180s
```

---

## Phase 3 — Gateway Enhancements

Phase 3 extends the gateway from a simple proxy into a resilient traffic management layer. It adds load balancing across multiple upstreams, automatic failure detection and recovery, transparent retry logic, response shaping, and live service discovery — all without restarting the process.

### Load Balancing

The gateway maintains a pool of upstream servers. On each request, `lb.pick()` selects one using the configured algorithm. Three algorithms are supported:

**Round-Robin** — cycles through servers sequentially. No state beyond a cursor index. O(1) per pick.

**Least Connections** — routes to whichever server has the fewest active in-flight requests. Requires per-server counters updated via `lb.acquire()` before the request and `lb.release()` in the finally path.

**Weighted Round-Robin** — GCD-reduces the weight list into a flat sequence. A weight-2 server appears twice for every one time a weight-1 server appears. No float math.

```
servers: [{ url: '3001', weight: 2 }, { url: '3002', weight: 1 }]
WRR sequence: ['3001', '3001', '3002']
```

Each server tracks `activeConnections`, `requestsHandled`, and `healthy` state. Inspect live at `GET /lb-stats`.

### Circuit Breaker

Wraps each upstream in a state machine with three states:

```
         failures > threshold
  CLOSED ──────────────────► OPEN
    ▲                          │
    │      test succeeds       │ resetTimeout elapsed
    └──── HALF-OPEN ◄──────────┘
               │
               │ test fails
               ▼
             OPEN
```

- **Closed** — requests flow normally
- **Open** — breaker trips, requests fast-fail immediately without attempting a connection
- **Half-Open** — after `resetTimeout` (10s), one test request is allowed through

Configuration:
```
timeout:                  5000ms   ← upstream must respond within this
errorThresholdPercentage: 50%      ← trip open if 50% of requests fail
resetTimeout:             10000ms  ← stay open for 10s then half-open
volumeThreshold:          3        ← need at least 3 requests before tripping
```

One breaker instance per upstream URL. Inspect state at `GET /cb-stats`.

### Retry Engine

When an upstream call fails, the request is retried on a freshly picked server before returning an error to the client. Because `lb.pick()` is called on each attempt, retries naturally land on a different upstream.

```
Attempt 1 → 3002 (circuit open) → fail
  wait 200ms
Attempt 2 → 3001 (healthy)      → 200 OK ← client sees this
```

The client receives a 200. The failure is invisible. Latency reflects the retry delay (200ms) plus the successful request time.

A 502 is only returned if all `maxAttempts` (3) are exhausted.

### Request/Response Transformation

Every request is mutated before hitting the upstream. Every JSON response is wrapped before reaching the client. No upstream needs to know about it.

**Request** — injects tracing headers, strips forbidden internal headers:
```
x-request-id:      nexus-<timestamp>-<random>
x-gateway-version: 1.0.0
x-internal-secret: [stripped]
x-admin-override:  [stripped]
```

**Response** — wraps JSON payloads with gateway metadata:
```json
{
  "data": { "...upstream response..." },
  "_meta": {
    "requestId": "nexus-1777322108070-hqu2e",
    "gateway": "nexus",
    "timestamp": "2026-04-27T20:35:08.090Z"
  }
}
```

Non-JSON responses (binary, plain text) are passed through untouched.

### Service Discovery

Upstreams are read from `services.json`, not hardcoded. A background poller re-reads the file every 5 seconds and diffs the current pool against the last known state.

- New URL in file → `lb.markHealthy(url)` — added to rotation live
- URL removed from file → `lb.markUnhealthy(url)` — pulled from rotation live

No restart. No downtime. Edit the file and the gateway converges within 5 seconds.

```json
{
  "services": [
    { "url": "http://127.0.0.1:3001", "weight": 2 },
    { "url": "http://127.0.0.1:3002", "weight": 1 }
  ]
}
```

### Phase 3 Request Lifecycle

```
Incoming Request
      │
      ▼
 onRequest    → [1] Rate limit check  → 429 if exhausted
              → [2] JWT verification  → 401 if invalid
      │
      ▼
 preHandler   → Inject X-Forwarded-For + X-Gateway-Timestamp
      │
      ▼
 handler      → withRetry (up to 3 attempts)
                  │
                  ▼
               lb.pick()             → 503 if no healthy servers
                  │
                  ▼
               transformRequest()    → inject x-request-id, strip headers
                  │
                  ▼
               breaker.fire(action)  → fast-fail if circuit open
                  │
                  ▼
               http.request()        → upstream call
                  │
                  ▼
               transformResponse()   → wrap JSON with _meta
                  │
                  ▼
               lb.release()          → decrement activeConnections
      │
      ▼
 onResponse   → Record telemetry
```

---

## Phase 4 — Event-Driven Architecture

Phase 4 decouples observability and automation from the gateway's hot path. Instead of reacting to failures inline — which adds latency and couples concerns — the gateway emits structured events to Kafka. A completely separate consumer service reads those events and acts on them independently.

The gateway fires and forgets. The consumer catches and reacts. Neither knows the other exists beyond the event contract.

### Why Kafka?

Kafka is a distributed commit log, not a message queue. Events are appended to ordered, immutable partitions and retained for a configurable window. This means:

- The consumer can crash and restart without losing events — it resumes from its last committed offset
- Multiple consumers can independently read the same events for different purposes
- Events can be replayed for auditing, debugging, or bootstrapping new services

A traditional queue deletes messages on consumption. Kafka keeps them. That's the difference between a fire-and-forget pipe and a durable event history.

### Event Topics

Three topics are emitted from the gateway:

| Topic                          | Fired When                        | Key Fields                                  |
|--------------------------------|-----------------------------------|---------------------------------------------|
| `gateway.rate_limit.hit`       | An IP exceeds its token bucket    | `ip`, `method`, `url`                       |
| `gateway.circuit.opened`       | A circuit breaker trips open      | `upstream`, `method`, `url`                 |
| `gateway.request.completed`    | Any proxied request finishes      | `method`, `url`, `statusCode`, `latencyMs`, `clientId` |

Every event also carries `emittedAt` — an ISO timestamp added by the producer before sending.

### Partition Key Strategy

Each message is keyed by `correlationId` when present, otherwise round-robined. Keying by correlation ID ensures all events for a single request land on the same partition, which guarantees ordered consumption per request. Out-of-order reads are eliminated without any coordination in the consumer.

### Producer Design

The producer is a single long-lived instance shared across all gateway hooks. KafkaJS producers hold an open TCP connection to the broker — creating one per request would exhaust broker connections immediately.

`emitEvent` is designed to never throw. If the producer is disconnected, the event is dropped with a warning. If the broker is slow, the send is fire-and-forget. Observability must never degrade the gateway's primary function.

### Consumer Service

The consumer runs as a completely separate Node.js process in `nexus-consumer/`. It joins a consumer group (`nexus-consumer-group`), subscribes to all three topics, and routes each message to a handler by topic name.

**Rate limit handler** — tracks per-IP violation counts in memory. After 5 hits from the same IP, it auto-blocks and logs an action alert.

**Circuit breaker handler** — logs a structured alert when any upstream trips open.

**Request completed handler** — maintains a rolling window of the last 50 request latencies. When a new request's latency exceeds 3× the rolling average, it flags a latency spike with full context.

```
[rate-limit] 127.0.0.1 hit rate limit (total: 1) — GET /api/hello
[rate-limit] 127.0.0.1 hit rate limit (total: 5) — GET /api/hello
[ACTION] AUTO-BLOCKED IP: 127.0.0.1 after 5 rate limit violations

[request] GET /api/hello → 200 in 4.23ms | client: gateway-client
[ACTION] LATENCY SPIKE — GET /api/products took 312ms (avg: 5.10ms) | client: gateway-client

[ACTION] ALERT — Circuit opened for upstream: http://127.0.0.1:3002
```

### Phase 4 Event Flow

```
API Gateway (port 3000)
      │
      │  fire-and-forget
      ▼
Kafka Broker (port 9092)
      │
      │  offset-tracked consumption
      ▼
Consumer Service (nexus-consumer)
      │
      ├─ gateway.rate_limit.hit   → count hits per IP → block at threshold
      ├─ gateway.circuit.opened   → alert on upstream failure
      └─ gateway.request.completed→ rolling latency baseline → spike detection
```

---

## Phase 5 — Observability

Phase 5 adds the three pillars of observability to the system: distributed tracing, metrics, and structured log correlation. None of this touches the hot path — spans are batched and flushed asynchronously, metrics are scraped on a pull model, and log fields are injected at request start with zero overhead.

The goal is to answer three different questions:
- **Traces** — where in this specific request did time go?
- **Metrics** — how is the system behaving across all requests right now?
- **Logs** — what exactly happened during this specific event?

### Phase 5a — Distributed Tracing (OpenTelemetry + Jaeger)

Every request through the gateway generates a **trace** — a tree of spans where each span represents a unit of work with a start time, end time, and attributes. The OTel SDK is initialized in `tracing.js` before any other module loads, which is why it must be `require()`-d as the absolute first line of `server.js`.

**How it works:**

```
Incoming Request
      │
      ▼
 onRequest  → extract W3C traceparent from headers (if present)
            → start ROOT span (SERVER kind)
              attributes: http.method, http.url, http.route, net.peer.ip
            → inject trace_id + span_id into Pino logger (child logger)
      │
      ▼
 Proxy      → start CHILD span (CLIENT kind) — upstream.proxy
            → inject traceparent header into outgoing request
              format: 00-<traceId>-<spanId>-01
            → upstream receives traceparent, can start its own child span
            → record http.status_code on response
            → end child span
      │
      ▼
 onResponse → set final http.status_code on root span
            → set span status (OK / ERROR based on status code)
            → end root span
            → BatchSpanProcessor flushes to Jaeger every 500ms
```

**What you see in Jaeger:**

```
nexus-gateway: GET /api/*                        19.4ms
  └─ upstream.proxy                              15.2ms
       peer.service: http://127.0.0.1:3001
       http.status_code: 404
```

The W3C `traceparent` header links spans across service boundaries. If the upstream service also instruments with OTel and reads that header, its spans appear as children of the same trace in Jaeger — one waterfall showing the full cross-service request path.

**Span export:** Spans are batched in-process (`maxQueueSize: 2048`, `scheduledDelayMillis: 500ms`, `maxExportBatchSize: 512`) and exported via OTLP/HTTP to Jaeger on port 4318. The batch processor ensures the hot path never blocks on a network flush.

### Phase 5b — Prometheus Metrics

Prometheus works on a pull model — it scrapes your `/metrics` endpoint on a schedule (every 15 seconds) and stores the values as time-series data. The gateway exposes six instruments:

**Counters** (monotonically increasing):

| Metric | Labels | Description |
|--------|--------|-------------|
| `nexus_http_requests_total` | method, route, status_code | every request received |
| `nexus_rate_limit_hits_total` | ip | every request rejected by rate limiter |
| `nexus_circuit_breaker_opens_total` | upstream | every circuit breaker trip |

**Histograms** (latency distributions):

| Metric | Labels | Description |
|--------|--------|-------------|
| `nexus_http_request_duration_ms` | method, route, status_code | full request latency |
| `nexus_upstream_request_duration_ms` | upstream, method, status_code | upstream call latency only |

**Gauge** (can increase and decrease):

| Metric | Labels | Description |
|--------|--------|-------------|
| `nexus_circuit_breaker_state` | upstream | 0=closed, 1=open, 2=half-open |

Histograms use buckets `[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000]` ms. Buckets let Prometheus compute true percentiles — `histogram_quantile(0.99, ...)` gives the p99 latency over any time window. A plain average can't do that.

The `/metrics` endpoint is unauthenticated and excluded from JWT verification — it's a Prometheus scrape target, not a user-facing route. Default Node.js runtime metrics are also collected under the `nexus_` prefix (event loop lag, heap usage, GC pauses, active handles).

**Grafana dashboards:**

Four panels are configured in the Nexus Gateway dashboard:

```
┌─────────────────────────────┐ ┌─────────────────────────────┐
│  Request Rate (req/sec)     │ │  p99 Request Latency (ms)   │
│  rate(nexus_http_requests   │ │  histogram_quantile(0.99,   │
│  _total[1m])                │ │  rate(..._bucket[1m]))      │
└─────────────────────────────┘ └─────────────────────────────┘
┌─────────────────────────────┐ ┌─────────────────────────────┐
│  Rate Limit Hits (last 5m)  │ │  p95 Upstream Latency (ms)  │
│  increase(nexus_rate_limit  │ │  histogram_quantile(0.95,   │
│  _hits_total[5m])           │ │  rate(..._bucket[1m]))      │
└─────────────────────────────┘ └─────────────────────────────┘
```

### Phase 5c — Structured Log Correlation

Every Pino log line for a request carries `trace_id` and `span_id` from the active OTel span. This is wired at the start of `onRequest` using Pino's child logger:

```javascript
const { traceId, spanId } = span.spanContext();
req.log = req.log.child({ trace_id: traceId, span_id: spanId });
```

Every subsequent `req.log.info(...)` call automatically includes those fields:

```json
{
  "level": 30,
  "reqId": "req-2",
  "trace_id": "c160fc98aa80ca92a9f9238453a4a6a1",
  "span_id": "afd2b0f4229a2dd1",
  "url": "/api/hello",
  "method": "GET",
  "msg": "request started"
}
```

Take any `trace_id` from a log line, paste it into Jaeger's search, and it returns the exact trace for that request. Logs and traces are linked without any external log aggregation system.

### Phase 5 — Full Observability Flow

```
Request arrives
      │
      ▼
 onRequest  → OTel span started (traceId generated)
            → trace_id injected into Pino child logger
            → all logs for this request carry trace_id
      │
      ▼
 Proxy      → traceparent injected into upstream headers
            → upstream span started (CLIENT kind)
            → upstream span ended with status + duration
            → Prometheus upstream histogram updated
      │
      ▼
 onResponse → OTel root span ended
            → Prometheus request counter incremented
            → Prometheus request histogram updated
            → Pino log emitted (with trace_id)
      │
      ▼
 Async      → BatchSpanProcessor flushes spans to Jaeger (500ms)
            → Prometheus scrapes /metrics (15s interval)
            → Grafana queries Prometheus (configurable interval)
```

---

## File Structure

```
nexus/
│
├── api-gateway/
│   ├── server.js           ← hooks, routes, proxy, OTel spans, Kafka emit, Prometheus record
│   ├── tracing.js          ← OTel SDK init, BatchSpanProcessor, OTLP export to Jaeger
│   ├── metrics.js          ← prom-client registry, counters, histograms, gauges
│   ├── kafka/
│   │   └── producer.js     ← single shared KafkaJS producer, connectProducer, emitEvent
│   ├── load-balancer.js    ← round-robin, least-conn, weighted RR
│   ├── circuit-breaker.js  ← opossum wrapper, per-upstream state machine
│   ├── retry.js            ← withRetry — configurable attempts + delay
│   ├── transformer.js      ← request/response transformation
│   ├── discovery.js        ← services.json poller, live pool sync
│   ├── services.json       ← upstream registry
│   ├── rateLimiter.js      ← token bucket (pure in-memory Map)
│   ├── auth.js             ← client credential store + validation
│   └── telemetry.js        ← request log + /telemetry endpoint
│
├── mock-service/
│   ├── server.js           ← upstream mock 1: /products, /health (3001)
│   └── mock2.js            ← upstream mock 2: /products, /health (3002)
│
├── nexus-cache-proxy/
│   ├── index.js            ← entry point, boots TCP server + Redis client
│   ├── server.js           ← TCP server, connection lifecycle, dispatch
│   ├── redis-client.js     ← single shared ioredis instance
│   ├── policy.js           ← loads and queries YAML policy
│   ├── policy.yaml         ← cache rules per command
│   ├── singleflight.js     ← in-flight request deduplication
│   ├── invalidation.js     ← tag-based cache invalidation
│   ├── invalidate.lua      ← atomic Lua deletion script
│   ├── hotkey.js           ← hit tracking + TTL extension
│   └── resp/
│       ├── parser.js       ← stateful streaming RESP decoder
│       └── serializer.js   ← RESP encoder
│
├── nexus-consumer/
│   └── consumer.js         ← KafkaJS consumer, topic router, IP blocker, spike detector
│
└── prometheus.yml          ← Prometheus scrape config (targets: host.docker.internal:3000)
```

---

## Getting Started

**Prerequisites:** Node.js v18+, Docker

### 1. Start Infrastructure

```bash
# Zookeeper (Kafka dependency)
docker run -d --name zookeeper -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  confluentinc/cp-zookeeper:7.5.0

# Kafka broker
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.5.0

# Redis
docker run -d --name redis -p 6379:6379 redis

# Jaeger (distributed tracing UI)
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4318:4318 \
  jaegertracing/all-in-one:1.53

# Prometheus (metrics scraper)
docker run -d --name prometheus \
  -p 9090:9090 \
  -v /path/to/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus:v2.45.0

# Grafana (dashboards)
docker run -d --name grafana \
  -p 3004:3000 \
  grafana/grafana:10.2.0
```

After a reboot, start existing containers:
```bash
docker start zookeeper kafka redis jaeger prometheus grafana
```

### 2. Run Mock Services

```bash
cd mock-service
npm install
node server.js        # port 3001
node mock2.js         # port 3002 (separate terminal)
```

### 3. Run API Gateway

```bash
cd api-gateway
npm install
node server.js        # port 3000
```

### 4. Run Cache Proxy

```bash
cd nexus-cache-proxy
npm install
node index.js         # port 6380
```

### 5. Run Consumer Service

```bash
cd nexus-consumer
npm install
node consumer.js      # listens to all Kafka topics
```

---

## Usage

### Get an Access Token

```bash
curl -X POST http://localhost:3000/auth/token \
  -H "Content-Type: application/json" \
  -d '{"client_id": "gateway-client", "client_secret": "supersecret123"}'
```

### Hit a Protected Route

```bash
curl http://localhost:3000/api/products \
  -H "Authorization: Bearer <your_token>"
```

### Check Load Balancer State

```bash
curl http://localhost:3000/lb-stats \
  -H "Authorization: Bearer <your_token>"
```

### Check Circuit Breaker State

```bash
curl http://localhost:3000/cb-stats \
  -H "Authorization: Bearer <your_token>"
```

### Test the Cache Proxy Directly

```bash
redis-cli -p 6380 SET foo bar
redis-cli -p 6380 GET foo
```

### Inspect Telemetry

```bash
curl http://localhost:3000/telemetry \
  -H "Authorization: Bearer <your_token>"
```

### View Prometheus Metrics

```bash
curl http://localhost:3000/metrics
```

### View Distributed Traces

Open `http://localhost:16686`, select service `nexus-gateway`, click Find Traces.

### View Grafana Dashboards

Open `http://localhost:3004`, navigate to Dashboards → Nexus Gateway.

---

## Port Map

| Service          | Port  |
|------------------|-------|
| API Gateway      | 3000  |
| Mock 1           | 3001  |
| Mock 2           | 3002  |
| Grafana          | 3004  |
| Redis            | 6379  |
| Cache Proxy      | 6380  |
| Zookeeper        | 2181  |
| Kafka Broker     | 9092  |
| Jaeger UI        | 16686 |
| Jaeger OTLP/HTTP | 4318  |
| Prometheus       | 9090  |

---

## Key Design Decisions

**Why `onRequest` for rate limiting and auth?**
It's the earliest Fastify lifecycle hook — before body parsing, before routing. A rejected request at this stage costs microseconds. Putting these checks later wastes cycles on requests that were never going to succeed.

**Why token bucket over fixed-window counters?**
Fixed windows punish users for timing. A burst at 11:59:59 and another at 12:00:01 count as two separate windows despite being 2 seconds apart. Token bucket accumulates allowance over real elapsed time and tolerates natural bursts while enforcing a true average rate.

**Why `0.0.0.0` on the gateway but `127.0.0.1` on the mock services?**
The gateway is the public entry point — it binds to all interfaces. Mock services should never be directly reachable from outside. Binding to loopback enforces that at the network level, not just in code.

**Why replace `@fastify/http-proxy` with a manual proxy?**
`@fastify/http-proxy` hardcodes a single upstream at registration time. Load balancing requires selecting the upstream per request, at runtime. Once you need that control, you own the proxy logic.

**Why one circuit breaker per upstream URL?**
A single breaker across all upstreams would trip because of failures on one server, cutting off healthy ones. Isolating breakers means a failing 3002 only affects traffic routed to 3002. 3001 keeps running uninterrupted.

**Why does the retry loop call `lb.pick()` on each attempt?**
Retrying the same server that just failed makes no sense. Calling `lb.pick()` fresh each time lets the load balancer route the retry to a different — likely healthy — upstream. The LB and retry logic compose naturally without any explicit coordination.

**Why `services.json` for discovery instead of a config in `server.js`?**
A file can be edited while the process is running. A hardcoded config cannot. The 5-second polling interval is a deliberate tradeoff — fast enough to react to changes, slow enough to not waste I/O on every request.

**Why a custom RESP parser instead of using ioredis on both sides?**
ioredis abstracts RESP into JS promises — `client.get('key')`. A proxy needs to be protocol-transparent. It must parse raw bytes to understand what command was issued before deciding whether to serve from cache or forward to Redis. You have to own the parse/serialize cycle.

**Why Lua for cache invalidation?**
Redis executes Lua scripts atomically. No command can interleave between reads and deletes inside the script. Without atomicity, concurrent invalidation requests could partially delete a tag — leaving stale keys in cache with no way to track them.

**Why singleflight?**
A cache miss under load without singleflight means every concurrent request fires its own Redis call. With singleflight, the first request fires, and everyone else awaits the same promise. One network round-trip per key no matter how many concurrent requests arrive simultaneously.

**Why Kafka over a simple event emitter or HTTP webhook?**
An event emitter is in-process — if the gateway crashes, all unsent events are gone. An HTTP webhook is synchronous — it adds latency and couples the gateway to the consumer's availability. Kafka gives durability (events survive crashes), decoupling (consumer can be offline and catch up), and replay (re-consume from any offset for debugging or bootstrapping new services).

**Why a separate consumer process instead of handling events inside the gateway?**
The consumer does things the gateway shouldn't — maintain IP block state, compute rolling averages, trigger alerts. Mixing those concerns into the gateway makes it harder to reason about, test, and scale. A separate process can be redeployed, restarted, or scaled without touching the gateway.

**Why fire-and-forget for `emitEvent`?**
The gateway's job is to proxy requests. Kafka emission is observability infrastructure. If the broker is slow or down, waiting for acknowledgment would degrade every request. Dropping an event is far less harmful than adding 50ms to a user-facing request.

**Why `tracing.js` must be the first require in `server.js`?**
The OTel SDK works by monkey-patching Node's `http` module before it's used by anything else. If Fastify or any other module loads `http` first, the instrumentation misses those calls entirely. The require order is a hard constraint, not a convention.

**Why `BatchSpanProcessor` instead of `SimpleSpanProcessor`?**
`SimpleSpanProcessor` exports spans synchronously — every span end blocks until the export completes. On a busy gateway, this would add the Jaeger network round-trip latency to every single request. `BatchSpanProcessor` queues spans in memory and flushes them in background batches. The hot path never waits on observability infrastructure.

**Why histograms for latency instead of gauges or summaries?**
A gauge only holds the current value — you lose history between scrapes. A Prometheus summary computes percentiles client-side and can't be aggregated across instances. A histogram stores the full bucket distribution server-side, so Prometheus can compute any percentile (`histogram_quantile`) across any time window and aggregate across multiple gateway instances correctly.

**Why pull-based scraping (Prometheus) instead of push-based metrics?**
Push-based systems require the application to know where to send metrics. Pull-based systems let the infrastructure decide what to monitor and when. If the gateway crashes, Prometheus notices immediately — the scrape fails. With push-based metrics, a crashed service just goes silent and you might not notice for minutes.

**Why correlate logs with trace IDs instead of using a centralized log aggregator?**
Centralized log aggregators (ELK, Loki) are the right answer at scale, but they require additional infrastructure. Log-trace correlation via `trace_id` in Pino gives you the link between a specific log event and its full distributed trace in Jaeger without any additional tooling. It's a zero-cost addition that pays off every time you need to debug a specific request.

---

## What's Next

- **Phase 6** — Docker Compose full local stack (single `docker-compose.yml` for all services), GitHub Actions CI/CD pipeline, AWS deployment via ECS + S3 + Lambda

---

## Tech Stack

| Layer               | Technology                            |
|---------------------|---------------------------------------|
| Runtime             | Node.js                               |
| Gateway Framework   | Fastify                               |
| Auth                | @fastify/jwt (HS256)                  |
| Rate Limiting       | Custom token bucket (in-memory Map)   |
| Load Balancing      | Custom (RR / Least-Conn / WRR)        |
| Circuit Breaker     | opossum                               |
| Proxy Transport     | Node.js `http` module                 |
| Cache Proxy         | Custom TCP server (`net` module)      |
| Cache Protocol      | RESP — hand-rolled parser + serializer|
| Redis Client        | ioredis                               |
| Policy Config       | js-yaml                               |
| Atomic Invalidation | Redis Lua scripting                   |
| Event Streaming     | Kafka (KafkaJS)                       |
| Distributed Tracing | OpenTelemetry SDK + Jaeger            |
| Metrics             | prom-client + Prometheus + Grafana    |
| Structured Logging  | Pino (trace_id correlated)            |
| Infrastructure      | Docker                                |

---

*The interesting insights don't come from getting things right — they come from understanding exactly why things go wrong.*


> A distributed backend system built from first principles.

---

