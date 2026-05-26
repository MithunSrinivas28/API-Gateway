# Nexus — Backend Engineering Mastery Notes

> Complete notes of nexus made to learn backend 
---

## How to Use This Document

- Read **top to bottom** for first-time mastery
- Jump to any section independently for revision
- Each section: concept → internals → tradeoffs → interview defense
- `> 💡 Real Learning` blocks = honest confusion points + how understanding evolved
- `> ⚠️ Current State` blocks = honest implementation status vs intended design

---

## Table of Contents

1. [Project Vision + Problem Statement](#1-project-vision--problem-statement)
2. [System Architecture Overview](#2-system-architecture-overview)
3. [End-to-End Request Lifecycle](#3-end-to-end-request-lifecycle)
4. [API Gateway Deep Dive](#4-api-gateway-deep-dive)
5. [Load Balancing + Resilience](#5-load-balancing--resilience)
6. [Intelligent Cache Proxy](#6-intelligent-cache-proxy)
7. [Event-Driven Architecture](#7-event-driven-architecture)
8. [Observability Stack](#8-observability-stack)
9. [Failure Scenarios + System Behavior](#9-failure-scenarios--system-behavior)
10. [Scalability + Bottlenecks](#10-scalability--bottlenecks)
11. [Interview Mastery + Defense](#11-interview-mastery--defense)

---

## 1. Project Vision + Problem Statement

---

Every major e-commerce platform has the same failure story.
Traffic spikes. 90% of requests hammer the same 20 cache keys.
One key expires mid-spike and 12,000 requests hit the database simultaneously.
The database falls over. The sale page goes blank.
This is not a scaling problem. This is a cache intelligence problem.
Standard Redis caching has no awareness of how hot a key is when it expires.
It doesn't know that product:homepage is being hit 10,000 times per second.
It expires it anyway. Every waiting request goes straight to the database.
The database was never built to absorb that spike. It collapses.



### The Problem: Cache Stampede

```
Normal traffic — cache key "product:homepage" is warm:

  1000 req/s ──► Cache Proxy ──► Cache HIT ──► response (fast)
                                 ↑
                              TTL: 58s remaining


The key expires at t=0:

  t=0ms   ──► 1000 concurrent requests arrive
              ↓ cache miss
              ↓ cache miss          All 1000 requests
              ↓ cache miss    ──►   hit the database
              ↓ cache miss          simultaneously
              ↓ (×997 more)
                                    Database: overwhelmed
                                    Latency: spikes
                                    Cascading failure begins
```

This is a **cache stampede** (also called thundering herd). It's not a bug — it's a structural problem in how most caching systems work. The cache does its job (expires stale data), and that triggers a failure cascade.

**Why traditional systems fail:**
- Standard Redis + application pattern: if key is missing, every concurrent request calls the database
- HTTP-layer cache (nginx, Varnish): doesn't help when the problem is at the application-to-database layer
- Simple TTL extension: you don't know *which* keys are hot until they're already expiring under load

---

### What Nexus Does Differently

```
What Nexus Does
Nexus is a TCP-based Redis proxy that sits transparently between your
application and Redis — no code changes required. Any Redis client points
at port 6380 instead of 6379. That's it.
Behind that transparent interface, Nexus applies intelligence that standard
Redis clients don't have:

  Application ──► port 6380 ──► [Nexus Proxy] ──► port 6379 ──► Redis
                                      │
                              reads RESP protocol
                              applies intelligence:
                              ├─ singleflight (1 Redis call, N waiters)
                              ├─ hot key detection (auto-extend TTL)
                              ├─ atomic tag invalidation (Lua)
                              └─ YAML policy engine (configurable rules)
```

The proxy is **transparent** — any Redis client can use it without code changes, because it speaks the same RESP protocol Redis does.

---

### What Makes This Project Unusual for a Student Portfolio

Most student backend projects do CRUD over HTTP with a framework. Nexus goes deeper in three ways:

| Dimension | Typical Student Project | Nexus |
|---|---|---|
| Protocol layer | HTTP only | TCP + binary RESP protocol |
| Caching | Redis client library calls | Custom proxy with protocol parsing |
| Resilience | Try/catch | Circuit breaker + retry orchestration |
| Observability | console.log | Distributed tracing + Prometheus metrics |
| Architecture | Monolith | 4 services + event-driven consumer |
| Failure thinking | "it works" | Stampede prevention, atomic invalidation |

**Honest positioning:** This is an advanced learning project. It demonstrates deep engineering curiosity and systems thinking. It is not a production system — Redis is single-instance, rate limiter state is in-memory, and the intelligent caching modules are not yet fully integrated. The architectural decisions are informed by how production systems solve these problems, even where the implementations are simplified.

---

### Core Engineering Claims (All Defensible)

```
✅  Hand-built RESP protocol parser (binary-safe, streaming, fragmentation-aware)
✅  Singleflight deduplication (Promise-sharing, .finally() cleanup)
✅  Atomic Lua cache invalidation (Redis-level atomicity)
✅  TCP proxy transparent to any Redis client
✅  Per-upstream circuit isolation (not shared)
✅  GCD-reduced weighted round-robin (no float arithmetic)
✅  Retry orchestration composing with circuit breaker
✅  Distributed tracing with W3C trace context propagation
✅  Prometheus histogram-based latency percentiles
✅  Kafka event streaming with durable offset-tracked consumption

⚠️  Intelligent caching not yet wired into live TCP path
⚠️  Rate limiter state is per-process (not distributed)
⚠️  Redis is single-instance (no HA)
⚠️  Credentials are hardcoded (learning context)
```

---

## 2. System Architecture Overview

---

### Full System Map

```
┌─────────────────────────────────────────────────────────────────────┐
│  CLIENT (curl / application)                                        │
└────────────────────────────┬────────────────────────────────────────┘
                             │ HTTP request
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  API GATEWAY  :3000                                                 │
│                                                                     │
│  onRequest ──► OTel root span started                               │
│            ──► Token bucket rate limiter  ──► 429 if exhausted      │
│            ──► JWT verification           ──► 401 if invalid        │
│                                                                     │
│  preHandler ──► x-forwarded-for, x-gateway-timestamp injected       │
│                                                                     │
│  /api/* handler                                                     │
│    └─ withRetry (3 attempts, 200ms delay)                           │
│         └─ lb.pick()     ←── services.json poll (5s)               │
│         └─ transformRequest()  → x-request-id, strip headers       │
│         └─ breaker.fire(action)                                     │
│              └─ http.request() ──► upstream                         │
│         └─ transformResponse() → wrap JSON in _meta                │
│                                                                     │
│  onResponse ──► OTel span closed                                    │
│             ──► Prometheus counters + histogram                     │
│             ──► Kafka emit (fire-and-forget)                        │
│             ──► Telemetry logged                                    │
└──────┬──────────────────┬───────────────────────────────────────────┘
       │                  │
       ▼                  ▼
┌──────────┐      ┌──────────┐         Services auto-discovered
│ Mock     │      │ Mock     │         from services.json every 5s
│ :3001    │      │ :3002    │
└──────────┘      └──────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  CACHE PROXY  :6380   (TCP server, speaks RESP)                     │
│                                                                     │
│  TCP socket                                                         │
│    └─ RespParser.feed(chunk) ──► accumulate bytes                   │
│         └─ _parse() ──► INCOMPLETE or complete command              │
│              └─ emit 'command'                                      │
│                   └─ redisClient.call(command, ...args)             │
│                        └─ serialize response ──► socket.write()     │
│                                                                     │
│  Intelligent modules (built, tested, not yet wired into path):     │
│    policy.js      YAML-driven command caching rules                 │
│    singleflight.js  Promise deduplication per key                   │
│    hotkey.js      Hit tracking + automatic TTL extension            │
│    invalidation.js  Atomic Lua-based tag invalidation               │
└────────────────────────────┬────────────────────────────────────────┘
                             │ ioredis connection
                             ▼
                    ┌─────────────────┐
                    │  REDIS  :6379   │
                    │  (Docker)       │
                    └─────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  KAFKA BROKER  :9092                                                │
│                                                                     │
│  Topics:                                                            │
│    gateway.rate_limit.hit      ← emitted on 429                    │
│    gateway.circuit.opened      ← emitted on circuit trip           │
│    gateway.request.completed   ← emitted on every proxied request  │
└────────────────────────────┬────────────────────────────────────────┘
                             │ KafkaJS consumer, offset-tracked
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  CONSUMER SERVICE  (nexus-consumer)                                 │
│                                                                     │
│  rate_limit.hit   ──► count per IP ──► auto-block at 5 hits        │
│  circuit.opened   ──► log upstream failure alert                    │
│  request.completed──► rolling latency window ──► spike detection   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  OBSERVABILITY STACK                                                │
│                                                                     │
│  Jaeger    :16686  ← OTel spans via OTLP/HTTP :4318                │
│  Prometheus :9090  ← scrapes /metrics every 15s                    │
│  Grafana   :3004   ← queries Prometheus, dashboards                │
└─────────────────────────────────────────────────────────────────────┘
```

---

### Service Responsibilities (One-Line Each)

| Service | Single Responsibility |
|---|---|
| API Gateway | Enforce auth, rate limits, and route requests to healthy upstreams |
| Cache Proxy | Intercept Redis commands at protocol level and apply caching intelligence |
| Redis | Store cached values and tag sets |
| Kafka | Durable, ordered event log for gateway-emitted events |
| Consumer | React to gateway events — block IPs, detect anomalies |
| Jaeger | Visualize distributed traces across services |
| Prometheus | Store and query time-series metrics |
| Grafana | Dashboard over Prometheus metrics |

---

### Port Map (Quick Reference)

```
:3000  API Gateway
:3001  Mock upstream 1
:3002  Mock upstream 2
:3004  Grafana
:6379  Redis
:6380  Cache Proxy
:9090  Prometheus
:9092  Kafka
:16686 Jaeger UI
:4318  Jaeger OTLP/HTTP (span ingest)
```

---

### Data vs Control Flow

```
DATA FLOW (requests):
  Client ──► Gateway ──► Upstream ──► Gateway ──► Client

CACHE FLOW (Redis commands):
  Application ──► Cache Proxy :6380 ──► Redis :6379

EVENT FLOW (observability):
  Gateway ──► Kafka ──► Consumer
  Gateway ──► Prometheus (pull-based scrape)
  Gateway ──► Jaeger (push via BatchSpanProcessor)

DISCOVERY FLOW (config):
  services.json ──► Gateway poller (5s) ──► lb pool updated live
```

---

### Why Each Service Is Separate

**Gateway vs Cache Proxy:** Different protocol layers. Gateway is HTTP. Proxy is TCP/RESP. Separating them keeps each focused on one protocol abstraction.

**Consumer vs Gateway:** The consumer does things that don't belong in a request path — maintaining IP block state, computing rolling averages, triggering alerts. Mixing these into the gateway would couple request handling to background analytics. A separate process can be restarted, redeployed, or scaled independently.

**Observability stack:** None of the observability services are in the hot path. Metrics are scraped (pull). Spans are batched and exported async. Events are fire-and-forget. If any observability service goes down, requests continue unaffected.

---

## 3. End-to-End Request Lifecycle

> Tracing one request fully through the system is the single best interview preparation exercise. If you can narrate this path fluently, you understand the entire system.

---

### The Request: `GET /api/products` with a valid JWT

```
Client (curl / frontend)
  └─► API Gateway :3000
        └─► onRequest Hook [1]: OTel span started
        └─► onRequest Hook [2]: Rate limiter checked
        └─► onRequest Hook [2]: JWT verified
        └─► preHandler Hook: Headers injected
        └─► Route Handler /api/*
              └─► withRetry (attempt 1)
                    └─► lb.pick() → Server 3001
                    └─► lb.acquire(server)
                    └─► transformRequest() → x-request-id injected
                    └─► breaker(3001).fire(action)
                          └─► http.request() → Mock Service :3001
                                └─► /products → 200 OK
                          └─► transformResponse() → JSON wrapped in _meta
                          └─► reply.send(wrappedResponse)
                    └─► lb.release(server)
        └─► onResponse Hook
              └─► OTel span closed
              └─► Prometheus counters + histogram updated
              └─► Kafka: gateway.request.completed emitted
              └─► Telemetry logged
```

---

### Stage 1: OTel Span Started (`onRequest`, Hook 1)

The very first thing that happens — before rate limiting, before auth — is starting a distributed trace span.

```javascript
fastify.addHook('onRequest', async (req, reply) => {
  const parentCtx = propagation.extract(context.active(), req.headers);
  const span = tracer.startSpan(`${req.method} ${req.url}`, {
    kind: SpanKind.SERVER,
  }, parentCtx);

  req.otelSpan = span;                                           // ← attached to request
  req.otelCtx = trace.setSpan(parentCtx, span);                 // ← context for child spans

  const { traceId, spanId } = span.spanContext();
  req.log = req.log.child({ trace_id: traceId, span_id: spanId }); // ← every log carries trace ID
});
```

**Why first?** The span needs to wrap the entire request lifetime, including auth and rate limiting. Starting it here means every subsequent log line for this request automatically carries the trace ID — because `req.log` is replaced with a child logger that includes `trace_id` and `span_id`.

**`propagation.extract()`**: Checks if the incoming request carries a `traceparent` header (W3C trace context format). If a service upstream of the gateway already started a trace, this links the gateway's span as a child of that trace. If no header exists, a new root trace is started.

---

### Stage 2: Rate Limiter (`onRequest`, Hook 2)

```javascript
fastify.addHook('onRequest', async (request, reply) => {
  const ip = request.ip;
  if (!isAllowed(ip)) {
    emitEvent('gateway.rate_limit.hit', { ip, method, url });   // ← Kafka, fire-and-forget
    rateLimitHitsTotal.inc({ ip });                              // ← Prometheus counter
    return reply.code(429).send({ error: 'Too Many Requests' });
  }
  // ...
});
```

`isAllowed(ip)` runs the token bucket algorithm:

```javascript
function isAllowed(ip) {
  const bucket = getBucket(ip);   // ← get or create bucket for this IP
  refillBucket(bucket);           // ← add tokens based on elapsed time
  if (bucket.tokens >= 1) {
    bucket.tokens -= 1;
    return true;
  }
  return false;
}
```

**Token bucket math:**
```
tokens = min(BUCKET_CAPACITY, tokens + (elapsed_seconds × REFILL_RATE))
```

`BUCKET_CAPACITY = 10`, `REFILL_RATE = 2 tokens/second`.

A new IP starts with 10 tokens. If you send 10 requests instantly, all succeed. Request 11 fails (429). After 0.5 seconds, 1 token refills. After 5 seconds, full bucket again.

**Why at `onRequest`?** It's the earliest Fastify lifecycle hook — before body parsing, before JWT verification, before routing. A rate-limited request costs microseconds. If this hook were at `preHandler` or later, the gateway would have already done expensive work (body parse, JWT decode) before deciding to reject.

**Measured cost:** 429 response → ~0.38ms. 200 response → 5-7ms. Rate limiting catches abusive IPs before they burn any meaningful resources.

---

### Stage 3: JWT Verification (`onRequest`, Hook 2, continued)

```javascript
const publicPaths = ['/auth/token', '/health', '/metrics'];
if (publicPaths.includes(request.url)) return;  // ← skip auth for public routes

try {
  await request.jwtVerify();
} catch (err) {
  return reply.code(401).send({ error: 'Unauthorized' });
}
```

`request.jwtVerify()` is provided by `@fastify/jwt`. It reads the `Authorization: Bearer <token>` header, decodes the JWT, and verifies the signature using the HS256 secret.

**HS256 verification:** The signature is re-computed using `HMAC-SHA256(header.payload, secret)` and compared against the signature in the token. If they match, the token is authentic and unmodified. Pure CPU — no database lookup, no network call.

**Why HS256 for this project:**
HS256 uses the same secret for signing and verification. For a single service where only the gateway signs and verifies tokens, this is appropriate. In a distributed system where multiple services need to independently verify tokens (without sharing a secret), RS256 (asymmetric) is preferred — the signer keeps the private key, verifiers only need the public key.

**Middleware ordering rationale:** Rate limiting before JWT verification is intentional. Verifying a JWT requires HMAC computation (CPU). Verifying a thousand forged tokens per second from one IP would waste CPU. Rate limiting rejects that IP at O(1) Map lookup before JWT verification ever runs.

---

### Stage 4: Header Injection (`preHandler`)

```javascript
fastify.addHook('preHandler', async (request, reply) => {
  request.headers['x-forwarded-for'] = request.ip;
  request.headers['x-gateway-timestamp'] = new Date().toISOString();
});
```

Two headers are injected into every upstream-bound request:
- `x-forwarded-for`: the real client IP (upstream services can log who actually made the request)
- `x-gateway-timestamp`: when the gateway received the request (useful for upstream latency analysis)

This runs after auth, so only authenticated requests reach this point.

---

### Stage 5: Route Handler + Retry Wrapper (`/api/*`)

```javascript
fastify.all('/api/*', async (request, reply) => {
  try {
    await withRetry(async (attempt) => {
      // All proxy logic lives inside this closure
    }, { maxAttempts: 3, delayMs: 200 });
  } catch (err) {
    reply.code(502).send({ error: 'Bad Gateway' });
  }
});
```

Everything inside `withRetry` can be retried up to 3 times. The `try/catch` outside `withRetry` is the last resort — if all 3 attempts fail, the client gets a 502.

---

### Stage 6: Load Balancer Pick + Request Transformation

```javascript
let server = lb.pick();     // → { url: 'http://127.0.0.1:3001', weight: 2, ... }
lb.acquire(server);         // → activeConnections++

const targetUrl = new URL(request.url, server.url);

const { options, requestId } = transformRequest(options, request);
// options.headers['x-request-id'] = 'nexus-<timestamp>-<random>'
// options.headers['x-gateway-version'] = '1.0.0'
// delete options.headers['x-internal-secret']
```

`transformRequest()` injects a unique request ID (`x-request-id`) for distributed tracing and strips internal headers that clients should never be able to forward upstream.

---

### Stage 7: OTel Child Span + Circuit Breaker + HTTP Call

```javascript
const upstreamSpan = tracer.startSpan('upstream.proxy', {
  kind: SpanKind.CLIENT,
  attributes: { 'http.url': targetUrl.href, 'peer.service': server.url },
}, request.otelCtx);

// Inject traceparent into outgoing headers
propagation.inject(request.otelCtx, outgoingHeaders);

const action = () => new Promise((resolve, reject) => {
  const proxyReq = http.request(transformedOptions, async (proxyRes) => {
    const transformed = await transformResponse(proxyRes, requestId);
    reply.send(transformed);
    upstreamSpan.end();
    lb.release(server);
    resolve();
  });
  proxyReq.on('error', (err) => {
    lb.release(server);
    reject(err);
  });
});

await breaker.fire(action);
```

The W3C `traceparent` header is injected into the outgoing request. If the upstream service (mock or real) also instruments with OTel, its spans appear as children of this trace in Jaeger — a single waterfall view of the cross-service request path.

`lb.release(server)` is called in both the success path and the error handler. This is critical — if the error handler didn't release, `activeConnections` would count down but never release on failure, eventually making every server look fully loaded.

---

### Stage 8: Response Transformation

```javascript
async function transformResponse(proxyRes, requestId) {
  const contentType = proxyRes.headers['content-type'] || '';
  if (!contentType.includes('application/json')) return null;  // ← passthrough non-JSON

  const raw = await readBody(proxyRes);
  const parsed = JSON.parse(raw);

  return JSON.stringify({
    data: parsed,
    _meta: { requestId, gateway: 'nexus', timestamp: new Date().toISOString() }
  });
}
```

JSON responses are wrapped. The original upstream response lives under `data`. `_meta` adds tracing context (request ID) and gateway provenance. Non-JSON responses (binary files, plain text) pass through unmodified.

**Why wrap responses?** It gives clients a consistent envelope format regardless of which upstream served them. It also provides a trace ID at the response level — the client can log `_meta.requestId` and correlate it with gateway logs.

---

### Stage 9: `onResponse` Hook — Telemetry + Observability

After the reply is sent, three things happen:

```javascript
fastify.addHook('onResponse', async (request, reply) => {
  // 1. Close OTel root span
  request.otelSpan.setAttribute('http.status_code', reply.statusCode);
  request.otelSpan.setStatus(reply.statusCode >= 500 ? { code: 2 } : { code: 1 });
  request.otelSpan.end();  // ← span exported async to Jaeger via BatchSpanProcessor

  // 2. Prometheus metrics
  httpRequestsTotal.inc({ method, route, status_code: reply.statusCode });
  httpRequestDurationMs.observe({ ... }, parseFloat(reply.elapsedTime));

  // 3. Kafka event
  emitEvent('gateway.request.completed', { method, url, statusCode, latencyMs, clientId });
});
```

**Why `onResponse` and not `onSend`?** `reply.elapsedTime` is only finalized after the response is sent. Using `onResponse` guarantees the latency measurement is complete.

**Fire-and-forget for Kafka:** `emitEvent()` is not awaited. The Kafka send happens asynchronously — it does not block the response. If the Kafka broker is slow or unavailable, the event is dropped with a warning. Observability must never degrade the primary request path.

---

### The Full Timing Picture

```
t=0ms     onRequest fires (OTel span started, rate check, JWT verify)
t=0.5ms   preHandler fires (headers injected)
t=1ms     handler fires, withRetry called, lb.pick(), transformRequest()
t=1.5ms   breaker.fire(), http.request() dispatched
t=6ms     upstream responds, transformResponse() completes, reply.send()
t=6.1ms   onResponse fires, span closed, metrics recorded, Kafka emitted

Client sees response at ~t=6ms
OTel span arrives in Jaeger at ~t=6.5ms (async BatchSpanProcessor)
Prometheus records metric at next scrape (~15s later)
Kafka consumer processes event within milliseconds
```

---

## 4. API Gateway Deep Dive

---

### 4.1 Fastify Lifecycle + Hook Ordering

Fastify processes every request through a defined sequence of lifecycle stages. Hooks let you attach logic at specific points. Understanding this ordering is essential — it determines which requests pay for which work.

```
Incoming Request
       │
       ▼
  ┌─────────────────────────────────────────────┐
  │  onRequest                                  │  ← earliest hook
  │    1. OTel span started                     │  ← must be first
  │    2. Rate limit check  ──► 429 if denied   │  ← O(1), kill early
  │    3. JWT verification  ──► 401 if invalid  │  ← CPU only, no I/O
  └─────────────────────────────────────────────┘
       │ (only authenticated, non-rate-limited requests continue)
       ▼
  ┌─────────────────────────────────────────────┐
  │  preParsing                                 │  ← Fastify parses body here
  └─────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────┐
  │  preHandler                                 │
  │    • Inject x-forwarded-for                 │
  │    • Inject x-gateway-timestamp             │
  └─────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────┐
  │  Route Handler  (/api/*, /auth/token, etc.) │
  │    • withRetry wrapper                      │
  │    • lb.pick() → transformRequest()         │
  │    • breaker.fire() → http.request()        │
  │    • transformResponse() → reply.send()     │
  └─────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────┐
  │  onSend                                     │  ← response being sent
  └─────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────┐
  │  onResponse                                 │  ← after response sent
  │    • reply.elapsedTime now final            │
  │    • OTel span closed                       │
  │    • Prometheus metrics recorded            │
  │    • Kafka event emitted                    │
  │    • Telemetry logged                       │
  └─────────────────────────────────────────────┘
```

**Key design principle:** Work that rejects a request should happen as early as possible. Expensive work (body parsing, upstream calls) should only happen for requests that will succeed.

```
Cost per rejected request:
  Rate limited at onRequest  →  ~0.38ms  (Map lookup + token math)
  Auth failed at onRequest   →  ~0.5ms   (HMAC-SHA256)
  Upstream timeout           →  ~5000ms  (network wait)
  
Moving rate limiting to preHandler would waste:
  body parsing time + route resolution time
  for every request that gets rejected anyway
```

---

### 4.2 Token Bucket Rate Limiter

#### The Mental Model

```
Each IP has a bucket:

  BUCKET_CAPACITY = 10 tokens
  REFILL_RATE     = 2 tokens/second

  ┌─────────────────────────┐
  │  ██ ██ ██ ██ ██ ██ ██  │  7 tokens remaining
  │  ██ ██ ██              │
  └─────────────────────────┘
  
  Request arrives → check if ≥1 token → consume 1 → allow
  No tokens left  → reject 429

  Time passes → tokens refill:
    elapsed = (now - lastRefill) / 1000 seconds
    tokens  = min(CAPACITY, tokens + elapsed × REFILL_RATE)
```

#### The Math

```javascript
function refillBucket(bucket) {
  const now = Date.now();
  const secondsElapsed = (now - bucket.lastRefill) / 1000;
  const tokensToAdd = secondsElapsed * REFILL_RATE;
  bucket.tokens = Math.min(BUCKET_CAPACITY, bucket.tokens + tokensToAdd);
  bucket.lastRefill = now;
}
```

**Worked example:**
```
t=0s:   bucket = 10 tokens (full)
t=0s:   10 rapid requests → 10 tokens consumed → 0 remaining
t=0s:   request 11 → 0 tokens → 429
t=0.5s: elapsed = 0.5s → tokensToAdd = 0.5 × 2 = 1 → tokens = 1
t=0.5s: request 12 → 1 token consumed → allowed
t=5s:   elapsed = 4.5s → tokensToAdd = 4.5 × 2 = 9 → tokens = min(10, 0+9) = 9
```

#### Why Token Bucket, Not Fixed Window

```
Fixed window problem:

  Window: 0s–1s  │  Window: 1s–2s
  ───────────────┼───────────────
      10 requests │ 10 requests
      at 0.99s   │ at 1.01s
  
  0.02 seconds apart, but counted as two separate windows.
  Client sends 20 requests in 20ms. Fixed window allows all 20.
  
Token bucket doesn't care about windows:
  It tracks actual elapsed time continuously.
  20 requests at 10 capacity → 10 allowed, 10 rejected.
  No gaming with timing.
```

#### Implementation Notes

```javascript
// rateLimiter.js
const buckets = new Map();  // ip → { tokens, lastRefill }

// New IP: bucket starts FULL (10 tokens)
// Means: a new visitor gets burst allowance immediately
// Production consideration: start at 0 or half-full to prevent
// burst-on-first-visit from new IPs in a DDoS scenario
```

**Limitation:** One `Map` per process. Two gateway instances have no shared state. A client hitting instance A and instance B can exceed the intended limit.

**Production fix:** Redis-backed rate limiter using `INCR` + `EXPIRE` per key, or a sliding window counter with `ZADD` + `ZREMRANGEBYSCORE`.

---

### 4.3 JWT Authentication (HS256)

#### Token Flow

```
Client                          Gateway
  │                               │
  ├─ POST /auth/token ────────────►│
  │  { client_id, client_secret }  │
  │                               │ validateClient() → Map lookup
  │                               │ jwtSign({ client_id }, { expiresIn: '1h' })
  │◄── { access_token: "eyJ..." } ─┤
  │                               │
  ├─ GET /api/products ───────────►│
  │  Authorization: Bearer eyJ...  │
  │                               │ jwtVerify() → HMAC-SHA256 check
  │                               │ → decode payload → req.user
  │◄── 200 + products data ────────┤
```

#### What HS256 Verification Does

```
Token structure:  header.payload.signature
                  (all base64url encoded)

Verification:
  1. Split token on "."
  2. Re-compute: HMAC-SHA256(header + "." + payload, secret)
  3. Compare result with signature in token
  4. If match → authentic + unmodified → allowed
  5. If mismatch → reject 401

Cost: ~microseconds (CPU hashing only, zero I/O)
```

#### HS256 vs RS256 — When Each Is Right

```
HS256 (Symmetric):
  Same secret → signs AND verifies
  ✅ Simple, fast
  ✅ Good when one service signs + verifies
  ❌ Everyone who verifies must know the secret
  ❌ Secret exposure = anyone can forge tokens
  
  Use when: single service, controlled environment
  ← Nexus uses this (appropriate for scope)

RS256 (Asymmetric):
  Private key → signs
  Public key  → verifies (can be distributed freely)
  ✅ Verifiers never know signing key
  ✅ Scales to distributed microservices
  ❌ More complex key management
  
  Use when: multiple services need independent verification
  ← Production microservices pattern
```

---

### 4.4 Request + Response Transformation

```
Incoming from client:
  GET /api/products
  Authorization: Bearer eyJ...
  x-internal-secret: admin123   ← dangerous internal header

After transformRequest():
  x-request-id: nexus-1716800000000-abc12    ← injected
  x-gateway-version: 1.0.0                   ← injected
  x-internal-secret: [DELETED]               ← stripped
  x-admin-override: [DELETED]                ← stripped

Upstream sees clean request with trace headers.

Upstream returns:
  { "products": [{ "id": 1, "name": "Laptop" }] }

After transformResponse():
  {
    "data": { "products": [{ "id": 1, "name": "Laptop" }] },
    "_meta": {
      "requestId": "nexus-1716800000000-abc12",
      "gateway": "nexus",
      "timestamp": "2026-05-26T10:00:00.000Z"
    }
  }
```

**Why strip internal headers?** A client could craft a request with `x-internal-secret` or `x-admin-override` trying to elevate privileges at the upstream. The gateway is the trust boundary — it ensures no client-supplied header reaches an upstream with a privileged name.

**Why wrap responses?** Consistent envelope for all clients regardless of upstream. The `requestId` in `_meta` links the response to a specific gateway log line and OTel trace.

---

### 4.5 Service Discovery

```
services.json on disk:
  {
    "services": [
      { "url": "http://127.0.0.1:3001", "weight": 2 },
      { "url": "http://127.0.0.1:3002", "weight": 1 }
    ]
  }

Gateway polls every 5 seconds:

  poll() runs:
    read + parse services.json
    diff current URLs vs lastKnown Set
    
    new URL found?    → lb.markHealthy(url)   → added to rotation
    URL disappeared?  → lb.markUnhealthy(url) → removed from rotation
    
    update lastKnown

  Result: live upstream pool with no gateway restart needed
```

**The 5-second tradeoff:**
- Too fast (e.g., 100ms): wasted I/O, file reads on every request
- Too slow (e.g., 60s): long window where a removed upstream still receives traffic
- 5 seconds: fast enough for manual ops changes to take effect quickly

**What it doesn't do:** Active health checking. If an upstream is in `services.json` but not responding, it stays in the pool. The circuit breaker detects the failure passively (on actual requests). Production systems combine both: service discovery for registration + active health probing for liveness.

---

## 5. Load Balancing + Resilience

> This section covers how Nexus routes traffic across multiple upstreams and prevents cascading failures. Three algorithms, a circuit breaker, and a retry engine — and crucially, how they compose together.

---

### 5.1 Load Balancing Algorithms

#### Why Load Balancing Exists
A single upstream server has finite capacity. Load balancing distributes incoming requests across a pool of servers so no single server becomes a bottleneck. In Nexus, the gateway maintains a pool (`servers` array in `LoadBalancer`) and `pick()` selects one server per request using the configured algorithm.

#### Algorithm 1: Round Robin

**What it does:** Cycles through servers in sequence. Request 1 → Server A, Request 2 → Server B, Request 3 → Server A, and so on.

**Internal state:** One integer cursor (`_rrCursor`). Increments on each call, wraps around with modulo.

```javascript
_roundRobin(pool) {
  this._rrCursor = (this._rrCursor + 1) % pool.length;
  return pool[this._rrCursor];
}
```

**Complexity:** O(1). No state beyond a counter. Stateless relative to the servers themselves.

**Tradeoff:** Ignores server load. If Server A is handling a 5-second database query and Server B is idle, round-robin still sends the next request to Server A.

**When it's appropriate:** Homogeneous servers with similar request processing times. Good default for most web workloads.

---

#### Algorithm 2: Least Connections

**What it does:** Routes each request to whichever server has the fewest currently active (in-flight) requests.

**Internal state:** `activeConnections` counter per server, updated by `acquire()` before the request and `release()` after it completes.

```javascript
_leastConnections(pool) {
  return pool.reduce((best, s) =>
    s.activeConnections < best.activeConnections ? s : best);
}

acquire(server) { server.activeConnections++; server.requestsHandled++; }
release(server) { if (server.activeConnections > 0) server.activeConnections--; }
```

**Complexity:** O(n) — scans all servers to find the minimum. Acceptable for small pools (2-10 servers).

**Why it's better than round-robin for variable workloads:** If Server A is handling a slow request, its `activeConnections` is higher. Least-connections routes new traffic to Server B automatically without any manual configuration.

**Critical implementation detail:** `release()` must be called in a `finally` block — not just on success. If the upstream call throws an error, you still need to decrement the counter. Nexus handles this via `lb.release(server)` in the `proxyReq.on('error')` handler.

---

#### Algorithm 3: Weighted Round Robin (WRR)

**What it does:** Distributes traffic in proportion to server weights. A server with weight 2 receives twice as many requests as a server with weight 1.

**The GCD Reduction — Why It Matters:**

Suppose your servers have weights `[4, 2, 2]`. A naive approach builds a sequence by repeating each server proportionally:
```
[A, A, A, A, B, B, C, C]  ← length 8
```

But `4:2:2` reduces to `2:1:1`. The GCD of `{4, 2, 2}` is 2. Dividing all weights by GCD gives `[2, 1, 1]`:
```
[A, A, B, C]  ← length 4, same ratio
```

Same distribution, half the array size. For large weights (e.g., `100:50:50`), this matters considerably.

**Why it mathematically preserves fairness:**
Dividing all weights by their GCD scales them down uniformly. The *ratio* between weights is unchanged — `4:2:2 = 2:1:1`. The sequence built from the reduced weights has the same proportional representation as the original weights.

```javascript
_buildWRRSequence() {
  const healthy = this.servers.filter(s => s.healthy);
  const g = healthy.reduce((acc, s) => gcd(acc, s.weight), 0);  // find GCD
  const seq = [];
  for (const s of healthy) {
    for (let i = 0; i < s.weight / g; i++) seq.push(s.url);     // repeat reduced times
  }
  return seq;
}
```

**Nexus configuration:**
```javascript
// server.js
const lb = new LoadBalancer(
  [
    { url: 'http://127.0.0.1:3001', weight: 2 },
    { url: 'http://127.0.0.1:3002', weight: 1 },
  ],
  'round-robin'
);
```
GCD(2,1) = 1, so no reduction needed. Sequence: `[3001, 3001, 3002]`.

**WRR Tradeoff:** The sequence is static — built once and cycled. If a server goes down mid-sequence, the WRR sequence is rebuilt (via `_buildWRRSequence()` in `markUnhealthy()`). But WRR still doesn't account for real-time load variation like least-connections does.

---

### 5.2 Circuit Breaker

#### What It Solves
Without a circuit breaker: upstream Server A is down. Every request waits 5 seconds (timeout) before failing. With 100 concurrent requests, that's 100 × 5 seconds of blocked threads/event loop resources, all ending in failure.

The circuit breaker detects repeated failures and **fast-fails** subsequent requests immediately — no waiting, no wasted resources — until the upstream shows signs of recovery.

#### The Three-State Machine

```
         failures exceed threshold
  CLOSED ──────────────────────────► OPEN
    ▲                                  │
    │  test request succeeds           │ resetTimeout elapses
    └──────── HALF-OPEN ◄──────────────┘
                  │
                  │ test request fails
                  ▼
                OPEN (reset timer restarted)
```

- **CLOSED** — Normal operation. Requests flow through. Failures are counted.
- **OPEN** — Breaker has tripped. All requests fast-fail with an error immediately (no upstream call made).
- **HALF-OPEN** — `resetTimeout` has passed. One test request is allowed through. If it succeeds, the breaker closes. If it fails, it reopens.

#### Nexus Configuration

```javascript
const BREAKER_OPTIONS = {
  timeout: 5000,                  // upstream must respond within 5s
  errorThresholdPercentage: 50,   // trip if 50%+ of requests fail
  resetTimeout: 10000,            // stay OPEN for 10s before trying HALF-OPEN
  volumeThreshold: 3,             // need at least 3 requests before tripping
};
```

`volumeThreshold: 3` prevents the breaker from tripping on the very first request. If 1 request fails, that's 100% failure rate — but you'd be tripping the breaker too aggressively without sufficient sample size.

#### Per-Upstream Isolation — Why This Design Matters

```javascript
// circuit-breaker.js
const breakers = new Map();  // url → CircuitBreaker instance

function getBreaker(url) {
  if (!breakers.has(url)) {
    const breaker = new CircuitBreaker((fn) => fn(), BREAKER_OPTIONS);
    breakers.set(url, breaker);
  }
  return breakers.get(url);
}
```

One breaker per upstream URL. If Server 3002 goes down, its circuit opens. Server 3001 continues operating normally. A single shared breaker would trip because of 3002 and cut off 3001 — the opposite of fault isolation.

**Interview answer:** *"Isolating circuit breakers per upstream ensures a failure in one upstream doesn't affect traffic to healthy upstreams. A shared breaker would create correlated failures — one bad server taking down all healthy ones."*

#### How `breaker.fire(action)` Works
The opossum circuit breaker wraps an identity function `(fn) => fn()`. The actual action is passed at `.fire()` time:

```javascript
const action = () => new Promise((resolve, reject) => {
  // ... http.request to upstream
});

const breaker = getBreaker(server.url);
await breaker.fire(action);  // ← action passed here, not at breaker creation
```

**Why action at `fire()` time, not at breaker creation?**
The upstream server URL changes per request (load balancer picks it). If you passed the action at creation time, it would be a stale closure pointing to whatever server was selected when the breaker was instantiated — not the current request's server. Passing the action at `fire()` time ensures the action always uses the current request context.

> **💡 Real Learning**
> This was one of the hardest bugs to debug in Phase 3. The circuit breaker was firing the wrong upstream action — always using the first server picked when the breaker was created. The fix was understanding that opossum's identity function `(fn) => fn()` means "execute whatever function you pass at fire time." The breaker wraps the *mechanism* (state machine, timeout, stats), not the *action* itself.

---

### 5.3 Retry Engine

#### What It Does
When an upstream request fails, the retry engine attempts the request again on a freshly picked server before returning an error to the client.

```javascript
// retry.js
async function withRetry(action, { maxAttempts = 3, delayMs = 200, onRetry } = {}) {
  let lastErr;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await action(attempt);
    } catch (err) {
      lastErr = err;
      if (onRetry) onRetry(attempt, err);
      if (attempt < maxAttempts) await sleep(delayMs);
    }
  }
  throw lastErr;  // ← re-throw after all attempts exhausted
}
```

Key points:
- **`action(attempt)`** — the attempt number is passed in, allowing the action to log retry context
- **`await sleep(delayMs)`** — 200ms delay between attempts (not after the last attempt)
- **`throw lastErr`** — after exhausting all attempts, throws the last error so the caller handles the 502

#### Why Each Retry Picks a Fresh Server

```javascript
// server.js — inside withRetry callback
await withRetry(async (attempt) => {
  let server = lb.pick();   // ← fresh pick on EVERY attempt
  lb.acquire(server);
  // ... proxy request to server
});
```

Calling `lb.pick()` fresh each time means the retry naturally lands on a different server. The retry and load balancer compose without explicit coordination — the LB's job is to pick the best available server, and it does that regardless of retry context.

If the same server were retried, you'd be hammering a server that just failed. Calling `lb.pick()` again lets the LB route away from it (especially if the circuit breaker has opened for that server).

---

### 5.4 How Retry + Circuit Breaker Compose

This is the interaction that most interviewers miss when they ask about this — and that you should be prepared to explain.

#### The Happy-ish Path (One Failure, Recovery)

```
Attempt 1:
  lb.pick() → Server 3002
  breaker(3002).fire(action)
  → upstream call fails (3002 down)
  → circuit breaker records failure
  → throw error

  withRetry catches error, waits 200ms

Attempt 2:
  lb.pick() → Server 3001 (round-robin advances)
  breaker(3001).fire(action)
  → upstream call succeeds
  → client receives 200 OK

Result: Client sees success. 3002 failure is invisible.
```

#### The Full Failure Path (All Retries Exhausted)

```
Attempt 1: 3002 → fails
Attempt 2: 3001 → fails  
Attempt 3: 3002 → circuit now OPEN, fast-fails immediately (no network call)

withRetry exhausts maxAttempts → throws lastErr
server.js catches → reply.code(502).send({ error: 'Bad Gateway' })
```

#### The Circuit Breaker Fast-Fail Under Retry

After 3002 has failed enough times to trip its breaker, `breaker.fire(action)` throws **immediately** without making a network call. This matters under retry because it means attempt 3 on a broken server costs ~0ms rather than another 5-second timeout. The retry delay (200ms) dominates, not the upstream timeout (5000ms).

#### The Fallback — What It Does

```javascript
breaker.fallback(() => {
  emitEvent('gateway.circuit.opened', { upstream: server.url, ... });
  circuitBreakerOpensTotal.inc({ upstream: server.url });
  lb.release(server);
  throw new Error(`Circuit open for ${server.url}`);  // ← must throw, not return
});
```

**Why the fallback must throw, not return a response:**
If the fallback returns a value, opossum treats the circuit breaker call as successful and the error is swallowed. The retry engine never sees it. `withRetry` doesn't retry. The client gets whatever the fallback returned instead of trying another upstream.

Throwing ensures the error propagates up through `breaker.fire()`, gets caught by `withRetry`, and triggers the next attempt.

> **💡 Real Learning**
> This was the second hardest bug in Phase 3. The fallback was returning a reply instead of throwing. The circuit breaker was silently eating the error, `withRetry` thought the request succeeded, and the client received whatever the fallback returned. The fix required understanding that opossum's fallback determines the *circuit-open response path* — and that throwing from a fallback is intentional and correct.

> **⚠️ Honest Limitation**
> The retry engine does not implement exponential backoff. A fixed 200ms delay between retries is simpler but less sophisticated than production retry strategies. Under high failure rates, fixed-delay retries can create synchronized retry storms — all failed requests retrying at the same time. Exponential backoff with jitter (randomized delay) is the production-grade approach used by AWS SDKs, Google Cloud clients, etc.

---

## 6. Intelligent Cache Proxy

> This is the most technically unique component in Nexus. It operates at the protocol level, not the application level. Understanding it deeply is what separates this project from typical CRUD portfolio work.

---

### 6.1 Why TCP? Why Not HTTP?

#### Basic Definition
The cache proxy is a TCP server that intercepts Redis commands at the wire protocol level — before they reach the actual Redis instance.

#### The Core Reasoning
Redis does not speak HTTP. Redis speaks **RESP** (REdis Serialization Protocol) over raw TCP sockets. When your application does `client.get("key")`, ioredis serializes that into a RESP-formatted byte sequence and sends it over TCP to port 6379. The bytes look like this on the wire:

```
*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
```

If you want to intercept that transparently — without the application knowing — you need to sit at the same layer. You need to speak the same language. An HTTP proxy can't do this. You need a raw TCP server that reads those bytes, understands the protocol, and either responds from cache or forwards to real Redis.

#### Why This Matters for the Interview
This is the answer to: *"Why didn't you just add a caching layer in your API gateway?"*

The gateway operates at HTTP. The cache proxy operates at the Redis protocol level. They serve different layers of the stack. A Redis proxy is transparent to the application — any Redis client (ioredis, redis-py, Jedis) can point at port 6380 instead of 6379 without changing a single line of application code. That's the architectural value.

#### What Node.js Gives You
Node's `net` module exposes raw TCP. `net.createServer()` gives you a socket — a stream of bytes with no framing, no message boundaries, no format assumptions. You own the entire parsing responsibility. That's why a custom RESP parser is necessary.

```
Application
    │  ioredis → serializes to RESP bytes
    ▼
port 6380 ← Cache Proxy (TCP server, speaks RESP)
    │  cache miss → forward bytes to real Redis
    ▼
port 6379 ← Real Redis
```

> **💡 Real Learning**
> The hardest mental shift was understanding that TCP is just a stream of bytes — there's no concept of "one message arrived." `SET foo bar` might arrive as three separate `data` events: `*3\r\n`, `$3\r\nSET\r\n`, `$3\r\nfoo\r\n$3\r\nbar\r\n`. The parser has to buffer and accumulate until it has a complete command. This is packet fragmentation, and it's why a stateful parser is non-negotiable.

---

### 6.2 RESP Protocol Internals

#### What RESP Is
RESP is the serialization protocol Redis uses for all client-server communication. It is simple, fast, and binary-safe. Every message starts with a single byte that identifies its type.

#### The 5 RESP Types

| First Byte | Type | Example | Used For |
|---|---|---|---|
| `+` | Simple String | `+OK\r\n` | Success responses |
| `-` | Error | `-ERR unknown command\r\n` | Error responses |
| `:` | Integer | `:1000\r\n` | Counts, TTLs, boolean-like results |
| `$` | Bulk String | `$6\r\nfoobar\r\n` | Keys, values, command arguments |
| `*` | Array | `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n` | Commands sent from client to Redis |

#### Why Commands Are Arrays
When your application sends `GET foo`, it doesn't send `GET foo` as a plain string. It sends an **array of bulk strings**:

```
*2\r\n       ← array of 2 elements
$3\r\n       ← first element is 3 bytes long
GET\r\n      ← first element value
$3\r\n       ← second element is 3 bytes long
foo\r\n      ← second element value
```

Every Redis command is an array. The proxy receives this, parses out the array, reads `args[0]` to identify the command (`GET`), and can then make intelligent decisions — cache it? forward it? apply a TTL?

#### Why Bulk Strings Are Read by Byte Count (Binary Safety)
Simple strings stop at `\r\n`. But a **bulk string** tells you its length first: `$6\r\nfoobar\r\n`. The parser reads exactly 6 bytes of data, then consumes the trailing `\r\n`. It never scans for `\r\n` inside the value.

This is called **binary safety**. If your cached value is a JSON blob containing `\r\n` characters, a length-unaware parser would terminate early and corrupt the data. RESP's bulk string format makes this impossible.

```javascript
// From parser.js — binary-safe bulk string parsing
case '$': {
  const len = parseInt(line, 10);
  const dataEnd = afterLine + len;       // ← byte count, not newline scan
  if (this._buf.length < dataEnd + 2) return INCOMPLETE;
  const data = this._buf.slice(afterLine, dataEnd).toString('utf8');
  return { value: { type: 'bulk_string', value: data }, bytesConsumed: dataEnd + 2 - offset };
}
```

---

### 6.3 Streaming Parser Internals

#### The Core Problem
Node.js fires a `data` event every time bytes arrive on a TCP socket. TCP makes no guarantees about how many bytes arrive per event. One `SET foo bar` command could arrive in one chunk or five. The parser must handle both cases correctly.

#### How the Parser Solves This: Internal Buffer

```javascript
// From resp/parser.js
constructor() {
  super();
  this._buf = Buffer.alloc(0);  // ← accumulation buffer
}

feed(chunk) {
  this._buf = Buffer.concat([this._buf, chunk]);  // ← append new bytes
  while (this._buf.length > 0) {
    const result = this._parse(0);
    if (result === INCOMPLETE) break;             // ← not enough bytes yet, stop
    const { value, bytesConsumed } = result;
    this._buf = this._buf.slice(bytesConsumed);   // ← consume processed bytes
    this.emit('command', value);                  // ← emit complete parsed command
  }
}
```

**The lifecycle of one command:**

```
data event fires → feed(chunk) called
  → append chunk to internal buffer
  → attempt _parse(0)
    → if INCOMPLETE: wait for next data event
    → if complete: emit 'command', slice consumed bytes off buffer
  → attempt _parse(0) again (might be another command in buffer)
  → repeat until buffer empty or INCOMPLETE
```

#### The `INCOMPLETE` Sentinel
Instead of throwing an error or returning null, the parser returns a special `Symbol('INCOMPLETE')`. Symbols are unique values — they can never accidentally equal anything else. This is a defensive engineering choice: it prevents any accidental truthy/falsy confusion when checking whether a parse succeeded.

```javascript
const INCOMPLETE = Symbol('INCOMPLETE');
// ...
if (this._buf.length <= offset) return INCOMPLETE;
```

#### Nested Parsing for Arrays
Arrays are parsed recursively. When the parser encounters `*3\r\n`, it reads the count (3), then calls `_parse()` recursively three times, passing the updated offset each time. Each recursive call returns how many bytes it consumed, so the outer call can advance the offset correctly.

```javascript
case '*': {
  const count = parseInt(line, 10);
  let currentOffset = afterLine;
  for (let i = 0; i < count; i++) {
    const el = this._parse(currentOffset);         // ← recursive
    if (el === INCOMPLETE) return INCOMPLETE;
    elements.push(el.value);
    currentOffset += el.bytesConsumed;             // ← advance offset
  }
  return { value: { type: 'array', value: elements }, bytesConsumed: currentOffset - offset };
}
```

#### Why EventEmitter?
The parser extends `EventEmitter` and emits `'command'` events. This decouples parsing from handling. The TCP server registers a listener: `parser.on('command', async (parsed) => { ... })`. The parser doesn't need to know anything about what happens to commands after they're parsed. Clean separation of concerns.

> **💡 Real Learning**
> Initially the hardest part was understanding *why* you can't just process each `data` event independently. The mental model that finally clicked: TCP is like water flowing through a pipe. You can't assume packets are aligned with messages. You have to fill a bucket (the buffer), check if you have a complete unit of work, process it, and wait for more water if you don't.

---

### 6.4 RESP Serializer

The serializer is the reverse of the parser — it takes a JavaScript object and produces RESP-formatted bytes to write back to the client.

```javascript
// From resp/serializer.js
function serialize(value) {
  switch (value.type) {
    case 'simple_string': return `+${value.value}\r\n`;
    case 'error':         return `-${value.value}\r\n`;
    case 'integer':       return `:${value.value}\r\n`;
    case 'bulk_string':
      if (value.value === null) return '$-1\r\n';   // ← null bulk string = Redis nil
      const bytes = Buffer.byteLength(value.value, 'utf8');
      return `$${bytes}\r\n${value.value}\r\n`;     // ← length-prefixed
    case 'array':
      if (value.value === null) return '*-1\r\n';
      return `*${value.value.length}\r\n${value.value.map(serialize).join('')}`;
  }
}
```

**Key detail:** `Buffer.byteLength(value, 'utf8')` — not `value.length`. String `.length` counts JavaScript characters (UTF-16 code units). `Buffer.byteLength` counts actual bytes in UTF-8 encoding. For ASCII they're identical, but for multi-byte characters (emoji, Chinese characters, etc.) they differ. Using `.length` would produce incorrect byte counts and corrupt the RESP framing.

---

### 6.5 Singleflight Request Deduplication

#### The Problem It Solves: Thundering Herd
Imagine a cache key `product:homepage` expires at 12:00:00. At exactly that moment, 500 concurrent requests arrive, all asking for the same key. Without singleflight:

```
500 requests → cache miss → 500 Redis calls → Redis overwhelmed
```

With singleflight:
```
500 requests → cache miss
  Request #1 → fires Redis call, stores promise in Map
  Requests #2-500 → find existing promise in Map, await it
  → 1 Redis call → 500 responses
```

#### Implementation Deep Dive

```javascript
// From singleflight.js
class Singleflight {
  constructor() {
    this._inflight = new Map();   // key → Promise
  }

  async do(key, fn) {
    if (this._inflight.has(key)) {
      return this._inflight.get(key);    // ← reuse existing promise
    }

    const promise = fn().finally(() => {
      this._inflight.delete(key);        // ← cleanup regardless of success/failure
    });

    this._inflight.set(key, promise);
    return promise;
  }
}
```

#### The Lifecycle, Step by Step

```
Request A arrives for key "product:1":
  → Map has no entry for "product:1"
  → fn() is called (e.g., Redis GET)
  → fn() returns a Promise (not yet resolved)
  → .finally() is chained to clean up after resolution
  → Promise stored in Map: { "product:1" → <pending Promise> }
  → Request A awaits the Promise

Request B arrives for key "product:1" (fn() still in flight):
  → Map HAS entry for "product:1"
  → Returns the SAME pending Promise
  → Request B awaits the SAME Promise

Redis responds:
  → Promise resolves with value
  → Request A gets value
  → Request B gets value (same Promise, same resolution)
  → .finally() runs → Map entry deleted

Request C arrives for key "product:1" (after resolution):
  → Map has no entry (deleted)
  → Starts fresh: calls fn() again
```

#### Why `.finally()` and Not `.then()`
`.then()` only runs on success. If the Redis call throws an error, `.then()` doesn't execute — the Map entry stays forever. Every future request for that key would receive the same rejected Promise, permanently broken.

`.finally()` runs on **both success and failure**. The Map entry is always cleaned up. If the call failed, the next request will try again fresh.

This is a subtle but important correctness detail.

#### Edge Cases + Honest Limitations

| Concern | Reality |
|---|---|
| Memory leak | Entries are always cleaned up by `.finally()`. No leak under normal operation. |
| Error propagation | All waiters receive the same rejection if `fn()` throws. This is correct — they all asked for the same thing at the same time. |
| Stale data | All waiters get the same result. If the result is slightly stale by the time later waiters receive it, that's an acceptable tradeoff for dramatically reduced backend pressure. |
| **Single process only** | **This only works within one Node.js process. Two gateway instances running in parallel each have their own `_inflight` Map. The thundering herd problem returns under horizontal scaling.** |

> **⚠️ Current State**
> Singleflight is fully implemented and tested in isolation (`test-singleflight.js` confirms 1 Redis call for 3 concurrent requests). It is not yet wired into the live TCP request path in `server.js`. See Section 6.9 for the full integration picture.

> **💡 Real Learning**
> The `.finally()` vs `.then()` distinction wasn't obvious at first. The key insight: in JavaScript, a Promise represents a future value — positive or negative. You're not storing "the result," you're storing "the work in progress." `.finally()` is the right hook because cleanup should happen regardless of outcome.

---

### 6.6 YAML Policy Engine

#### What It Does
The policy engine lets you configure caching behavior per Redis command without changing code. It reads `policy.yaml` and returns the caching rule for a given command:

```yaml
policies:
  - match: GET
    ttl: 60
    enabled: true
  - match: SET
    ttl: 0
    enabled: false
```

```javascript
// From policy.js
function getPolicy(command) {
  const { policies } = loadPolicy();          // ← reads + parses YAML file
  const match = policies.find(
    (p) => p.match.toUpperCase() === command.toUpperCase()
  );
  return match || { enabled: false, ttl: 0 }; // ← default: no caching
}
```

#### Design Value
Separating cache policy from code is a real production pattern. At companies like Cloudflare or Fastly, cache rules are often declarative configs that ops teams can change without a code deploy. The YAML approach is a simplified version of that principle.

#### Known Implementation Issues (Be Honest About These)

**Issue 1: File read on every call**

`loadPolicy()` is called inside `getPolicy()`. Every time any command is processed, the YAML file is read from disk and parsed. Under high traffic this becomes a real performance concern.

```javascript
// Current: reads disk on every call
function getPolicy(command) {
  const { policies } = loadPolicy();   // ← fs.readFileSync() + yaml.load() every time
```

**Fix:** Load the policy once at startup, cache it in memory. Optionally watch the file for changes using `fs.watch()` to support hot-reloading.

```javascript
// Better approach
let cachedPolicy = loadPolicy();
fs.watch(policyFile, () => { cachedPolicy = loadPolicy(); });

function getPolicy(command) {
  const { policies } = cachedPolicy;   // ← in-memory, no disk I/O
```

**Issue 2: O(n) lookup**

`policies.find()` scans the array linearly. For 2 policies this is irrelevant. For a production system with 50+ command rules, a Map keyed by command name would be O(1).

```javascript
// Better: Map for O(1) lookup
const policyMap = new Map(policies.map(p => [p.match.toUpperCase(), p]));
function getPolicy(cmd) { return policyMap.get(cmd.toUpperCase()) || { enabled: false, ttl: 0 }; }
```

> **Interview Frame:** These are implementation optimizations that a production system would require. Identifying them demonstrates systems thinking — you understand the difference between "works correctly" and "works at scale."

---

### 6.7 Tag-Based Cache Invalidation + Lua

#### The Problem
Suppose you cache `product:1`, `product:2`, `product:3` separately. A product update comes in. You need to invalidate all product-related keys. How do you know which keys to delete?

Option 1: Delete by key name pattern (e.g., `SCAN` for `product:*`) — slow, requires multiple round-trips, not atomic.

Option 2: Maintain a **tag set** — a Redis Set that tracks which keys belong to a logical group. Invalidate the tag, delete everything in it atomically.

#### How Tags Work in Nexus

```javascript
// Tag a key as belonging to the "products" group
await tagKey('products', 'product:1');
// Internally: SADD tag:products product:1

// Later, when product data changes:
await invalidateTag('products');
// Internally: run Lua script on tag:products
```

#### Why Lua — The Atomicity Argument

This is the most important concept to understand and defend.

Without Lua:
```
Node.js:
  1. SMEMBERS tag:products        → returns ["product:1", "product:2"]
  ← network round-trip ←
  2. DEL product:1
  ← network round-trip ←
  3. DEL product:2
  ← network round-trip ←
  4. DEL tag:products
```

If another request reads `product:1` between steps 1 and 2, it gets stale data. If the Node.js process crashes between steps 2 and 4, the tag set still exists pointing at already-deleted keys — inconsistent state forever.

With Lua:
```lua
-- invalidate.lua
local keys = redis.call('SMEMBERS', KEYS[1])
for _, key in ipairs(keys) do
  redis.call('DEL', key)
end
redis.call('DEL', KEYS[1])
return #keys
```

Redis executes Lua scripts **atomically**. The entire script runs as a single unit. No other Redis command from any other client can execute between the `SMEMBERS` and the `DEL` calls. Partial invalidation is impossible.

**The framing:** Lua scripts are Redis's version of a database transaction. You use them when you need multiple operations to behave as one.

#### What the Lua Script Actually Does, Line by Line

```lua
local keys = redis.call('SMEMBERS', KEYS[1])
-- KEYS[1] = "tag:products"
-- returns: ["product:1", "product:2"]

for _, key in ipairs(keys) do
  redis.call('DEL', key)
  -- deletes product:1, then product:2
end

redis.call('DEL', KEYS[1])
-- deletes the tag set itself

return #keys
-- returns the number of keys deleted (for logging)
```

The Node.js call:
```javascript
const deleted = await redisClient.eval(script, 1, `tag:${tag}`);
// eval(script, numkeys, key1, key2, ...)
// 1 = number of KEYS arguments
// `tag:${tag}` = KEYS[1] inside Lua
```

---

### 6.8 Hot Key Detection + TTL Extension

#### The Problem It Solves
A "hot key" is a cache key that receives disproportionately high traffic. The irony: the more popular a key is, the harder it hurts when it expires. A homepage cache key hit 10,000 times/second that expires during peak traffic causes a thundering herd.

The solution: detect keys that are getting hammered and automatically extend their TTL. Keep hot data warm exactly when it matters most.

#### Implementation

```javascript
// From hotkey.js
const HOT_THRESHOLD = 10;    // hit count before key is "hot"
const TTL_EXTENSION = 120;   // extra seconds added to TTL

const hitCounts = new Map();  // key → hit count

async function checkAndExtend(key) {
  const count = trackHit(key);

  if (count >= HOT_THRESHOLD) {
    const ttl = await redisClient.ttl(key);
    if (ttl > 0) {                                          // ← only if key has a TTL
      await redisClient.expire(key, ttl + TTL_EXTENSION);  // ← extend, don't reset
      console.log(`[hotkey] ${key} is hot (${count} hits) — TTL extended`);
    }
  }
}
```

**Why `ttl > 0` check?**
- `ttl = -1` means the key has no expiry (persistent key). Extending a persistent key doesn't make sense.
- `ttl = -2` means the key doesn't exist. Nothing to extend.
- `ttl > 0` means the key exists and has an expiry — the only case where extension is meaningful.

**Why `ttl + TTL_EXTENSION` instead of setting a fixed new TTL?**
Setting a fixed TTL (e.g., always `expire key 180`) resets the clock completely. If the key was about to expire in 1 second, it jumps to 180 seconds regardless. Adding to the existing TTL preserves the original expiry intent while extending it proportionally.

#### Known Limitation: Memory Growth

`hitCounts` is a `Map` that grows indefinitely. Every unique key that passes through the proxy adds an entry and that entry never gets removed. In a long-running process handling thousands of unique keys, this becomes a memory leak.

**Production fix:** Use a fixed-size LRU cache (evicts least-recently-used entries) or use Redis itself to store hit counts with an expiry — so the tracking data expires along with the cached data.

> **⚠️ Current State**
> The hot key module is implemented and tested in isolation (`test-hotkey.js` verifies TTL extension after 10 hits). Not yet wired into the live TCP request path. Same integration gap as singleflight — see Section 6.9.

---

### 6.9 Current State vs Intended Design

> This section is the most important for interview honesty. Understanding the gap between architecture and implementation is itself a sign of engineering maturity.

#### What the TCP Server Currently Does

Looking at `nexus-cache-proxy/server.js`:

```javascript
parser.on('command', async (parsed) => {
  const args = parsed.value.map((el) => el.value);
  const [command, ...rest] = args;

  const result = await redisClient.call(command, ...rest);  // ← raw forward to Redis
  // ... serialize response and write back
});
```

**Every command is forwarded directly to Redis.** The policy engine, singleflight, and hot key detector are not called here. The proxy is currently a **transparent pass-through** — it speaks RESP and correctly proxies all commands, but the intelligent caching layer is not yet wired in.

#### What Was Built and Tested

All intelligent modules exist as complete, tested units:

| Module | Status |
|---|---|
| RESP Parser | ✅ Complete, integrated |
| RESP Serializer | ✅ Complete, integrated |
| TCP Server (pass-through) | ✅ Complete, working |
| Policy Engine | ✅ Complete, tested in isolation |
| Singleflight | ✅ Complete, tested in isolation |
| Tag Invalidation + Lua | ✅ Complete, tested in isolation |
| Hot Key Detection | ✅ Complete, tested in isolation |
| **Full integration into TCP path** | ⚠️ Not yet complete |

#### Why This Architecture Was Chosen
Building and validating each module independently first is a legitimate engineering approach. It's easier to verify correctness of the singleflight logic in isolation than to debug it when it's embedded in a TCP handler. Each module has its own test file precisely because of this.

#### What Full Integration Would Look Like

The `server.js` command handler would become:

```javascript
parser.on('command', async (parsed) => {
  const args = parsed.value.map((el) => el.value);
  const [command, ...rest] = args;

  // Step 1: Check policy — should we cache this command?
  const policy = getPolicy(command);

  if (policy.enabled && command.toUpperCase() === 'GET') {
    const key = rest[0];

    // Step 2: Try cache first
    const cached = await redisClient.get(key);
    if (cached !== null) {
      // Step 3: Track hot key on cache hit
      await checkAndExtend(key);
      socket.write(serialize({ type: 'bulk_string', value: cached }));
      return;
    }

    // Step 4: Cache miss — use singleflight to prevent thundering herd
    const result = await singleflight.do(key, () => redisClient.call(command, ...rest));

    // Step 5: Write to cache with TTL from policy
    if (result !== null) {
      await redisClient.set(key, result, 'EX', policy.ttl);
    }

    socket.write(serialize({ type: 'bulk_string', value: result }));
  } else {
    // Non-cacheable command — forward directly
    const result = await redisClient.call(command, ...rest);
    socket.write(serialize(/* ... */));
  }
});
```

#### How to Frame This in Interviews

*"I deliberately built and validated each intelligent module independently — policy engine, singleflight, tag invalidation, hot key tracking — before wiring them into the live request path. The integration is the next engineering step. This approach let me verify the correctness of each component in isolation before reasoning about their interaction in a concurrent TCP environment."*

This framing is honest, shows good engineering judgment, and turns what could be a weakness into evidence of disciplined development practice.

#### The `connect()` Consistency Issue

There is a minor implementation inconsistency in `nexus-cache-proxy/index.js`:

```javascript
async function start() {
  await redisClient.connect();  // ← explicit connect call
```

But `redis-client.js` creates an ioredis instance without `lazyConnect: true`:

```javascript
const client = new Redis({ host: '127.0.0.1', port: 6379 });
// ioredis auto-connects immediately on instantiation
```

ioredis without `lazyConnect` starts connecting the moment `new Redis()` is called. The explicit `redisClient.connect()` call in `start()` is redundant — and depending on timing, could throw because a connection attempt is already in progress.

**Why it may have worked anyway:** ioredis is fairly tolerant of duplicate connect calls in some versions and may silently ignore them if a connection is already established. But this is undefined behavior relying on library internals.

**Fix:** Either add `lazyConnect: true` to the Redis constructor (deferring connection until `.connect()` is explicitly called), or remove the `await redisClient.connect()` line from `index.js` and let ioredis auto-connect.

This is worth knowing not because it breaks the project, but because it demonstrates understanding of library connection lifecycle — a real engineering detail.

---

## 7. Event-Driven Architecture

---

### 7.1 The Core Idea: Decouple Observation from Action

```
WITHOUT Kafka (inline handling):

  Request arrives
       │
       ▼
  Rate limit check → 429
       │
       ├──► IP blocking logic (in-process)
       ├──► Alert sending (blocks if alert service is slow)
       ├──► Anomaly detection (CPU in request path)
       │
  Response sent
  
  Problems:
    • Alert service slow? Request is slow.
    • IP block logic crashes? Affects gateway.
    • Want a new consumer? Redeploy the gateway.


WITH Kafka (event-driven):

  Request arrives
       │
       ▼
  Rate limit check → 429
       │
       ├──► emit('gateway.rate_limit.hit', { ip, ... })  ← fire-and-forget
       │     └─ non-blocking, <1ms
       │
  Response sent
  
  Separately, asynchronously, in another process:
  Consumer reads event → IP blocking → alerts → anomaly detection
  
  Benefits:
    • Gateway never blocks on consumer work
    • Consumer can be restarted without touching gateway
    • Multiple consumers can read same events independently
    • Events survive process crashes (stored in Kafka)
```

---

### 7.2 Kafka Concepts (What You Need to Know)

```
Kafka is a distributed commit log, not a message queue.

Traditional queue:         Kafka:
  Producer ──► Queue        Producer ──► Topic (partitioned log)
  Consumer reads             Consumer A reads (offset 0→N)
  Message deleted            Consumer B reads (offset 0→N)
                             Messages retained (configurable window)
                             
Key difference:
  Queue: consumed once, deleted
  Kafka: offset-tracked, can be replayed, multiple consumers
```

```
Topic structure:

  Topic: gateway.request.completed
  ┌──────────────────────────────────────────┐
  │  Partition 0                             │
  │  [msg0][msg1][msg2][msg3][msg4]...       │
  │        ↑                                │
  │    consumer offset                      │
  │    (consumer remembers where it left)   │
  └──────────────────────────────────────────┘
  
  Consumer crash + restart:
    → resume from last committed offset
    → no events lost
    → may reprocess some events (at-least-once delivery)
```

---

### 7.3 Producer Design

```javascript
// kafka/producer.js — key design decisions

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
  allowAutoTopicCreation: true,  // ← topics created if not exist
});

// Single shared instance — NOT created per request
// KafkaJS producers hold an open TCP connection to broker
// Creating per-request would exhaust broker connection limits
```

```
emitEvent() flow:

  gateway calls emitEvent('gateway.rate_limit.hit', { ip, ... })
       │
       ▼
  is producer connected?
       ├─ NO  → log warning, return (drop event)
       └─ YES → producer.send({ topic, messages: [{ key, value }] })
                    │
                    └─ NOT awaited in onResponse hook
                       fire-and-forget
                       gateway continues immediately
```

**Why fire-and-forget (not `await`):**
```
Gateway's job: proxy requests.
Kafka's job:   carry observability events.

If Kafka broker is under load:
  await producer.send() → blocks onResponse hook
  → every request waits for Kafka acknowledgment
  → Kafka slowness becomes user-facing latency
  
Fire-and-forget:
  emitEvent() returns immediately
  Kafka send happens in background
  If it fails → log warning, drop event
  User never sees the difference
```

---

### 7.4 Three Topics and What They Carry

```
Topic: gateway.rate_limit.hit
  Fired: when isAllowed(ip) returns false
  Payload: { ip, method, url, emittedAt }
  Consumer action: count per IP → auto-block at 5 hits

Topic: gateway.circuit.opened
  Fired: when circuit breaker fallback executes
  Payload: { upstream, method, url, emittedAt }
  Consumer action: log upstream failure alert

Topic: gateway.request.completed
  Fired: in onResponse for /api/* routes
  Payload: { method, url, statusCode, latencyMs, clientId, emittedAt }
  Consumer action: rolling latency window → spike detection
```

---

### 7.5 Consumer Service

```
Consumer process lifecycle:

  start
    └─ consumer.connect()
    └─ consumer.subscribe({ topics: [...], fromBeginning: true })
    └─ consumer.run({ eachMessage })

  eachMessage({ topic, partition, message }):
    └─ JSON.parse(message.value)
    └─ route(topic, payload)
         ├─ 'gateway.rate_limit.hit'    → handleRateLimitHit()
         ├─ 'gateway.circuit.opened'    → handleCircuitOpened()
         └─ 'gateway.request.completed' → handleRequestCompleted()
```

```
Latency spike detection (rolling window):

  latencyWindow = []  (last 50 values)
  LATENCY_SPIKE_MULTIPLIER = 3×

  New request completes at 312ms:
    push 312 to window
    if window.length === 50:
      avg = sum / 50 = 5.10ms
      312 > 5.10 × 3 = 15.3ms  → SPIKE DETECTED
      log: "LATENCY SPIKE — GET /api/products took 312ms (avg: 5.10ms)"

  Why rolling window and not all-time average?
    All-time average drifts over time.
    Rolling window stays sensitive to recent behavior.
    A spike after midnight low traffic wouldn't get buried by daytime averages.
```

---

### 7.6 Kafka vs Alternatives

```
Option A: In-process EventEmitter
  gateway.emit('rate_limit.hit', data)
  
  ✅ Zero latency
  ✅ No infrastructure
  ❌ Events lost on process crash
  ❌ Consumer and producer in same process (can't scale independently)
  ❌ No replay

Option B: HTTP webhook to consumer
  fetch('http://consumer/events', { method: 'POST', body })
  
  ✅ Simple
  ❌ Consumer must be online when gateway sends
  ❌ Adds HTTP round-trip latency to every request (if awaited)
  ❌ No durability, no replay
  ❌ Tight coupling (gateway knows consumer's URL)

Option C: Kafka
  producer.send({ topic, messages })
  
  ✅ Durable (events survive crashes)
  ✅ Decoupled (gateway doesn't know about consumers)
  ✅ Replayable (consumer can restart from offset 0)
  ✅ Multi-consumer (add new consumer without touching gateway)
  ❌ Operational overhead (broker, Zookeeper)
  ❌ More complex setup

For Nexus: Kafka is the right choice for learning distributed systems patterns.
The operational overhead is acceptable in a local dev environment.
```

---

### 7.7 Honest Limitation: Consumer Block List Is Not Enforced

```
Consumer detects IP 1.2.3.4 should be blocked:
  blockedIPs.add('1.2.3.4')
  log: "AUTO-BLOCKED IP: 1.2.3.4"

Gateway receives next request from 1.2.3.4:
  isAllowed('1.2.3.4') → checks token bucket only
  → NOT blocked
  → request proceeds

The block list is in the consumer's memory.
The gateway has no way to read it.

Fix:
  Consumer: await redis.sadd('blocked:ips', ip)
  Gateway:  const isBlocked = await redis.sismember('blocked:ips', ip)
            if (isBlocked) return reply.code(403).send({ error: 'Forbidden' })

This is the missing link. The architecture supports it
(consumer has Redis access, gateway has Redis access),
but the enforcement hook is not wired in the gateway yet.
```

---

## 8. Observability Stack

> Observability answers three different questions. Traces: *where did time go in this specific request?* Metrics: *how is the system behaving across all requests right now?* Logs: *what exactly happened during this event?* Each pillar is useless without the others.

---

### 8.1 The Three Pillars

```
                    ONE REQUEST                  ALL REQUESTS
                    (specific)                   (aggregate)
                         │                            │
                         ▼                            ▼
                     TRACING                      METRICS
                   (where did                  (how many, how
                   time go?)                   fast, how often?)
                         │                            │
                         └──────────┬─────────────────┘
                                    │
                                    ▼
                                 LOGS
                         (what exactly happened?)
                         (linked to trace via trace_id)
```

---

### 8.2 Distributed Tracing (OpenTelemetry + Jaeger)

#### What a Trace Is

```
One request = one trace = tree of spans

GET /api/products (19.4ms total)
├─ [SERVER span] nexus-gateway: GET /api/*          0ms → 19.4ms
│     attributes: http.method=GET, http.url=..., http.status_code=200
│
└─ [CLIENT span] upstream.proxy                     1.5ms → 16.7ms
      attributes: peer.service=http://127.0.0.1:3001
                  http.status_code=200

Read as: The full request took 19.4ms.
         The upstream call took 15.2ms of that.
         ~4ms was spent in gateway logic (auth, transform, etc.)
```

#### W3C traceparent Header

```
Gateway receives request (no traceparent):
  → new root trace created
  → traceId: c160fc98aa80ca92a9f9238453a4a6a1
  → spanId:  afd2b0f4229a2dd1

Gateway sends request to upstream:
  → injects header: traceparent: 00-c160fc98...-afd2b0f4...-01
                                 └ version  traceId   spanId  flags

Upstream (if OTel-instrumented):
  → reads traceparent
  → creates child span under same traceId
  → appears in Jaeger as nested span under gateway's trace
```

#### Initialization Order — Critical

```javascript
// server.js — MUST be line 1
require('./tracing');   // ← patches http module BEFORE anything else loads

// If Fastify or any dependency loads http first:
//   OTel never sees those http calls
//   upstream spans never appear in traces
//   no error thrown — just silent missing data
```

```
tracing.js does:
  1. Set OTEL_SERVICE_NAME = 'nexus-gateway'
  2. Create OTLPTraceExporter (sends to Jaeger :4318)
  3. Create BatchSpanProcessor (async batching)
  4. NodeSDK.start() → monkey-patches http, https, etc.
```

#### BatchSpanProcessor vs SimpleSpanProcessor

```
SimpleSpanProcessor:
  span.end() called
       │
       ▼ (synchronous)
  HTTP export to Jaeger
       │ ← request path BLOCKS here
       ▼
  span.end() returns
  
  Problem: Jaeger export latency added to EVERY request


BatchSpanProcessor:
  span.end() called
       │
       ▼ (non-blocking)
  span added to in-memory queue
       │
       ▼ (immediate return)
  request continues
  
  Meanwhile (every 500ms):
  BatchSpanProcessor.flush()
       └─ HTTP export to Jaeger (background)
  
  Config:
    maxQueueSize: 2048         ← drop oldest if queue full
    scheduledDelayMillis: 500  ← flush every 500ms
    maxExportBatchSize: 512    ← max spans per export call
```

#### Log-Trace Correlation

```javascript
// onRequest hook:
const { traceId, spanId } = span.spanContext();
req.log = req.log.child({ trace_id: traceId, span_id: spanId });
// ↑ ALL subsequent req.log.info() calls now include trace_id automatically
```

```
Pino log output:
{
  "level": 30,
  "reqId": "req-2",
  "trace_id": "c160fc98aa80ca92a9f9238453a4a6a1",   ← same as Jaeger
  "span_id": "afd2b0f4229a2dd1",
  "url": "/api/products",
  "method": "GET",
  "msg": "request started"
}

Workflow:
  See error in log → copy trace_id
  Open Jaeger → paste trace_id → find exact trace
  See which span was slow/errored
  No separate log aggregation system needed
```

---

### 8.3 Prometheus Metrics

#### Pull vs Push

```
PUSH model (e.g., StatsD):
  Application pushes metrics to collector
  Application must know collector's address
  If app crashes → silent (no data, no alert)
  
PULL model (Prometheus):
  Prometheus scrapes /metrics endpoint every 15s
  Application exposes data, Prometheus decides when to collect
  If app crashes → scrape fails → Prometheus fires alert
  ← Nexus uses this
```

#### Six Instruments in Nexus

```
COUNTERS (only go up):

  nexus_http_requests_total{method, route, status_code}
    → "how many requests total, by type"
    → rate(nexus_http_requests_total[1m]) = requests/second

  nexus_rate_limit_hits_total{ip}
    → "how many rate limit rejections, by IP"

  nexus_circuit_breaker_opens_total{upstream}
    → "how many times each upstream's breaker tripped"


HISTOGRAMS (distribution across buckets):

  nexus_http_request_duration_ms{method, route, status_code}
    buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000] ms

  nexus_upstream_request_duration_ms{upstream, method, status_code}
    → separates upstream latency from total gateway latency


GAUGE (can go up or down):

  nexus_circuit_breaker_state{upstream}
    0 = CLOSED (healthy)
    1 = OPEN (tripped)
    2 = HALF-OPEN (testing recovery)
```

#### Why Histograms, Not Averages or Gauges

```
Average latency = 20ms

Sounds fine. But the distribution might be:

  99% of requests: 5ms
   1% of requests: 1615ms
  
  Average: (0.99 × 5) + (0.01 × 1615) = 4.95 + 16.15 = 21.1ms
  
  That 1% is real users experiencing 1.6 second latency.
  The average hides it completely.

Histogram buckets capture this:
  nexus_http_request_duration_ms_bucket{le="10"}  = 99000   ← 99% under 10ms
  nexus_http_request_duration_ms_bucket{le="2000"} = 100000 ← all under 2000ms
  
  histogram_quantile(0.99, rate(nexus_http_request_duration_ms_bucket[5m]))
  → p99 latency = ~1600ms ← now you can see the problem
```

#### PromQL Quick Reference

```
Request rate (req/sec, 1-min window):
  rate(nexus_http_requests_total[1m])

p99 request latency:
  histogram_quantile(0.99, rate(nexus_http_request_duration_ms_bucket[1m]))

p95 upstream latency:
  histogram_quantile(0.95, rate(nexus_upstream_request_duration_ms_bucket[1m]))

Rate limit hits in last 5 minutes:
  increase(nexus_rate_limit_hits_total[5m])

Circuit breaker state per upstream:
  nexus_circuit_breaker_state
```

---

### 8.4 How All Three Pillars Connect

```
Request comes in at t=0ms

  ┌─ TRACE starts ───────────────────────────────────────────────────┐
  │  trace_id: c160fc98...                                          │
  │  → injected into Pino child logger                              │
  │  → injected into upstream request (traceparent header)          │
  └─────────────────────────────────────────────────────────────────┘

  ┌─ LOG emitted ────────────────────────────────────────────────────┐
  │  { "msg": "request started", "trace_id": "c160fc98...",         │
  │    "url": "/api/products", "method": "GET" }                    │
  └─────────────────────────────────────────────────────────────────┘

  ← upstream call happens →

  ┌─ TRACE child span ends ──────────────────────────────────────────┐
  │  upstream.proxy: 15.2ms, status 200                             │
  └─────────────────────────────────────────────────────────────────┘

  ┌─ TRACE root span ends ───────────────────────────────────────────┐
  │  → async batch export to Jaeger                                 │
  └─────────────────────────────────────────────────────────────────┘

  ┌─ METRICS recorded ───────────────────────────────────────────────┐
  │  nexus_http_requests_total{method=GET, route=/api/*, status=200}│
  │  nexus_http_request_duration_ms.observe(19.4)                   │
  │  nexus_upstream_request_duration_ms.observe(15.2)               │
  └─────────────────────────────────────────────────────────────────┘

  ┌─ KAFKA event emitted ────────────────────────────────────────────┐
  │  { method: GET, url: /api/products, latencyMs: 19.4, ... }      │
  │  → consumer's rolling latency window updated                    │
  └─────────────────────────────────────────────────────────────────┘


Debugging workflow:
  1. User reports slow request → check Grafana p99 dashboard
  2. Spike visible at 14:32 → check Jaeger for traces at 14:32
  3. Find trace with slow upstream span → copy trace_id
  4. Search Pino logs for trace_id → see full request context
  5. Identify: which upstream was slow, what headers were sent, etc.
```

---

## 9. Failure Scenarios + System Behavior

> Knowing what your system does when things go wrong is more impressive than knowing what it does when things go right. Every interviewer will probe failure paths.

---

### How to Think About Failures

For each failure, ask four questions:
1. **What detects it?**
2. **What happens to the current request?**
3. **What happens to subsequent requests?**
4. **What recovers, and how?**

---

### Failure 1: One Upstream Goes Down

**Scenario:** Server 3002 stops responding mid-traffic.

**Detection:** The HTTP request to 3002 times out (5000ms) or immediately errors with `ECONNREFUSED`.

**Current request:**
- `http.request()` fires `error` event → `lb.release(server)` → `reject(err)`
- `breaker(3002).fire()` records the failure
- `withRetry` catches the error, waits 200ms
- Attempt 2: `lb.pick()` selects Server 3001 (round-robin advances, or 3001 has fewer connections)
- Request succeeds on 3001
- Client sees 200 OK — failure is invisible

**Subsequent requests (after enough failures):**
- Once `errorThresholdPercentage` (50%) is exceeded with `volumeThreshold` (3) requests sampled, the breaker for 3002 opens
- `breaker(3002).fire()` now fast-fails immediately — no network call, no timeout wait
- `lb.pick()` may still return 3002 (it doesn't consult breaker state) — the breaker handles this at `fire()` time
- The fallback throws, retry picks 3001, request succeeds

**Recovery:**
- After `resetTimeout` (10s), breaker enters HALF-OPEN
- One test request is allowed through to 3002
- If 3002 has recovered: breaker closes, normal traffic resumes
- If still down: breaker reopens, resets timer

**What doesn't recover automatically:**
- If 3001 also goes down, all retries exhaust and clients receive 502
- Service discovery (`services.json` poller) would need to remove 3002 from the pool entirely — currently it only removes URLs that disappear from the file

---

### Failure 2: All Upstreams Down

**Scenario:** Both 3001 and 3002 are unreachable.

**What happens:**
```
Attempt 1: lb.pick() → 3002 → fails
Attempt 2: lb.pick() → 3001 → fails
Attempt 3: lb.pick() → 3002 → circuit open → fast-fail immediately
withRetry exhausts → throws lastErr
→ reply.code(502).send({ error: 'Bad Gateway' })
```

**Client experience:** After ~10.4 seconds (attempt 1: 5s timeout + 200ms delay + attempt 2: 5s timeout + 200ms delay + attempt 3: fast-fail ~0ms), receives 502.

With both circuits open (after enough failures), all three attempts fast-fail. Total time drops to ~400ms (200ms × 2 delays + fast-fail overhead). Clients fail faster, freeing the event loop from blocking on dead upstreams.

**What survives:** The gateway itself stays running. `/health`, `/metrics`, `/auth/token` continue serving. Rate limiting still works. New JWTs can still be issued. The gateway is a proxy — it reports what it can't reach, but it doesn't go down with its upstreams.

---

### Failure 3: Redis Down (Cache Proxy)

**Scenario:** The Docker Redis container stops.

**What happens to the cache proxy:**
- `redisClient.call(command, ...rest)` throws with connection error
- The `try/catch` in `server.js` catches it: `socket.write(serialize({ type: 'error', value: 'ERR ...' }))`
- The TCP connection to the proxy client remains open
- The proxy sends a Redis error response back to the caller

**What this means for callers:** Any application using the proxy as its Redis endpoint will receive Redis-style error responses. Depending on how the application handles Redis errors, it may crash or degrade gracefully.

**What currently has no fallback:**
The cache proxy does not attempt to forward to a backup Redis instance, nor does it have a "cache bypass" mode that falls through to the application without caching. In production, Redis high-availability (Redis Sentinel or Redis Cluster) would handle this. For this project, Redis down = cache proxy effectively unavailable.

**Honest framing:** *"Redis is a single point of failure in the current architecture. Production hardening would involve Redis Sentinel for automatic failover or Redis Cluster for horizontal partitioning. For a learning project running locally, single-instance Redis is an acceptable simplification."*

---

### Failure 4: Cache Stampede (The Problem Nexus Was Built to Solve)

**Scenario:** A high-traffic cache key (`product:homepage`) expires under load.

**Without singleflight (the naive system):**
```
t=0ms: key expires
t=1ms: 500 concurrent requests for product:homepage → all see cache miss
→ 500 Redis calls (or 500 database calls if Redis is the write-through layer)
→ Redis/database overwhelmed
→ Cascading latency spike
→ Potentially cascading failure
```

**With singleflight (intended Nexus behavior):**
```
t=0ms: key expires
t=1ms: 500 concurrent requests for product:homepage → all see cache miss
  Request #1: fn() called → 1 Redis call → promise stored in Map
  Requests #2-500: Map has entry → await same promise
t=50ms: Redis responds
→ All 500 requests receive same value
→ 1 Redis call total
```

**Current state caveat:** Singleflight is implemented and proven correct in isolation. It is not yet wired into the live TCP path. The cache stampede scenario is the core problem that motivates the full cache proxy design, and the architecture correctly addresses it — the wiring is the remaining work.

---

### Failure 5: Malformed RESP Packet

**Scenario:** A client sends malformed RESP data to the proxy.

**What happens:**
```javascript
parser.on('error', (err) => {
  console.error(`[proxy] parse error from ${addr}:`, err.message);
  socket.write(serialize({ type: 'error', value: `ERR ${err.message}` }));
  socket.destroy();  // ← close the connection
});
```

A RESP error is sent back (`-ERR invalid command format\r\n`), and the connection is destroyed. This prevents the parser's internal buffer from getting into an inconsistent state and affecting subsequent connections.

**Incomplete RESP data:** If a partial packet arrives and never completes (client disconnects mid-send), the buffer accumulates bytes and `INCOMPLETE` is returned repeatedly until the socket closes. The `socket.on('close')` handler calls `parser.reset()`, clearing the buffer. No corruption.

---

### Failure 6: Kafka Broker Down

**Scenario:** The Kafka broker at `localhost:9092` is unreachable.

**What happens at the gateway:**
```javascript
async function emitEvent(topic, payload) {
  if (!connected) {
    console.warn(`[kafka] producer not connected, dropping event`);
    return;
  }
  try {
    await producer.send({ ... });
  } catch (err) {
    console.error(`[kafka] failed to emit to ${topic}:`, err.message);
    // ← swallowed, not re-thrown
  }
}
```

Events are dropped silently with a warning. The gateway continues processing requests normally. No 500 errors. No request failures. Observability data is lost, but the core proxy function is unaffected.

**Framing:** *"Observability is infrastructure, not application logic. The gateway's job is to proxy requests. If the Kafka broker is unavailable, we drop events and log a warning. The alternative — blocking the request path waiting for Kafka acknowledgment — would be far worse than losing some event data."*

**What doesn't recover:** The consumer stops receiving events. IP auto-blocking and latency spike detection stop working. Metric accuracy degrades. But none of this causes user-facing errors.

---

### Failure 7: Partial Tag Invalidation Race (Without Lua)

**Scenario (theoretical — explains why Lua is used):**

Without atomic Lua execution, tag invalidation from Node.js would look like:
```
1. SMEMBERS tag:products → ["product:1", "product:2"]
   ← 1ms network round-trip ←
   ← another request reads product:1 here → gets stale data ←
2. DEL product:1
3. DEL product:2
4. DEL tag:products
   ← process crashes here → tag:products never deleted → stale tag set forever ←
```

**With Lua:**
```
EVAL script 1 tag:products
→ runs atomically inside Redis
→ no interleaving possible
→ either all keys deleted or none (script errors atomically)
```

This is not a failure that occurs in the current implementation — it's a failure that the Lua approach *prevents*. Understanding why it can't happen is the correct framing.

---

### Failure Summary Table

| Failure | Detection | Request Outcome | Recovery |
|---|---|---|---|
| One upstream down | HTTP error / timeout | Retry on different server | Circuit breaker half-open after 10s |
| All upstreams down | All retries fail | 502 Bad Gateway | Manual restart of upstreams |
| Redis down | ioredis error | RESP error to cache proxy client | Redis restart |
| Cache stampede | (prevented by singleflight) | 1 Redis call, all waiters served | N/A — prevention, not reaction |
| Malformed RESP | Parser error event | RESP error response + connection closed | Parser reset on close |
| Kafka down | Producer send throws | Event dropped, request unaffected | Kafka restart |
| Partial invalidation | (prevented by Lua atomicity) | N/A | N/A — prevention, not reaction |

---

## 10. Scalability + Bottlenecks

> Every system has a weakest link. Knowing yours before an interviewer finds it is the difference between a strong answer and a stumble.

---

### What Breaks First Under Load

```
Scale dimension: 10× traffic increase

Layer                     Current design          Breaks at
─────────────────────────────────────────────────────────────
Rate limiter              In-memory Map           Multiple instances
Singleflight              In-memory Map           Multiple instances
Hot key hit counts        In-memory Map           Memory + multiple instances
Circuit breaker state     In-memory per process   Multiple instances
Consumer IP block list    In-memory Set           Process restart / multiple instances
Redis                     Single instance         Redis itself becomes bottleneck
RESP parser buffer        Per-connection Buffer   Many long-lived connections = memory
Policy YAML loading       fs.readFileSync()       High command rate
```

---

### Bottleneck 1: In-Memory State (Single-Process Limitation)

```
Current:

  Instance A                Instance B
  ┌──────────────────┐      ┌──────────────────┐
  │ rateLimiter Map  │      │ rateLimiter Map   │
  │  1.2.3.4 → 8/10 │      │  1.2.3.4 → 8/10  │
  └──────────────────┘      └──────────────────┘
  
  Client sends 10 requests → 5 to A, 5 to B
  A sees 5 (under limit), B sees 5 (under limit)
  Client effectively gets 10 tokens instead of 10 total

Production fix:
  Redis-backed rate limiter:
    MULTI
      INCR   rate:{ip}
      EXPIRE rate:{ip} 60
    EXEC
  
  Both instances share the same Redis counter.
  
  Or: use a consistent hash load balancer so all requests
  from the same IP always go to the same instance.
  (Consistent hashing trades load distribution for stickiness.)
```

---

### Bottleneck 2: Node.js Single-Threaded Event Loop

```
Node.js runs JavaScript on one thread.

CPU-intensive operations block ALL other work:

  Request A: HMAC-SHA256 computation (fast, ~microseconds, fine)
  Request B: synchronous JSON.parse of large payload (slow if large)
  Request C: waiting... waiting... waiting...

The gateway's main work is I/O (network calls), not CPU.
I/O is async and non-blocking — 100 in-flight upstream requests
cost almost no CPU while they wait.

Where the event loop COULD be blocked in Nexus:
  • transformResponse() reads entire upstream body into memory
    before parsing: readBody() accumulates chunks in array,
    Buffer.concat() at the end
  • Very large responses (50MB upstream payload) would
    accumulate fully in memory before transformation
  
  Production fix: streaming transform — pipe response directly
  to client, transform chunks as they arrive.
```

---

### Bottleneck 3: RESP Parser Buffer Growth

```
Current parser behavior:

  Each TCP connection gets its own RespParser instance
  Parser._buf = Buffer.alloc(0) initially
  
  Slow client sends 1 byte per second:
    After 1000 seconds: _buf has 1000 bytes
    Parser keeps waiting for INCOMPLETE command
    Memory: minimal, fine
  
  Malicious client sends 100MB of garbage:
    Parser accumulates entire payload
    _buf = 100MB Buffer in memory
    No backpressure, no limit
  
  Production fix:
    socket.pause() when buffer exceeds threshold
    socket.resume() after processing
    Or: destroy socket if buffer exceeds max size
```

---

### Bottleneck 4: Policy YAML Re-Read Per Command

```
Current:
  getPolicy('GET') → loadPolicy() → fs.readFileSync() + yaml.load()
  
  At 10,000 commands/second = 10,000 file reads/second
  
  Each call:
    disk seek + read
    UTF-8 decode
    YAML parse
    Array.find() scan
  
  Even with OS file cache (frequently-read files stay in page cache),
  the YAML parse overhead per call is non-trivial.

Fix:
  Load once at startup, cache in module scope:
    let policy = loadPolicy();
    setInterval(() => { policy = loadPolicy(); }, 30000); // reload every 30s
  
  Or watch for file changes:
    fs.watch(policyFile, () => { policy = loadPolicy(); });
  
  getPolicy() becomes:
    return policyMap.get(command.toUpperCase()) || defaultPolicy;
    → O(1) Map lookup, no I/O
```

---

### Bottleneck 5: Single Redis Instance

```
Current:
  All cache reads + writes → one Redis :6379

Redis can handle ~100,000-200,000 ops/second (single instance).
For a learning project: not a bottleneck.

For production under high load:
  Read replicas → scale read throughput
    Master handles writes
    Replicas handle reads
    
  Redis Cluster → horizontal sharding
    Keys distributed across multiple nodes by hash slot
    Each node handles a subset of key space
    
  Redis Sentinel → high availability
    Monitor master for failure
    Auto-promote replica to master
    Zero manual intervention on failure
```

---

### Bottleneck 6: Hot Key Hit Count Memory Growth

```
Current hotkey.js:
  const hitCounts = new Map();  // grows forever
  
  1,000,000 unique keys cached → 1,000,000 Map entries
  Each entry: key string (~20 bytes) + count (8 bytes) ≈ 30 bytes
  1M entries × 30 bytes ≈ 30MB
  
  10M entries → 300MB
  Memory grows proportionally to unique keys seen since process start.
  
  No eviction. No TTL. No cleanup.

Production fix: LRU Cache with fixed size
  Use a fixed-capacity LRU (least-recently-used) eviction map
  When Map is full, evict the least-recently-accessed entry
  
  Libraries: lru-cache (npm)
  Or: implement doubly-linked list + Map (classic LRU)
  
  Result: bounded memory regardless of unique key count
```

---

### Scaling Roadmap (What Production Would Look Like)

```
Phase 1 (current): Single process, in-memory state
  Handles: ~hundreds of req/s on one machine

Phase 2: Distributed state
  Rate limiter → Redis-backed atomic counters
  Circuit breaker state → shared (or accept per-instance)
  Singleflight → Redlock for distributed deduplication
  IP block list → Redis Set read by gateway

Phase 3: Horizontal scale
  Run N gateway instances behind a hardware/software load balancer
  Consistent hashing for session affinity where needed
  Redis Cluster for cache layer

Phase 4: High availability
  Redis Sentinel or Cluster
  Multiple Kafka brokers
  Circuit breaker metrics exported to shared store
  Health check endpoint used by load balancer for routing

What stays the same at every phase:
  The RESP parser
  The RESP serializer  
  The Lua atomicity reasoning
  The circuit isolation principle
  The BatchSpanProcessor reasoning
  The histogram-vs-average argument
  
  These are protocol and algorithmic decisions.
  They don't change with scale — they're correct at any size.
```

---

### Summary: What to Say When Asked About Scale

```
"What breaks first?"
  → In-memory rate limiter. Single-process state shared
    nowhere. Fix: Redis-backed distributed counter.

"What can't scale horizontally?"
  → Singleflight. Each instance deduplicates independently.
    Full horizontal dedup requires distributed coordination
    (Redlock, or route all traffic for a key to one instance).

"What's your biggest architectural risk?"
  → Redis is a single point of failure. If Redis goes down,
    the cache proxy becomes unavailable. Production fix:
    Redis Sentinel for automatic failover.

"What would you change if this needed to handle 100× traffic?"
  → Replace all in-memory Maps with Redis-backed equivalents.
    Add Redis Cluster. Run multiple gateway instances behind
    a load balancer. Export circuit breaker state to shared store.
    Add streaming response transformation instead of buffering.
```

---

## 11. Interview Mastery + Defense

---

### 11.1 Project Summary

#### 30-Second Version
*"I built Nexus — a distributed API Gateway and intelligent Redis cache proxy from scratch in Node.js. The gateway handles JWT auth, token bucket rate limiting, load balancing across multiple upstreams with circuit breaking and retries. The cache proxy operates at the protocol level — it parses Redis's binary RESP protocol over raw TCP, and the design includes singleflight request deduplication to prevent thundering herds, atomic tag-based cache invalidation using Lua scripts, and hot key TTL extension. I also added Kafka-based event streaming, distributed tracing with OpenTelemetry and Jaeger, and Prometheus metrics. The project is still actively developed — the intelligent caching modules are built and tested independently, and wiring them into the live TCP path is the next step."*

#### 2-Minute Version
*"Nexus started from a real caching problem I wanted to understand deeply: cache stampedes. When a popular cache key expires under high traffic, every concurrent request misses the cache simultaneously and hammers the underlying data store. I built a TCP-based Redis proxy in Node.js that sits transparently between the application and Redis — any Redis client can use it without code changes, because it speaks the same RESP binary protocol.*

*The proxy includes a singleflight deduplication mechanism: when 100 concurrent requests miss the same key, only one Redis call is made — everyone else waits on the same promise. I implemented Redis's RESP protocol from scratch with a stateful streaming parser that handles TCP packet fragmentation. For cache invalidation, I use Redis Lua scripts for atomic group deletion — so invalidating a set of related keys either happens fully or not at all.*

*The API gateway in front handles JWT authentication, per-IP token bucket rate limiting, weighted round-robin load balancing, per-upstream circuit breaking with opossum, and retry orchestration. I added Kafka event streaming so the gateway emits structured events that a separate consumer service processes — for IP auto-blocking and latency anomaly detection. Distributed tracing with OpenTelemetry correlates spans across the gateway and upstreams.*

*The project is still in active development — the intelligent cache modules are built and unit-tested, and I'm currently integrating them into the live TCP request path. It's primarily a deep learning project, but the architectural decisions are all informed by how production systems at scale actually solve these problems."*

---

### 11.2 Questions by Subsystem

#### Cache Proxy — Most Likely Questions

**Q: Why did you build a TCP proxy instead of adding caching at the HTTP gateway level?**

*Redis uses RESP over raw TCP — not HTTP. A proxy that intercepts Redis commands must speak the same protocol. An HTTP proxy can't parse RESP frames. By operating at the TCP level, the proxy is transparent to any Redis client — they just point at port 6380 instead of 6379 with no code changes. That transparency is the architectural value.*

---

**Q: What is packet fragmentation and why does your parser handle it?**

*TCP is a stream protocol — it makes no guarantees about how data is segmented across delivery events. A single Redis command (`SET foo bar`) might arrive in one `data` event or several. My parser maintains an internal byte buffer and accumulates chunks until it has a complete RESP frame. If the buffer doesn't contain enough bytes for a complete message, it returns `INCOMPLETE` and waits for the next `data` event. Without this, commands would be processed mid-frame and corrupted.*

---

**Q: How does singleflight actually work? Walk me through the code.**

*Every in-flight request is tracked by key in a `Map`. When a request arrives, I check if the key is already in the Map. If it is, I return the existing Promise — the caller awaits the same Promise that's already in flight. If it isn't, I call the underlying function, store the resulting Promise in the Map, and attach a `.finally()` handler to delete the Map entry when it resolves or rejects. `.finally()` runs on both success and failure — if I used `.then()` instead, a failed call would leave a stale entry in the Map and all future requests for that key would receive the same rejection forever.*

---

**Q: Why Lua for cache invalidation? Why not just call DEL from Node.js?**

*Calling SMEMBERS + multiple DEL commands from Node.js requires multiple round-trips to Redis, and nothing prevents another client from reading a key between my SMEMBERS and DEL calls — they'd get stale data. Worse, if my process crashes after deleting some keys but before deleting others, I'm left with partial invalidation and a corrupted tag set.*

*Lua scripts execute atomically inside Redis — the entire script runs as a single unit with no interleaving from other commands. It's Redis's equivalent of a database transaction. Either all keys in the tag are deleted, or none are.*

---

**Q: Does your cache proxy currently perform intelligent caching?**

*Honestly, no — not yet. The proxy currently passes all commands through to Redis directly. The intelligent modules — singleflight, policy engine, hot key detection, Lua invalidation — are all built and tested in isolation. My development approach was to validate each component independently before reasoning about their interaction in a concurrent TCP environment. Wiring them into the live command handler is the active next step. I can walk you through exactly what the integration would look like if you're interested.*

*(This is the honest answer. Follow it immediately with the integration sketch from Section 6.9 if probed.)*

---

#### Load Balancing + Resilience — Most Likely Questions

**Q: What's the difference between your three load balancing algorithms?**

*Round-robin cycles sequentially — O(1), stateless relative to servers, good default. Least-connections tracks active in-flight requests per server and routes to the least loaded — O(n) scan but better for variable request durations. Weighted round-robin uses GCD reduction to build a proportional sequence — a weight-2 server appears twice for every once a weight-1 server appears, without floating-point arithmetic.*

---

**Q: Explain the GCD reduction in weighted round-robin.**

*If servers have weights `[4, 2, 2]`, a naive sequence would be 8 elements long. The GCD of `{4, 2, 2}` is 2. Dividing all weights by GCD gives `[2, 1, 1]`, producing a 4-element sequence with the same proportional distribution. The ratio is preserved — `4:2:2 = 2:1:1` — but the sequence is half the size. For weights like `100:50`, this matters considerably. The key insight is that dividing by GCD is a uniform scaling operation that preserves ratios.*

---

**Q: Why one circuit breaker per upstream URL instead of one shared breaker?**

*A single shared circuit breaker across all upstreams would trip when any upstream fails. If Server 3002 goes down and trips the shared breaker, traffic to Server 3001 is also blocked — even though 3001 is healthy. Per-upstream circuit breakers isolate failures. A failure in 3002 only affects traffic routed to 3002. 3001 continues handling requests uninterrupted. Fault isolation is the entire point.*

---

**Q: What happens when both upstream servers are down?**

*All three retry attempts fail. Depending on whether the circuit breakers have opened, attempts either time out (5s each) or fast-fail immediately. After exhausting `maxAttempts`, `withRetry` re-throws the last error. The gateway handler catches it and returns 502 Bad Gateway. The gateway itself stays running — health endpoint, metrics endpoint, and auth endpoint continue working.*

---

**Q: Why does the retry engine call `lb.pick()` fresh on each attempt?**

*Retrying the same server that just failed makes no sense — it would just fail again. Calling `lb.pick()` fresh each attempt lets the load balancer route to a different, potentially healthy server. The retry engine and load balancer compose naturally — neither needs to know about the other's internal state. The circuit breaker handles fast-failing on a still-broken server if the LB happens to pick it again.*

---

#### Gateway — Most Likely Questions

**Q: Why is rate limiting checked before JWT verification?**

*JWT verification requires HMAC-SHA256 computation — it's cheap, but non-trivial CPU work. Under a brute-force attack sending thousands of requests per second, verifying each JWT before rate-limiting means spending CPU on every request before deciding to reject it. Rate limiting uses an O(1) Map lookup. Checking it first means abusive IPs are rejected before any expensive computation runs.*

---

**Q: What's the difference between HS256 and RS256?**

*HS256 is symmetric — the same secret signs and verifies tokens. Both sides must know the secret. RS256 is asymmetric — a private key signs, a public key verifies. The verifier never needs the private key.*

*For this project, HS256 is appropriate: only the gateway signs tokens, only the gateway verifies them. In a distributed system where multiple independent services need to verify tokens without sharing a secret, RS256 is the right choice — you can distribute the public key freely without exposing signing capability.*

---

**Q: Why does `tracing.js` have to be the first `require` in `server.js`?**

*The OpenTelemetry SDK instruments Node.js by monkey-patching the `http` module — it wraps the original `http.request` to automatically create spans for outgoing calls. This patching must happen before any other module imports `http`. If Fastify or any dependency loads `http` first, the instrumentation misses those calls entirely. The require order is a hard constraint of how OTel's auto-instrumentation works.*

---

#### Kafka + Observability — Most Likely Questions

**Q: Why Kafka instead of just emitting events in-process?**

*An in-process event emitter is synchronous and ephemeral — if the gateway process crashes, all unsent events are lost. An HTTP webhook is synchronous — sending it adds latency to every request and couples availability to the consumer. Kafka gives durability (events survive crashes and are retained on disk), decoupling (consumer can be offline and catch up via offset replay), and multi-consumer support (multiple services can independently read the same events for different purposes).*

---

**Q: Why `BatchSpanProcessor` instead of `SimpleSpanProcessor`?**

*`SimpleSpanProcessor` exports spans synchronously — every `span.end()` blocks until the export network call to Jaeger completes. On a busy gateway, that adds Jaeger's round-trip latency to every single request. `BatchSpanProcessor` queues spans in memory and flushes them to Jaeger in background batches every 500ms. The hot request path never blocks on observability infrastructure.*

---

**Q: Why histograms for latency instead of averages?**

*Averages hide distribution. A p99 latency of 2000ms — where 99% of requests are under 2 seconds — would show an average of ~20ms if the other 99% are fast. The 1% of users experiencing 2-second requests wouldn't show up in your average. Histograms store the full distribution in configurable buckets. Prometheus can then compute any percentile (`histogram_quantile(0.99, ...)`) over any time window. Averages can't be aggregated correctly across multiple instances either — histogram buckets can.*

---

### 11.3 Common Traps and How to Navigate Them

**Trap: "Is this production-ready?"**

Don't say yes. Don't get defensive. Say:

*"It's a learning project intentionally built to production architectural standards, not production operational standards. The architectural decisions — protocol-level proxying, singleflight deduplication, atomic Lua invalidation, per-upstream circuit isolation — are all patterns used in production systems. The simplifications — single-instance Redis, in-memory rate limiting state, no TLS — are knowingly made for learning clarity and would need hardening before real deployment."*

---

**Trap: "This is just a wrapper around Redis/opossum/Kafka — what did you actually build?"**

*"The protocol-level RESP parser is entirely hand-built — it handles TCP packet fragmentation, binary-safe bulk string parsing, nested array parsing, and the INCOMPLETE sentinel pattern. The singleflight implementation is hand-built from first principles using Promise sharing. The load balancer with GCD-reduced weighted round-robin is hand-built. The token bucket rate limiter is hand-built. opossum and ioredis are libraries, yes — the same way production systems use libraries. What matters is understanding why I chose them, what they do internally, and where they fall short."*

---

**Trap: "Your rate limiter is in-memory — how does it work with multiple instances?"**

*"It doesn't, and I'm aware of that. In-memory token buckets are per-process. With horizontal scaling, each gateway instance has its own independent bucket state — a client could hit 10 requests to instance A and 10 to instance B without being rate-limited, even though they sent 20 total. The production fix is a Redis-backed distributed rate limiter using atomic INCR + EXPIRE, or a sliding window counter. For this single-instance learning environment, in-memory is the correct simplification."*

---

**Trap: "Your consumer logs 'AUTO-BLOCKED IP' but the gateway doesn't enforce it — the block doesn't actually do anything."**

*"That's correct — and I noticed it. The consumer's block list is currently in-memory and not shared with the gateway. The complete implementation would push blocked IPs to a Redis Set, and the gateway's rate limiter check would consult that Set on each request. The consumer-gateway connection through shared Redis is the missing link. The architecture is designed for it — the consumer already runs as a separate process and has access to Redis — but the enforcement hook in the gateway isn't wired yet."*

---

### 11.4 Resume Bullets

These are for a fresher/student context. They're accurate, don't overclaim, and use action → mechanism → outcome structure:

```
• Built a TCP-based Redis cache proxy in Node.js that parses Redis's binary RESP 
  protocol from scratch, handling TCP packet fragmentation via a stateful streaming 
  buffer — enabling transparent cache interception for any Redis client without 
  application code changes

• Implemented singleflight request deduplication to prevent cache stampede: collapses 
  concurrent in-flight requests for the same cache key into a single Redis call by 
  sharing a Promise reference, reducing backend pressure under burst traffic by up to 
  100× per key

• Designed atomic tag-based cache invalidation using Redis Lua scripting, ensuring 
  related cache keys are invalidated as a single atomic operation — eliminating partial 
  invalidation under concurrent access patterns

• Built an API Gateway in Fastify with per-IP token bucket rate limiting (O(1) 
  in-memory Map), HS256 JWT authentication, and a manual reverse proxy supporting 
  three load balancing algorithms: round-robin, least-connections, and GCD-reduced 
  weighted round-robin

• Implemented per-upstream circuit breaking (opossum) with 3-attempt retry 
  orchestration — failures on one upstream fast-fail in <1ms once tripped, while 
  healthy upstreams continue serving traffic uninterrupted

• Integrated distributed tracing (OpenTelemetry + Jaeger), Prometheus metrics 
  (counters, histograms, gauges), and structured Pino logging with trace ID correlation 
  — trace ID injected into every log line for zero-infrastructure cross-signal debugging

• Designed a Kafka event-driven architecture decoupling the gateway from observability 
  concerns: gateway emits structured events fire-and-forget, a separate consumer service 
  handles IP auto-blocking and rolling latency spike detection without affecting 
  request-path latency
```

---

### 11.5 What Interviewers Are Actually Testing

When they ask about Nexus, they're not testing whether you built a perfect production system. They're testing:

| What They Ask | What They're Actually Testing |
|---|---|
| "Walk me through your RESP parser" | Can you reason about byte-level protocols? Do you understand streaming data? |
| "How does singleflight prevent thundering herd?" | Do you understand concurrency and Promise semantics? |
| "Why Lua for invalidation?" | Do you understand atomicity and why it matters? |
| "What breaks at scale?" | Can you identify your own weaknesses? Do you think about failure? |
| "Is this production-ready?" | Are you honest? Do you know the gap between learning projects and real systems? |
| "What would you do differently?" | Growth mindset. Learning from experience. |

The strongest answer to "what would you do differently" for a fresher:

*"I'd wire the intelligent caching modules into the live TCP path earlier in development — I built them in isolation first which helped with correctness validation, but the integration gap means the system's most interesting feature isn't exercised end-to-end yet. I'd also add Redis Sentinel for high availability and replace the in-memory rate limiter with a Redis-backed distributed one for horizontal scalability."*

---
