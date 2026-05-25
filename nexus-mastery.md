# Nexus — Backend Engineering Mastery Notes

> Personal interview preparation + systems engineering reference.
> Built by Mithun Srinivas. Honest about current state. Deep on internals.

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

[To be written]

---

## 2. System Architecture Overview

[To be written]

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

### 4.1 Fastify Lifecycle + Hook Ordering
[To be written]

### 4.2 Token Bucket Rate Limiter
[To be written]

### 4.3 JWT Authentication (HS256)
[To be written]

### 4.4 Request Telemetry
[To be written]

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

### 7.1 Why Kafka?
[To be written]

### 7.2 Producer Design
[To be written]

### 7.3 Consumer Service
[To be written]

### 7.4 Event Topics + Partition Strategy
[To be written]

---

## 8. Observability Stack

### 8.1 Distributed Tracing (OpenTelemetry + Jaeger)
[To be written]

### 8.2 Metrics (Prometheus + Grafana)
[To be written]

### 8.3 Structured Log Correlation
[To be written]

### 8.4 Why Each Pillar Exists
[To be written]

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

[To be written]

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
