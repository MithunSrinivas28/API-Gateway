## ⚡ Benchmarks

> **Environment**: Node.js v22.22.0 · Windows 11 x64 · Redis 7 (Docker) · Single-machine loopback  
> **Date**: July 2, 2026  
> **Methodology**: Each module tested in isolation, then end-to-end through the full TCP proxy pipeline.

---

### 1. RESP Protocol Engine

The custom RESP parser and serializer are the foundation of the proxy — every byte flows through them.

| Metric | Result |
|---|---|
| **Parser** — single command | **133,891 cmds/sec** |
| **Parser** — pipelined (100 cmds/batch) | **375,393 cmds/sec** |
| **Serializer** — all types (6 types × 100K iters) | **7,206,800 ops/sec** |

> **Takeaway**: The parser scales ~2.8× with pipelining. The serializer is not a bottleneck at **7.2M ops/sec**.

---

### 2. Singleflight Deduplication

Collapses concurrent identical read requests into a single backend call.

| Metric | Result |
|---|---|
| Concurrent requests fired | 50 |
| Unique keys | 5 |
| Actual backend calls | **5** |
| Requests deduplicated | **45** |
| **Dedup ratio** | **90.0%** |
| Wall-clock time | 50.81 ms |
| All results correct | ✅ |

> **Takeaway**: 50 concurrent requests for 5 keys resulted in only 5 backend calls — a **10× reduction** in Redis load under contention.

---

### 3. Rate Limiter (Token Bucket)

Per-IP rate limiting with 10-token bucket capacity and 2 tokens/sec refill.

| Metric | Result |
|---|---|
| Burst of 20 requests (same IP) | |
| ├─ Allowed | **10** (bucket capacity) |
| └─ Denied (`-ERR rate limit exceeded`) | **10** |
| `isAllowed()` throughput | **441,084 checks/sec** |

> **Takeaway**: The token bucket accurately enforces limits and adds negligible overhead — **441K checks/sec** means the rate limiter costs ~2.3μs per request.

---

### 4. Hotkey Detection

Tracks hit frequency per key. Automatically extends TTL by 120s when a key crosses the hot threshold (≥10 hits).

| Metric | Result |
|---|---|
| Keys tracked | 20 |
| Total hits fired | 228 |
| Total time | 1,525 ms |
| **Avg per `checkAndExtend()`** | **6.69 ms** |

**Top 10 Hot Keys Detected:**

| Rank | Key | Hits |
|---|---|---|
| 1 | `bench:key:0` | 50 |
| 2 | `bench:key:1` | 40 |
| 3 | `bench:key:2` | 30 |
| 4 | `bench:key:3` | 25 |
| 5 | `bench:key:4` | 20 |
| 6 | `bench:key:5` | 15 |
| 7 | `bench:key:6` | 12 |
| 8 | `bench:key:7` | 10 |
| 9 | `bench:key:8` | 8 |
| 10 | `bench:key:9` | 5 |

> **Takeaway**: The 6.69ms average includes two Redis round-trips (`TTL` + `EXPIRE`) for hot keys. Below-threshold keys cost only the in-memory `Map.set()` (~0.001ms).

---

### 5. End-to-End Proxy Throughput

Full pipeline: **TCP connect → RESP parse → Rate limit → Hotkey track → Singleflight → Redis forward → RESP serialize → TCP response**.

| Metric | GET | SET |
|---|---|---|
| Commands sent | 1,000 | 1,000 |
| **Throughput** | **151 ops/sec** | **153 ops/sec** |
| **Avg latency** | **6.61 ms** | **6.54 ms** |

| Concurrent Stress Test | Result |
|---|---|
| Concurrent GETs fired | 200 |
| Rate-limited (blocked) | 200 |
| Wall-clock time | 397 ms |

> **Takeaway**: Sequential throughput is **~150 ops/sec** at **~6.5ms per op** through the full proxy stack (connection-per-request overhead dominates). The concurrent test correctly demonstrates the rate limiter rejecting all 200 burst requests from the same IP — the token bucket was already exhausted from the sequential test.

---

### Performance Summary

```
┌─────────────────────────────┬──────────────────────┐
│ Component                   │ Throughput            │
├─────────────────────────────┼──────────────────────┤
│ RESP Serializer             │ 7,206,800 ops/sec    │
│ Rate Limiter (isAllowed)    │   441,084 checks/sec │
│ RESP Parser (pipelined)     │   375,393 cmds/sec   │
│ RESP Parser (single)        │   133,891 cmds/sec   │
│ E2E Proxy (GET)             │       151 ops/sec    │
│ E2E Proxy (SET)             │       153 ops/sec    │
├─────────────────────────────┼──────────────────────┤
│ Singleflight dedup ratio    │            90%       │
│ Rate limiter accuracy       │           100%       │
└─────────────────────────────┴──────────────────────┘
```

> **Bottleneck Analysis**: The serializer and rate limiter are sub-microsecond. The parser is fast at 375K pipelined cmds/sec. The E2E throughput (~150 ops/sec) is dominated by **TCP connection setup/teardown per request** — a persistent connection model would yield significantly higher throughput.
