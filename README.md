# Shortly — URL Shortener

A production-patterned URL shortener built in Go to learn distributed systems concepts hands-on. Each phase introduces a new layer of complexity, from a simple Postgres-backed service through to a multi-service system with Kafka-driven async analytics.

## Architecture

```
                        ┌─────────────────────────────┐
                        │         Client (HTTP)        │
                        └──────────────┬──────────────┘
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
        POST /shorten            GET /{code}            GET /stats/{code}
              │                        │                        │
              └────────────────────────┼────────────────────────┘
                                       │
                        ┌──────────────▼──────────────┐
                        │    Gateway (HTTP :8080)      │
                        │  chi router · rate limiter   │
                        └──────┬──────────────┬────────┘
                               │ gRPC         │ gRPC
                    ┌──────────▼───┐    ┌─────▼──────────────┐
                    │  Shortener   │    │  Analytics         │
                    │  (:9090)     │    │  (:9091)           │
                    └──────┬───┬──┘    └──────┬─────────────┘
                           │   │              │ reads
                     Postgres  Redis     click_stats
                           │
                     Kafka (click.events)
                           │
                    ┌──────▼──────────────┐
                    │  Analytics Consumer  │
                    │  (same binary)      │
                    └──────┬─────────────┘
                           │ writes
                      click_stats
```

**Three binaries, one repo:**
- `./gateway` — HTTP server, routes requests to downstream gRPC services
- `./shortener` — gRPC service, owns URL creation and resolution
- `./analytics` — gRPC server (stats reads) + Kafka consumer (click writes) in one process

## Phases

### Phase 1 — Core Shortener (Postgres)

Generates a 7-character Base62 short code using `crypto/rand` (not `math/rand` — cryptographic randomness avoids collisions being predictable). Stores `(short_code, long_url)` in Postgres. On a `UNIQUE` constraint violation (`pgconn.PgError` code `23505`), retries with a new code up to 5 times.

**Key decisions:**
- `pgxpool` for connection pooling — a single connection would serialise all requests
- Sentinel errors (`ErrDuplicateCode`, `ErrNotFound`) so callers use `errors.Is` rather than string matching
- Migration runs at startup with `CREATE TABLE IF NOT EXISTS` — idempotent, safe on restart

### Phase 2 — Redis Caching (Cache-Aside + Stampede Protection)

Resolve flow: check Redis → on miss, query Postgres, populate cache. Two subtle problems addressed:

**Cache stampede** — if 1000 requests for the same uncached code arrive simultaneously, all 1000 hit Postgres. Fixed with `golang.org/x/sync/singleflight`: the first goroutine does the DB lookup; the rest wait for its result. One DB query for N concurrent requests.

**Negative caching** — a non-existent code would still cause a DB hit on every request. Storing a sentinel value (`__not_found__`) in Redis with a short TTL (5 min) blocks repeated lookups for codes that don't exist, while a distinct `ErrNegative` error lets the service distinguish "cache miss" from "confirmed not found".

### Phase 3 — gRPC Microservices

Split the monolith into gateway (HTTP) and shortener (gRPC). The gateway translates HTTP ↔ gRPC and owns client-facing concerns; the shortener owns data.

**Why gRPC over REST between services?** Strongly typed contracts via Protocol Buffers, generated client/server code, built-in deadline propagation. HTTP status codes don't carry enough semantic information between services — gRPC status codes (`codes.NotFound`, `codes.Unavailable`) map intent precisely.

**Interceptors** sit in front of every handler on the gRPC server:
- `RecoveryInterceptor` — catches panics with a named return value (`resp interface{}, err error`) so the defer can assign to `err` after recovery. Without named returns the assignment would silently do nothing.
- `LoggingInterceptor` — logs method, duration, and error for every call

**Context deadline passing** — the gateway sets a 5-second timeout (`context.WithTimeout`) before every gRPC call. This deadline propagates into the shortener and from there into every Postgres and Redis call. A slow DB query that exceeds the client's deadline cancels automatically rather than holding resources.

### Phase 4 — Distributed Rate Limiting

Sliding window rate limiter backed by Redis sorted sets. Each request adds a timestamped entry (UUID member, millisecond score). Before each insert, entries older than the window are pruned. If `ZCARD >= limit`, reject.

**Why a Lua script?** The prune → count → insert sequence is three separate Redis commands. Without atomicity, two concurrent requests could both read `ZCARD = 9` (under limit), both decide to allow, and both insert — allowing 11 requests when the limit is 10. The Lua script runs as a single atomic unit on the Redis server.

**Why `PEXPIRE` and not `EXPIRE`?** `EXPIRE` takes seconds. The window is tracked in milliseconds. Passing a millisecond value to `EXPIRE` would set a TTL of hours/days.

**Fail-open** — if Redis is unavailable, the middleware lets the request through rather than blocking all traffic. Analytics and rate limiting are best-effort; the core redirect path must stay up.

Two gateway replicas share one Redis instance. The rate limit is per-IP across the entire fleet, not per-replica.

### Phase 5 — Async Analytics (Kafka)

On every successful redirect, the shortener publishes a `ClickEvent` to Kafka topic `click.events`. The message key is `short_code`, which routes all events for the same code to the same partition — giving ordering guarantees per code.

**Fire-and-forget producer** — the hot path (redirect) must not block on Kafka. A buffered channel (1000 slots) sits in front of a background goroutine that writes to Kafka. If the channel fills up (Kafka down, producer slow), events are silently dropped. Redirects never fail because analytics are unavailable.

**Consumer design:**

The consumer uses a goroutine to fetch messages (`FetchMessage` with the parent context — never cancelled until shutdown). A separate ticker flushes the accumulated batch every 2 seconds. Keeping `FetchMessage` on a stable context is critical: cancelling the context mid-fetch causes kafka-go to close the connection and leave the consumer group, forcing a rejoin from scratch on every cycle.

**At-least-once delivery + idempotent writes:**

Kafka offsets are committed *after* the DB write succeeds. If the consumer crashes between the DB write and the offset commit, Kafka replays those messages on restart. The upsert handles the duplicate:

```sql
INSERT INTO click_stats(short_code, bucket_hour, clicks)
VALUES ($1, $2, 1)
ON CONFLICT (short_code, bucket_hour)
DO UPDATE SET clicks = click_stats.clicks + excluded.clicks
```

`excluded` is a Postgres pseudo-table holding the row that would have been inserted. A replay adds 1 again, slightly overcounting — acceptable for analytics. Perfect counts would require an `event_id` deduplication table.

**Batching** — 100 messages or 2 seconds, whichever comes first. One Postgres transaction for the batch instead of 100 individual inserts.

### Phase 6 — Analytics Read API

`GET /stats/{code}` → gateway → analytics gRPC → `click_stats`.

Results cached in Redis with a 30-second TTL. Analytics data is eventually consistent anyway (Kafka lag means the read model is slightly behind the write path), so a short cache is a reasonable tradeoff — it absorbs read bursts without hiding stale data for long.

**CQRS-lite:** the write path (Kafka → consumer → `click_stats`) and the read path (gRPC → `click_stats`) are separate code flows hitting the same table. Full CQRS would separate the stores entirely; for this scale sharing Postgres is pragmatic.

## Project Structure

```
shortly/
├── cmd/
│   ├── gateway/          # HTTP server binary
│   │   └── handler/      # HTTP handlers (shorten, redirect, stats)
│   ├── shortener/        # gRPC shortener binary
│   └── analytics/        # gRPC + Kafka consumer binary
├── internal/
│   ├── analytics/
│   │   ├── consumer/     # Kafka consumer + batch processor
│   │   ├── event/        # ClickEvent type
│   │   ├── grpc/         # Analytics gRPC server
│   │   ├── producer/     # Async Kafka producer
│   │   └── repository/   # click_stats DB queries
│   ├── codec/            # Base62 random code generation
│   ├── config/           # Env-based config per service
│   ├── gateway/
│   │   └── ratelimit/    # Sliding window limiter + middleware
│   └── shortener/
│       ├── cache/        # Redis cache-aside + negative caching
│       ├── domain/       # URLData type
│       ├── grpc/         # gRPC server + interceptors
│       ├── repository/   # Postgres URL store
│       └── service/      # Business logic + singleflight
├── migrations/
│   ├── 001_url.sql
│   └── 002_click_stats.sql
├── proto/
│   ├── shortener.proto
│   └── analyticsv1/
│       └── analytics.proto
├── Dockerfile            # Multi-stage build
└── docker-compose.yml    # Full stack: postgres, redis, kafka, shortener, analytics, 2x gateway
```

## Running Locally

```bash
# Start everything
docker compose up --build -d

# Shorten a URL
curl -X POST http://localhost:8080/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}' | jq .
# {"short_code": "abc1234"}

# Redirect
curl -L http://localhost:8080/abc1234

# Generate clicks
for i in $(seq 1 8); do curl -s -o /dev/null http://localhost:8080/abc1234; done

# Check stats (wait ~3s for consumer to flush)
curl http://localhost:8080/stats/abc1234 | jq .

# Inspect Kafka
docker exec -it shortly_kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic click.events --from-beginning --max-messages 5

# Inspect Postgres
docker exec -it shortly_postgres psql -U shortly -d shortly \
  -c "SELECT short_code, bucket_hour, clicks FROM click_stats;"
```

## Kill Consumer Mid-Stream (At-Least-Once Demo)

```bash
# Stop consumer, generate clicks, restart — no clicks should be lost
docker compose stop analytics
for i in $(seq 1 5); do curl -s -o /dev/null http://localhost:8080/abc1234; done

# DB count unchanged (consumer was down)
docker exec -it shortly_postgres psql -U shortly -d shortly \
  -c "SELECT clicks FROM click_stats WHERE short_code = 'abc1234';"

# Restart — Kafka replays uncommitted messages
docker compose start analytics
sleep 5

# Count now includes the 5 clicks from while it was down
docker exec -it shortly_postgres psql -U shortly -d shortly \
  -c "SELECT clicks FROM click_stats WHERE short_code = 'abc1234';"
```

## Stack

| Concern | Technology |
|---|---|
| Language | Go 1.26 |
| HTTP router | chi |
| Service communication | gRPC / Protocol Buffers |
| Primary store | Postgres 16 (pgx/v5) |
| Cache | Redis 7 |
| Message broker | Apache Kafka 3.8 (KRaft mode) |
| Config | caarlos0/env |
| Containerisation | Docker multi-stage build |

## What I Learned

- **Interface-driven design** keeps layers testable. `URLStore` and `URLCache` interfaces mean the service layer never imports Postgres or Redis packages directly — only the constructor does. Swap the implementation, tests don't change.
- **Context is a first-class citizen** in Go. Every network call accepts a context. A deadline set at the HTTP handler propagates automatically through gRPC and into every DB/Redis call below it — no manual timeout threading.
- **Atomicity matters at system boundaries.** The rate limiter's read-modify-write must be atomic (Lua script). The consumer's DB write and Kafka offset commit are intentionally *not* atomic — the ordering (DB first, then offset) is a deliberate choice that gives at-least-once semantics.
- **Fail-open vs fail-closed.** Rate limiting and analytics fail open (Redis down → allow traffic, Kafka down → drop events). The core redirect path is synchronous and fails closed (Postgres down → error). The distinction is whether the dependency is on the critical path.
- **Eventual consistency is a deliberate tradeoff, not a bug.** Click counts lag behind real-time because Kafka decouples the write path from the read model. The redirect is strongly consistent (reads from Postgres). Stats are eventually consistent (reads from a write-behind table). Choosing which operations need strong consistency is an architectural decision.
