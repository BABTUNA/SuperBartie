# Phase 6: Control API and Live Deployment

Goal: Bartie stops being a repo plus a video and becomes a URL. A control API makes the running pipeline visible and pokeable over HTTP, the whole stack runs in containers on one box behind Caddy, and a page on the site drives it.

This doc describes the code as it is. The reader and writer logic is unchanged: the writer gained a timing call per flush and a pause check, the reader gained a phase marker. Two later phases build on this one: [phase 7](phase-7-vector-destination.md) adds a second destination and `/ask`, [phase 8](phase-8-mcp.md) wraps this API as MCP tools.

Done when (verified locally against the containerized stack):

```bash
docker compose -f deploy/docker-compose.yml --profile live up -d --build
curl -s localhost:8088/pipelines/bartie/usage                                # latency + reader lag + backlog + merge ms
curl -s -X POST localhost:8088/demo/poke -d '{"op":"u", ...}'                 # source row changes, lands in ~2s
curl -s -X POST 'localhost:8088/pipelines/bartie/verify?timeout=10s'          # {"match": true, ...}
curl -s 'localhost:8088/demo/table/public.observations?limit=5'               # newest rows, source and destination
```

## Architecture

```text
                    ┌──────────────────── one box, docker compose ─────────────────────┐
                    │                                                                  │
 traffic ─writes─▶ source pg ──WAL──▶ reader ──▶ Redpanda ──┬──▶ writer ────▶ dest pg (tables)
 (ranger logs)       (terra)          :9102                 │    :9101           │
                                                            └──▶ vecwriter ─▶ dest pg (bartie_vectors)
                                                                 :9103           │
                    │        api :8080 ── reads source + dest, scrapes :9101-9103, reads errors.jsonl
                    │         │                                                  │
                    └──── Caddy (TLS, :8088 locally) ──────────────────────────────┘
                              │
   site: /bartie/live, /bartie/data, blog embed          Claude Code ──stdio──▶ mcp/server.py ──▶ api
```

Nine compose services: `source`, `redpanda`, `dest` always; `reader`, `writer`, `vecwriter`, `api`, `traffic`, `caddy` under the `live` profile. One image (`deploy/Dockerfile`) holds all four Go binaries; each service picks its binary with `command:`. A shared volume `bartie_data` (mounted at `/var/lib/bartie`) carries `errors.jsonl` and the traffic pause marker between containers.

## The idea

One HTTP server that makes the pipeline visible and pokeable. Paths mirror Artie's public API where one exists so the MCP server (phase 8) can generate tools with their names. Bartie has one pipeline; its uuid is the literal string `bartie`.

`usage` goes past Artie's. Theirs returns per-table `{count, latency}`. Bartie's also says where the latency is:

```text
readerLagBytes   = pg_wal_lsn_diff(pg_current_wal_lsn(), slot.confirmed_flush_lsn)   source is ahead of reader
backlogMessages  = sum over partitions(end offset - bartie-writer committed offset)  reader is ahead of writer
mergeMs          = p50/p95/last of the writer's flushTable durations                 destination is slow
```

Three numbers, three possible bottlenecks. The MCP monitoring skill (phase 8) reads them.

The api never shares a pool with the pipeline and never touches the replication connection. It reads the source and destination over ordinary connections, and learns about the processes two ways: each process serves a JSON snapshot on its own metrics port, and each appends errors to a shared JSONL file that outlives the process.

## Function tree

```text
cmd/api/main.go
├── embed.New(cfg)                                         nil when MINICDC_EMBED_PROVIDER=none
├── api.NewRAG(ctx, destDSN, emb, llm.New(cfg), interval)  only when an embedder exists (phase 7)
└── api.New(ctx, cfg, asker).Handler()                     internal/api/server.go

internal/api/server.go
├── Handler()                                              net/http ServeMux, wrapped in cors()
│   ├── GET  /health
│   ├── GET  /pipelines                                    handlePipelineList    -> {items: [PipelineSummary]}
│   ├── GET  /pipelines/{uuid}                             handlePipelineDetail  -> PipelineDetail
│   ├── GET  /pipelines/{uuid}/usage?from&to               handleUsage           (usage.go)
│   ├── GET  /pipelines/{uuid}/error-logs                  handleErrorLogs       -> metrics.ReadErrors(errors.jsonl, 100)
│   ├── POST /pipelines/{uuid}/status {status}             handleStatus          -> POST writer:9101/control   [token]
│   ├── POST /pipelines/{uuid}/verify?timeout              handleVerify          (usage.go)
│   ├── POST /demo/poke                                    handlePoke            (demo.go)                     [rate limit]
│   ├── GET  /demo/row/{table}/{pk}                        handleRow             (demo.go)
│   ├── GET  /demo/table/{table}?limit&offset&order        handleTable           (demo.go)
│   ├── GET  /demo/traffic                                 handleTrafficGet      (demo.go)
│   ├── POST /demo/traffic {enabled}                       handleTrafficSet      (demo.go)                     [token]
│   └── POST /ask {q, mode}                                handleAsk -> Asker.Ask (ask.go, phase 7)                 [rate limit]
├── withPipeline(next)                                     404 unless uuid == "bartie"
├── requireToken(next)                                     bearer check when MINICDC_API_TOKEN is set
├── cors(next)                                             allowlist from MINICDC_CORS_ORIGINS
├── summary(ctx)                                           status from writer snapshot (running|paused|unknown),
│                                                          hasBackfillingTables from reader phase
└── scrape(ctx, baseURL)                                   GET {base}/metrics.json -> metrics.Snapshot

internal/api/usage.go
├── handleUsage
│   ├── parseWindow(r)                                     RFC3339 from/to, default last hour
│   ├── tableStats(ctx, win)                               per dest table: count(*), avg(updated_at - commit_ts)
│   │   └── destTables(ctx)                                tables with __bartie_commit_ts, minus bartie_vectors*
│   ├── slotInfo(ctx)                                      pg_replication_slots on the source
│   ├── kafka.consumer(ctx, "bartie-writer")               Metadata + ListOffsets + OffsetFetch, cached 1s
│   ├── scrape(writer)                                     mergeMs, writerReachable
│   └── freshness(ctx)                                     max(__bartie_updated_at), max(__bartie_commit_ts)
└── handleVerify
    ├── no timeout  -> verify.Compare(ctx, cfg)            single pass
    └── ?timeout=   -> verify.Converge(ctx, cfg, d)        retry every 2s until match or deadline (max 45s)

internal/api/demo.go
├── handlePoke                                             one row on the SOURCE inside a tx
│   ├── demoTables allowlist                               public.animals, public.observations; writable columns only
│   ├── demoPK(pk, col)                                    integer, 9000..9099
│   ├── demoInsert / demoUpdate / DELETE
│   └── SELECT clock_timestamp() before COMMIT             returned as commitTs
├── handleRow                                              row_to_json from source and dest for one pk
├── handleTable                                            readTables allowlist (3 tables + bartie_vectors, dest only)
│   ├── order=recent (default) | pk, limit <= 100, offset
│   └── returns rows from both sides + count(*) per side
├── handleTrafficGet / handleTrafficSet                    stat / create / remove {dir of errors.jsonl}/traffic.paused
└── demoLimiter.limit(next)                                per-IP sliding minute, X-Forwarded-For aware

internal/metrics/metrics.go                                process-global state, one per binary
├── Init(service, errorLogPath)
├── SetPhase(p) / Paused() / SetPaused(v)
├── RecordFlush(table, events, dur)                        ring of 512
├── RecordError(table, message, err)                       slog + append one JSON line to errors.jsonl
├── ReadErrors(path, limit)                                newest first; missing file is an empty log
├── TakeSnapshot()                                         {service, startedAt, phase, paused, flushes, mergeMs}
├── Handler(controllable)                                  GET /metrics.json; POST /control {status} when controllable
└── Serve(addr, controllable)                              background listener; bind failure is logged, not fatal

internal/verify/verify.go                                  refactored to return structure
├── Compare(ctx, cfg) -> Result                            one pass
├── Converge(ctx, cfg, timeout) -> Result                  retry loop
└── Run(ctx, cfg, timeout)                                 cdcctl's printing wrapper over Converge

Deltas in existing code:
internal/writer/writer.go   Run: skip fetching while metrics.Paused(); flushAll: time each flushTable, RecordFlush / RecordError
internal/reader/reader.go   Run: SetPhase("backfill") then SetPhase("streaming")
cmd/reader, cmd/writer      metrics.Init + metrics.Serve; RecordError before exiting non-zero
internal/config/config.go   metrics addrs/URLs, error log path, api addr/token/CORS/rate limit, embed + llm + snapshot settings
```

## Data shapes

```json
// GET /pipelines/bartie/usage
{
  "tableStats": [
    {"tableName": "public.animals", "count": 412, "latency": 1.1},
    {"tableName": "public.observations", "count": 0, "latency": null}
  ],
  "window": {"from": "2026-10-01T17:27:52Z", "to": "2026-10-01T18:27:52Z"},
  "readerLagBytes": 288,
  "slotActive": true,
  "backlogMessages": 0,
  "mergeMs": {"samples": 512, "last": 30.2, "p50": 13.7, "p95": 24.3, "max": 44.0},
  "lastAppliedAt": "2026-10-01T17:23:17.934Z",
  "lastCommitTs": "2026-10-01T17:23:15.903Z",
  "writerReachable": true
}
```

`tableStats` is byte-compatible with Artie's `RouterPipelineTableStats`. `latency` is null when nothing moved in the window, which is not zero lag. `readerLagBytes`, `backlogMessages`, and `mergeMs` are null when the thing they measure could not be read.

```json
// GET /pipelines/bartie
{
  "uuid": "bartie", "name": "terra → warehouse", "status": "running",
  "sourceType": "PostgreSQL", "destinationType": "PostgreSQL",
  "isDeploying": false, "hasBackfillingTables": false, "hasUndeployedChanges": false,
  "tables": [{"name": "animals", "schema": "public"}],
  "slot": {"name": "bartie", "active": true, "confirmedFlushLsn": "0/19C0130", "currentWalLsn": "0/1A040D8", "lagBytes": 278440},
  "consumer": {"groupId": "bartie-writer", "backlog": 0, "partitions": [{"partition": 0, "committed": 1592, "end": 1592, "lag": 0}]},
  "processes": {"reader": "streaming", "writer": "running", "vecwriter": "running"}
}

// GET /pipelines/bartie/error-logs      (field names follow Artie's PayloadsPipelineErrorLog where they apply)
{"items": [{"timestamp": "...", "service": "writer", "table": "public.animals", "message": "flush failed", "errorDetail": "..."}]}

// POST /pipelines/bartie/verify?timeout=10s
{"tables": [{"table": "public.animals", "sourceRows": 150, "destRows": 150, "checksumMatch": true}], "match": true, "checkedAt": "..."}

// POST /demo/poke
{"op": "u", "table": "public.observations", "pk": {"observation_id": 9001}, "set": {"notes": "..."}}
-> {"op": "u", "table": "public.observations", "pk": 9001, "commitTs": "2026-09-30T04:25:10.006Z"}

// GET /demo/table/public.animals?limit=1&order=recent
{
  "table": "public.animals", "pk": "animal_id", "recency": "updated_at", "destOnly": false,
  "limit": 1, "offset": 0, "counts": {"source": 150, "dest": 150},
  "source": [{"animal_id": 105, "name": "Jabari the Watchful", "status": "juvenile", "updated_at": "..."}],
  "dest":   [{"animal_id": 105, "name": "Jabari the Watchful", "status": "juvenile", "updated_at": "...",
              "__bartie_commit_ts": "...", "__bartie_updated_at": "..."}]
}

// GET writer:9101/metrics.json   (what the api scrapes)
{"service": "writer", "startedAt": "...", "phase": "running", "paused": false,
 "flushes": [{"at": "...", "table": "public.animals", "events": 20, "durationMs": 4.79}],
 "mergeMs": {"samples": 10, "last": 4.79, "p50": 20.5, "p95": 34.1, "max": 34.1}}
```

## Deploy

```text
deploy/
├── docker-compose.yml        3 data services always; reader, writer, vecwriter, api, traffic, caddy under profile "live"
├── Dockerfile                multi-stage: go build ./cmd/... -> alpine, one image (bartie:local), built by the reader service
├── Caddyfile                 {$API_DOMAIN} -> api:8080; default http://localhost:8088 for laptops, a real domain gets TLS
├── .env.example              API_DOMAIN, CORS origins, token, rate limit, embed/llm keys, snapshot + traffic intervals
└── vm/
    ├── bootstrap.sh          fresh Ubuntu: docker, ufw (ssh/80/443), clone, fetch-terra, cron, up
    ├── redeploy.sh           git pull, up -d --build; volumes kept, so the slot survives and the reader resumes
    └── nightly-reset.sh      down -v, up -d, wait for /health; 03:00 UTC from /etc/cron.d/bartie-nightly

scripts/demo-traffic.sh       readable writes for the page; runs as the compose "traffic" service or by hand
```

The destination image is `pgvector/pgvector:pg16`: postgres 16 plus the vector extension, so the replica tables and the vector table share one database.

Health and ordering: `source` is healthy once the publication exists, `redpanda` once `rpk cluster health` is healthy, `dest` on `pg_isready`. The pipeline services use `depends_on: condition: service_healthy`, `restart: unless-stopped`. `vecwriter` and `traffic` use `restart: on-failure` because they exit 0 on purpose when disabled.

## What the traffic writer does

Every `TRAFFIC_INTERVAL` seconds (default 2): insert an observation of a random seeded animal at a random watering hole with notes like "Ranger log: Mosi the Watchful the hippopotamus seen resting in a pair at Mopane Crossing, the reed bed." Every 4th write flips a seeded animal's status; every 7th deletes a sighting from earlier in the run. Observation ids start at 3e9 in a per-run block, clear of the seed, `load.sh`, and the page's demo row. Before each write it waits while `traffic.paused` exists in the shared volume, which is how `POST /demo/traffic` switches it off.

## Gotchas, as found

- `docker kill` is a manual stop: the restart policy does not bring the container back. A process crash inside the container does restart it. Test crashes with a signal from inside, not `docker kill`.
- A single verify pass a moment after a write mismatches, correctly: events are in flight. That is why the endpoint has `timeout`.
- `readerLagBytes` has a floor of a few hundred bytes to a few hundred KB from WAL records that are not row changes, and the standby status update runs every 5s. Flat and small is healthy; growing is the signal.
- `mergeMs` lives in the writer's memory. A writer restart resets it to zero samples until the next flush.
- A `kill -9` leaves nothing in `errors.jsonl`. The evidence is `processes: {"writer": "unreachable"}`.
- The nightly reset drops every volume, so the slot goes too and the reader comes up on the phase 4 fresh-slot path (export snapshot, backfill, stream). That path runs every night.
- Only the `reader` service has a `build:` key; the others reuse `image: bartie:local`. `up -d --build <service>` on another service does not rebuild. Use `up -d --build` with no service name.
- `/demo/poke` writes to a public database. Three fences: table and column allowlist, pk range 9000..9099, per-IP rate limit. Never raw SQL.

## The site (me_me_me repo)

```text
src/lib/bartie.ts                    NEXT_PUBLIC_BARTIE_API_URL, default http://localhost:8088
src/components/bartie-live.tsx       the console: status strip, 1 watch rows land (+ traffic switch), 2 change a row,
                                     3 ask live vs batch, 4 verify
src/components/bartie-data.tsx       the data browser: table tabs incl. bartie_vectors, newest/by-id, 50 per page, counts
src/components/data-table.tsx        shared tables: planColumns, SideTable ("fit" and "scroll" layouts, sticky pk)
src/app/bartie/live/page.tsx         /bartie/live
src/app/bartie/data/page.tsx         /bartie/data
src/app/blog/[slug]/page.tsx         registers <BartieLive /> as an MDX component
src/content/blog/bartie-postgres-cdc.mdx   "Try it live" section embedding the console
```

Everything the pages do is a plain `fetch` to the api from the browser, so the api's CORS allowlist must include the site's origin. The traffic switch sends `Authorization: Bearer` from `localStorage.bartie_token` when present; with `MINICDC_API_TOKEN` set, visitors see the state and only the owner can flip it.

## Components

| Piece | State |
| --- | --- |
| `internal/metrics` | new: flush ring, phase, pause flag, JSONL error log, per-process HTTP snapshot |
| `internal/api` | new: control API, demo endpoints, table browser, traffic switch (`/ask` arrives in phase 7) |
| `internal/verify` | `Compare` and `Converge` return structure; `Run` prints |
| `internal/writer`, `internal/reader` | timing, pause check, phase marker; apply and decode logic untouched |
| `cmd/api` | new binary |
| `deploy/` | Dockerfile, Caddyfile, compose `live` profile, `.env.example`, `vm/` scripts |
| `scripts/demo-traffic.sh` | new |

Tests added: `internal/api` (pk fence, rate limiter, window parsing, uuid guard), `internal/metrics` (percentiles, error log round trip, pause).

## Not done

**Not deployed.** Everything above runs on a laptop. Going live needs a domain for `API_DOMAIN`, a VM running `deploy/vm/bootstrap.sh`, `NEXT_PUBLIC_BARTIE_API_URL` on Vercel, and the site origin in `MINICDC_CORS_ORIGINS`.

## Explicitly out of scope

Multiple pipelines or tenants, auth beyond one bearer token, a crash button on the page, latency charts over time, schema evolution (phase 5a, still deferred), failover slots.
