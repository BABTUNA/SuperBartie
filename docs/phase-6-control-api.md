# Phase 6: Control API and Live Deployment

Goal: the pipeline stops being something you run from a terminal and becomes a URL. One HTTP server makes it visible and pokeable, the whole stack runs in containers on one box, and a page drives it. The reader and writer logic does not change.

Done when:

```bash
docker compose -f deploy/docker-compose.yml --profile live up -d --build
curl -s localhost:8088/pipelines/bartie/usage                          # latency + where the lag is
curl -s -X POST localhost:8088/demo/poke -d '{"op":"u", ...}'           # row changes on the source, lands in ~2s
curl -s -X POST 'localhost:8088/pipelines/bartie/verify?timeout=10s'    # {"match": true, ...}
```

## The core idea

Artie's API reports per-table latency. This one also says where the latency is, with three numbers that each point at one component:

```text
readerLagBytes   source WAL position - slot's confirmed position      growing -> reader or source
backlogMessages  topic end offset - writer's committed offset         growing -> writer
mergeMs          how long the writer's staging + MERGE takes          rising  -> destination
```

The api never shares a connection pool with the pipeline. It reads both databases over ordinary connections, scrapes a small JSON snapshot each process serves on its own port, and reads an error file the processes append to. Paths follow Artie's (`/pipelines/{uuid}/usage`, `/error-logs`) so phase 8 can generate MCP tools with their names. The one pipeline's uuid is `bartie`.

## Function tree

```text
cmd/api/main.go
└── api.New(ctx, cfg, asker).Handler()                    internal/api/server.go
    ├── GET  /pipelines, /pipelines/{uuid}                status, tables, slot, consumer offsets, process phases
    ├── GET  /pipelines/{uuid}/usage                      handleUsage                internal/api/usage.go
    │   ├── tableStats(window)                            count + avg(updated_at - commit_ts) per dest table
    │   ├── slotInfo()                                    pg_replication_slots on the source
    │   ├── kafka.consumer("bartie-writer")               end offsets - committed offsets, cached 1s
    │   └── scrape(writer)                                GET writer:9101/metrics.json -> mergeMs
    ├── GET  /pipelines/{uuid}/error-logs                 metrics.ReadErrors(errors.jsonl)
    ├── POST /pipelines/{uuid}/status                     forwards to writer:9101/control   (bearer token)
    ├── POST /pipelines/{uuid}/verify?timeout             verify.Compare, or verify.Converge to retry
    ├── POST /demo/poke                                   move one sighting on the SOURCE, place from a fixed list   internal/api/demo.go
    ├── GET  /demo/row/{table}/{pk}                       the same row from both databases
    ├── GET  /demo/table/{table}                          a page of rows + counts from both databases
    └── GET|POST /demo/traffic                            pause marker for the traffic writer

internal/metrics/metrics.go                               one per process
├── RecordFlush(table, events, dur)                       ring of 512; called from writer.flushAll
├── RecordError(table, msg, err)                          appends a JSON line to errors.jsonl
└── Serve(addr)                                           GET /metrics.json, POST /control (writer only)
```

Deltas in existing code: `writer.Run` skips fetching while paused and times each flush; `reader.Run` marks its phase (`backfill`, then `streaming`); `verify` returns a struct instead of printing.

## Core data shapes

### 1. Usage

```json
{
  "tableStats": [{"tableName": "public.animals", "count": 412, "latency": 1.1}],
  "readerLagBytes": 288,
  "backlogMessages": 0,
  "mergeMs": {"samples": 512, "last": 30.2, "p50": 13.7, "p95": 24.3},
  "writerReachable": true
}
```

`tableStats` is the same shape as Artie's. `latency` is null when nothing moved in the window, which is not zero lag.

### 2. Demo poke

```json
{"op": "u", "pk": {"observation_id": 9001}, "place": "the north ridge"}
-> {"op": "u", "table": "public.observations", "pk": 9001, "place": "the north ridge", "commitTs": "2026-10-01T23:43:33.658Z"}
```

It writes to a public database, so a visitor never sends text. They pick a row and one of eight places, and the server rewrites the last clause of the sighting. Only `observations` can change, and only demo rows (9000..9099, full control) or sightings the traffic writer made (update only). `op: "c"` creates the demo row or puts it back to its default, which is what the page's reset button sends. A per-IP rate limit sits on top.

### 3. Verify

```json
{"match": true, "tables": [{"table": "public.animals", "sourceRows": 150, "destRows": 150, "checksumMatch": true}]}
```

## Deploy

```text
deploy/docker-compose.yml     source, redpanda, dest always; reader, writer, vecwriter, api, traffic, caddy under profile "live"
deploy/Dockerfile             one image, all Go binaries; each service picks one with `command:`
deploy/Caddyfile              {$API_DOMAIN} -> api:8080, TLS for a real domain, plain :8088 on a laptop
deploy/vm/                    bootstrap.sh, redeploy.sh, nightly-reset.sh (03:00 UTC)
scripts/demo-traffic.sh       a readable sighting every 2s so the page has something landing
```

The site (me_me_me repo) has two pages over this api: `/super-bartie/live` (the console) and `/super-bartie/data` (both databases side by side).

## Gotchas found while building

- A single verify pass right after a write mismatches, correctly: the event is in flight. `timeout` retries until it converges.
- `readerLagBytes` never sits at zero. WAL records that are not row changes give it a small floor. Growing is the signal, not non-zero.
- `kill -9` leaves nothing in the error log. The evidence is the process showing as `unreachable`.
- `docker kill` is a manual stop, so the restart policy does not fire. A crash inside the container does restart it.
- The nightly reset drops every volume, so the slot goes too and the reader runs the phase 4 fresh-slot backfill every night.

## Components

| Piece | What changes |
| --- | --- |
| `internal/api`, `cmd/api` | new: control api and demo endpoints |
| `internal/metrics` | new: flush timings, phase, pause flag, error file |
| `internal/verify` | `Compare` and `Converge` return a struct |
| `internal/writer`, `internal/reader` | timing, pause check, phase marker; apply and decode untouched |
| `deploy/`, `scripts/demo-traffic.sh` | new: image, compose profile, Caddy, VM scripts, traffic |

## Explicitly out of scope

More than one pipeline, auth beyond one bearer token, a crash button on the page, latency charts over time. Not deployed yet: that needs a domain, a VM, and the site's env var.
