# Phase 6: Bartie Live

**Status: built (6a, 6b, 6c).** Deviations from the plan below, all deliberate:

- The default embedder is a deterministic feature-hashing bag of words (`MINICDC_EMBED_PROVIDER=hash`), so the stack runs with no keys; OpenAI and Voyage are the paid upgrades. Without an LLM key `/ask` returns the top record verbatim instead of a generated sentence.
- `mergeMs` comes from the writer's own metrics port, scraped by the api, rather than a metrics table in the destination. Errors go to a shared JSONL file so they survive the process that hit them.
- `pipeline_verify` grew a `timeout` query param (retry until match) because a single pass a moment after a write is expected to mismatch on a live pipeline.
- The MCP exposes `destination_ask` as a seventh tool; `pipeline_update_status` shipped (pause/resume via the writer's control endpoint) and is the only mutation.
- The page lives in me_me_me at `/bartie/live` and is embedded in the blog post through an MDX component.

Goal: Bartie stops being a repo plus a video and becomes a URL. The pipeline runs on one VM, a page on babtuna.vercel.app drives it, and a second destination turns every row change into a vector so an AI answer reflects a source edit within seconds. Then the whole thing is operable from Claude Code through an MCP server built on Artie's own tool contract.

Three sub-phases, in build order, each with its own cut line:

- **6a Deploy + control API.** The link exists. Poke a row, watch it land, see latency.
- **6b Vector destination + RAG demo.** Artie's "real-time data for AI" thesis, running.
- **6c MCP server.** Same tool names as artie-mcp, plus `pipeline_verify`, which their product cannot offer.

Done when:

```bash
curl -s https://$API/pipelines/bartie/usage?from=...&to=...     # 6a: latency, reader lag, backlog, merge ms
curl -s -X POST https://$API/demo/poke -d '{"op":"u", ...}'      # 6a: source row changes, page shows it land
curl -s -X POST https://$API/ask -d '{"q":"where was lion 42 last seen?"}'   # 6b: answer reflects an edit made 2s ago
claude plugin install bartie@bartie-mcp && ask "is my pipeline behind?"      # 6c: answers via pipeline_usage
```

## Architecture

```text
                          ┌────────────── VM (docker compose) ──────────────┐
                          │                                                 │
 source pg ──WAL──▶ reader ──▶ Redpanda ──┬──▶ writer ──▶ dest pg (tables)  │
   (terra)                                │                  │              │
                                          └──▶ vecwriter ──▶ dest pg (pgvector)
                                                              │
                                       api ◀──────────────────┘  reads dest, pokes source
                                        │
                          └──── Caddy (TLS) ─────────────────────────────────┘
                                        │
      babtuna.vercel.app/bartie/live ───┘        Claude Code ──▶ bartie-mcp ──▶ api
```

Reader and writer do not change. Everything new hangs off the existing Kafka topic or reads the destination.

---

## 6a: Deploy + control API

### The idea

Two things: put the existing stack on a box, and add one HTTP server that exposes what the pipeline is doing. The API mirrors Artie's public API paths where one exists (`/pipelines/{uuid}/usage`, `/pipelines/{uuid}/error-logs`) so 6c can generate MCP tools from it with their names. Bartie has one pipeline; its uuid is the literal string `bartie`.

The usage endpoint goes past Artie's. Theirs returns per-table `{count, latency}`. Bartie's also returns where the latency is:

```text
reader lag   = pg_current_wal_lsn() - slot.confirmed_flush_lsn     (source is ahead of reader)
backlog      = topic high-water mark - consumer group committed offset   (reader is ahead of writer)
merge ms     = wall time of the last N flushTable calls               (destination is slow)
```

Three numbers, three possible bottlenecks. That is the whole "explain the lag" story; the MCP skill in 6c reads them.

### Function tree

```text
cmd/api/main.go                                            new
└── api.New(cfg).Serve(":8080")                            internal/api/server.go

internal/api/server.go                                     new
├── GET  /health
├── GET  /pipelines                                        -> [PipelineSummary]
├── GET  /pipelines/{uuid}                                 -> PipelineDetail (tables, offsets, slot)
├── GET  /pipelines/{uuid}/usage?from&to                   -> Usage (below)
├── GET  /pipelines/{uuid}/error-logs                      -> [ErrorLog]
├── POST /pipelines/{uuid}/verify                          -> VerifyResult   (wraps internal/verify)
├── POST /pipelines/{uuid}/status  {status: paused|running} -> pauses/resumes the writer (6a-optional)
├── POST /demo/poke   {op, table, pk, set}                 -> writes one row on the SOURCE, returns commit_ts
└── GET  /demo/row/{table}/{pk}                            -> {source: row, dest: row, dest_updated_at}

internal/api/usage.go                                      new
├── latencyStats(dest, from, to)                           reuse internal/bench/latency.go
├── readerLag(source)                                      SELECT pg_current_wal_lsn() - confirmed_flush_lsn FROM pg_replication_slots
├── backlog(kafka)                                         kafka-go: ReadPartitions + consumer group offsets
└── mergeStats()                                           read from internal/metrics (below)

internal/metrics/metrics.go                                new, tiny
├── RecordFlush(table, events, dur)                        called from writer.flushAll
├── RecordError(service, table, err)                       called from reader + writer error paths
└── Snapshot() -> {flushes ring buffer, errors ring buffer}  exposed by writer on :9101/metrics.json, api scrapes it

internal/writer/writer.go                                  one-line delta per flush: metrics.RecordFlush
internal/reader/reader.go                                  metrics.RecordError on decode/publish failures
```

The writer gets a metrics endpoint instead of writing to the destination because the api process is separate and must not share the writer's pool. Cheapest cross-process channel: the writer serves JSON on a localhost port, the api reads it.

### Data shapes

```json
// GET /pipelines/bartie/usage?from=...&to=...
{
  "tableStats": [
    {"tableName": "public.animals", "count": 412, "latency": 1.1}
  ],
  "readerLagBytes": 0,
  "backlogMessages": 0,
  "mergeMs": {"p50": 38, "p95": 120, "last": 41},
  "window": {"from": "...", "to": "..."}
}

// GET /pipelines/bartie/error-logs   (same field names as Artie's PayloadsPipelineErrorLog where they apply)
{"items": [{"timestamp": "...", "service": "writer", "table": "public.animals", "message": "...", "errorDetail": "..."}]}

// POST /pipelines/bartie/verify
{"tables": [{"table": "public.animals", "sourceRows": 1000, "destRows": 1000, "checksumMatch": true}], "match": true}

// POST /demo/poke
{"op": "u", "table": "public.animals", "pk": {"animal_id": 42}, "set": {"status": "adult"}}
-> {"commitTs": "2026-10-02T14:03:11.204Z"}
```

`tableStats` is byte-compatible with Artie's response so an artie-mcp skill would parse it unchanged. The extra fields are additive.

### Deploy

```text
deploy/
├── docker-compose.yml        existing 3 services + reader, writer, vecwriter, api, caddy
├── Dockerfile                multi-stage: go build ./cmd/... -> distroless, one image, 4 entrypoints
├── Caddyfile                 api.$DOMAIN -> api:8080, automatic TLS
├── .env.example              MINICDC_*, EMBED_API_KEY, LLM_API_KEY, DEMO_RATE_LIMIT
└── vm/
    ├── bootstrap.sh          docker + compose install, clone, fetch-terra, up -d
    └── nightly-reset.cron    03:00 UTC: demo-setup equivalent inside compose (fresh seed, backfill)
```

Box: Hetzner CX22 (2 vCPU, 4 GB). Redpanda stays at `--memory=1G`. Destination image becomes `pgvector/pgvector:pg16` now so 6b needs no migration.

Frontend: a new route `src/app/bartie/live` in me_me_me (Next.js). Client fetches the api over CORS (allow only the vercel origin). The blog post embeds it.

### Gotchas known going in

- `/demo/poke` writes to a public database. Rate limit per IP, whitelist ops to one demo table and a fixed pk range (animal_id 9000-9099), and reset nightly. Never expose raw SQL.
- Reader lag in bytes is meaningless to a reader; convert to seconds using the source's last commit time vs `now()` when the slot is behind. Report both.
- Consumer group lag via kafka-go needs a coordinator round trip; cache for 1s so a page polling at 500ms does not hammer Redpanda.
- CORS plus rate limiting live in the api, not Caddy, so local dev behaves the same as prod.
- The nightly reset drops the slot. The reader must tolerate "slot missing" by recreating it and backfilling, which is already the fresh-slot path from phase 4. Verify it still holds under compose restart ordering (reader may start before Postgres is ready: add `depends_on` with healthchecks).

---

## 6b: Vector destination + RAG demo

### The idea

Artie's May post: AI systems reading a nightly copy give confidently wrong answers; CDC fixes it. Show it. A second consumer on the same topic embeds text columns and upserts vectors into pgvector. A tiny `/ask` endpoint does retrieval plus one LLM call. The page puts source edit and AI answer side by side.

Consumer group `bartie-vecwriter`, independent offsets from the writer. Same crash contract: commit offsets only after the pgvector transaction commits. Embedding calls happen before the transaction, so a crash between embed and commit just re-embeds on replay. Idempotent, slightly wasteful, correct.

Which text to embed, from terra:

```text
public.animals       name || species || status                (short, one embedding per row)
public.observations  notes                                   (TOAST-sized, the interesting one)
```

Document per row = the row's text plus a header line ("animal 42, lion, adult") so retrieval on "lion 42" hits.

### Function tree

```text
cmd/vecwriter/main.go                                      new, mirrors cmd/writer

internal/vecwriter/vecwriter.go                            new; copy writer.go, swap flush
├── New(cfg): kafka.Reader{GroupID: "bartie-vecwriter"}, pgxpool, embed.Client
├── Run(ctx): same buffer/age/size loop as writer.Run    reuse internal/writer.Buffer (export it or move to internal/buffer)
└── flushAll(ctx)
    ├── dedupe(evts)                                        reuse writer.dedupe (export or move to internal/fold)
    ├── docs := toDocuments(evts)                           internal/vecwriter/document.go: table -> text template
    ├── vecs := embed.Batch(ctx, docs.texts)                internal/embed/client.go, one HTTP call per <=100 docs
    ├── tx: upsert (table, pk_json, text, embedding) for c/u/r; delete for d
    └── consumer.CommitMessages(msgs...)                     only after tx commits

internal/embed/client.go                                   new
├── Batch(ctx, texts) -> [][]float32                        provider behind an interface; one impl to start
└── Dim() int                                               1536 or 1024, fixed at table creation

internal/api/ask.go                                        new
└── POST /ask {q}
    ├── qv := embed.Batch([q])
    ├── SELECT table, pk_json, text FROM bartie_vectors ORDER BY embedding <=> $1 LIMIT 5
    ├── llm.Answer(system, context rows, q)                  internal/llm/client.go, one small model
    └── -> {answer, sources: [{table, pk, text, dest_updated_at}], retrievedAt}
```

### Data shapes

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE bartie_vectors (
  "table"      text NOT NULL,
  pk           jsonb NOT NULL,
  text         text NOT NULL,
  embedding    vector(1536) NOT NULL,
  __bartie_commit_ts  timestamptz,
  __bartie_updated_at timestamptz DEFAULT now(),
  PRIMARY KEY ("table", pk)
);
CREATE INDEX ON bartie_vectors USING hnsw (embedding vector_cosine_ops);
```

```json
// document built from an observations event
{"table": "public.observations", "pk": {"observation_id": 5512},
 "text": "observation 5512 of animal 42 (lion) at Kambi watering hole, 2026-09-30: seen resting near the south bank ..."}

// POST /ask
{"q": "where was lion 42 last seen?"}
-> {"answer": "At the south bank of Kambi watering hole (observation 5512).",
    "sources": [{"table": "public.observations", "pk": {"observation_id": 5512}, "updatedAt": "2026-10-02T14:03:12Z"}]}
```

### The demo beat

Page section "Live vs batch":

1. Left pane shows one observation's `notes` in an editable box. User changes "south bank" to "north ridge", saves. This is `/demo/poke` with op `u`.
2. Right pane: same question asked twice. **Live** hits `/ask` (vector table is ~2s behind). **Batch** hits `/ask?mode=batch`, which queries `bartie_vectors_snapshot`, a copy refreshed every 5 minutes by a cron in the api. Live says north ridge, batch says south bank, with the snapshot age shown.

The contrast is the pitch. The 5-minute snapshot is generous to batch; real nightly ETL is worse.

### Gotchas known going in

- Embedding dimension is baked into the table. Pick the provider before creating it; changing later is a drop and re-backfill of the vector table (fine, it is derived data).
- Unchanged TOAST columns: an update that does not retransmit `notes` arrives with `notes` in `Unchanged`. The vecwriter cannot embed what it does not have. Two options: read the current text back from the destination tables (the writer has it) or skip re-embedding when no embedded column changed. Do the second; it is also the cheap and correct behavior.
- Deletes carry no `after`. The document key is `(table, pk)`, so delete is fine without text.
- The RAG answer must cite `updatedAt` from the vector row. That timestamp is the proof the answer is fresh; put it on the page.
- Embedding API failures must not advance offsets. Retry with backoff inside the flush, then fail the process. Same posture as a destination outage.
- Cost: terra `tiny` is ~45k observations. One backfill embed run is cents. Nightly reset re-embeds; still cents. Cap `/ask` per IP anyway.

---

## 6c: MCP server

### The idea

artie-mcp is not hand-written tools. `server.py` loads `contract/policy.contract.json` and FastMCP's OpenAPI integration generates one tool per allowed route, then `plugins/artie/` ships a Claude Code plugin with skills that tell the agent how to sequence them. Bartie does the same thing over its own API. Because 6a used their paths, the generated tool names match: `pipeline_list`, `pipeline_detail`, `pipeline_usage`, `pipeline_error_logs`, `pipeline_update_status`, plus `pipeline_verify`.

### Layout

```text
mcp/
├── openapi.yaml                    Bartie's API, hand-written, ~10 paths, operationIds = Artie's tool names
├── server.py                       FastMCP.from_openapi(spec, httpx client -> $API); no auth beyond a bearer env var
├── pyproject.toml
├── tests/test_tools_match.py       every backticked tool in skills/ exists in openapi operationIds (the drift test)
└── plugins/bartie/
    ├── .claude-plugin/plugin.json
    ├── .mcp.json                   {"bartie": {"command": "uv", "args": ["run", "server.py"]}}
    ├── agents/bartie.md            copy artie-mcp.md's structure, Bartie's distinctions
    └── skills/
        ├── monitoring/SKILL.md     their table + the lag-triage rules below
        └── verify/SKILL.md         when to run pipeline_verify and how to read a mismatch
```

### The monitoring skill's triage table (the part their skill cannot have)

```text
readerLag growing, backlog flat, mergeMs flat   -> reader or source: slot blocked, reader down, source WAL burst
readerLag flat, backlog growing, mergeMs flat   -> writer down or writer slower than reader; check error-logs service=writer
backlog growing, mergeMs rising                 -> destination is the bottleneck; MERGE cost, locks, dest CPU
all flat, latency high                          -> flush interval; expected floor is ~flushInterval
```

Report what the numbers say and name the component. Do not pause or resume; hand mutations back to the user, same rule as theirs.

### Gotchas

- FastMCP's `from_openapi` names tools from `operationId`. Set them to Artie's names exactly; do not let it derive names from paths.
- Bartie's `pipeline_usage` returns extra fields. An agent using Artie's skill ignores them; Bartie's skill uses them. Both parse.
- Keep the server stateless and local (stdio). No hosted MCP, no OAuth; that is Artie's product surface, not the demo's.

---

## Components

| Piece | What changes |
| --- | --- |
| `internal/metrics` | new: flush timings + error ring buffers, JSON on a localhost port |
| `internal/writer` | export Buffer + dedupe (or move to shared packages); one RecordFlush call |
| `internal/reader` | RecordError on failure paths; tolerate slot recreation after nightly reset |
| `internal/api` | new: control API, demo endpoints, `/ask` |
| `internal/embed`, `internal/llm` | new: thin provider clients |
| `internal/vecwriter` | new: second consumer group, pgvector upsert |
| `cmd/api`, `cmd/vecwriter` | new binaries |
| `deploy/` | Dockerfile, Caddy, compose additions, VM bootstrap, nightly reset |
| `mcp/` | new: OpenAPI, FastMCP server, plugin, skills, drift test |
| me_me_me `src/app/bartie/live` | the page: poke, live row, latency, ask, live-vs-batch |
| blog post | embed the live page; update limitations |

## Decisions to make before building

- Domain for the api (needs a domain you control for Caddy TLS; the vercel subdomain cannot point at the VM).
- Embedding provider and dimension (fixes the table). Default: a small hosted model, 1536 dims.
- LLM for `/ask`: smallest model that answers from context reliably.
- Whether `pipeline_update_status` (pause/resume) ships in 6a or is cut. It is the only mutation; the page does not need it, the MCP demo is better with it.

## Explicitly out of scope

Schema evolution (phase 5a, still deferred), failover slots, multiple pipelines or tenants, hosted MCP with OAuth, embedding anything beyond the two terra tables, a general chat UI. The api has one pipeline and one job: make the engine visible and pokeable.
