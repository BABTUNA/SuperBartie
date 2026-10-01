# Phase 6: Bartie Live

Goal: Bartie stops being a repo plus a video and becomes a URL. The pipeline runs in containers on one box, a page drives it, and a second destination turns every row change into a vector so an AI answer reflects a source edit within seconds. The whole thing is also operable from Claude Code through an MCP server built on Artie's tool contract.

Three sub-phases, all built. This doc describes the code as it is, not the plan.

- **6a Control API + deployment.** `cmd/api`, `internal/metrics`, the compose `live` profile, VM scripts.
- **6b Vector destination + RAG.** `cmd/vecwriter`, `internal/embed`, `internal/llm`, `POST /ask`.
- **6c MCP server.** `mcp/`, a Claude Code plugin, two skills, a drift test.

The reader and writer logic is unchanged. The writer gained a timing call per flush and a pause check; the reader gained a phase marker. `Buffer`, `Dedupe`, and `Representative` moved from `internal/writer` to `internal/batch` so both consumers share them.

Done when (all verified locally against the containerized stack):

```bash
docker compose -f deploy/docker-compose.yml --profile live up -d --build
curl -s localhost:8088/pipelines/bartie/usage                                # 6a: latency + reader lag + backlog + merge ms
curl -s -X POST localhost:8088/demo/poke -d '{"op":"u", ...}'                 # 6a: source row changes, lands in ~2s
curl -s -X POST 'localhost:8088/pipelines/bartie/verify?timeout=10s'          # 6a: {"match": true, ...}
curl -s -X POST localhost:8088/ask -d '{"q":"where was Mosi last seen?"}'     # 6b: answer reflects an edit made 2s ago
BARTIE_API_URL=http://localhost:8088 uv run --directory mcp python tests/smoke_client.py   # 6c: 7 tools, all callable
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

---

## 6a: Control API + deployment

### The idea

One HTTP server that makes the pipeline visible and pokeable. Paths mirror Artie's public API where one exists so 6c can generate MCP tools with their names. Bartie has one pipeline; its uuid is the literal string `bartie`.

`usage` goes past Artie's. Theirs returns per-table `{count, latency}`. Bartie's also says where the latency is:

```text
readerLagBytes   = pg_wal_lsn_diff(pg_current_wal_lsn(), slot.confirmed_flush_lsn)   source is ahead of reader
backlogMessages  = sum over partitions(end offset - bartie-writer committed offset)  reader is ahead of writer
mergeMs          = p50/p95/last of the writer's flushTable durations                 destination is slow
```

Three numbers, three possible bottlenecks. The MCP monitoring skill reads them.

The api never shares a pool with the pipeline and never touches the replication connection. It reads the source and destination over ordinary connections, and learns about the processes two ways: each process serves a JSON snapshot on its own metrics port, and each appends errors to a shared JSONL file that outlives the process.

### Function tree

```text
cmd/api/main.go
├── embed.New(cfg)                                         nil when MINICDC_EMBED_PROVIDER=none
├── api.NewRAG(ctx, destDSN, emb, llm.New(cfg), interval)  only when an embedder exists (6b)
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
│   └── POST /ask {q, mode}                                handleAsk -> Asker.Ask (ask.go, 6b)                 [rate limit]
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

### Data shapes

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

### Deploy

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

### What the traffic writer does

Every `TRAFFIC_INTERVAL` seconds (default 2): insert an observation of a random seeded animal at a random watering hole with notes like "Ranger log: Mosi the Watchful the hippopotamus seen resting in a pair at Mopane Crossing, the reed bed." Every 4th write flips a seeded animal's status; every 7th deletes a sighting from earlier in the run. Observation ids start at 3e9 in a per-run block, clear of the seed, `load.sh`, and the page's demo row. Before each write it waits while `traffic.paused` exists in the shared volume, which is how `POST /demo/traffic` switches it off.

### Gotchas, as found

- `docker kill` is a manual stop: the restart policy does not bring the container back. A process crash inside the container does restart it. Test crashes with a signal from inside, not `docker kill`.
- A single verify pass a moment after a write mismatches, correctly: events are in flight. That is why the endpoint has `timeout`.
- `readerLagBytes` has a floor of a few hundred bytes to a few hundred KB from WAL records that are not row changes, and the standby status update runs every 5s. Flat and small is healthy; growing is the signal.
- `mergeMs` lives in the writer's memory. A writer restart resets it to zero samples until the next flush.
- A `kill -9` leaves nothing in `errors.jsonl`. The evidence is `processes: {"writer": "unreachable"}`.
- The nightly reset drops every volume, so the slot goes too and the reader comes up on the phase 4 fresh-slot path (export snapshot, backfill, stream). That path runs every night.
- Only the `reader` service has a `build:` key; the others reuse `image: bartie:local`. `up -d --build <service>` on another service does not rebuild. Use `up -d --build` with no service name.
- `/demo/poke` writes to a public database. Three fences: table and column allowlist, pk range 9000..9099, per-IP rate limit. Never raw SQL.

---

## 6b: Vector destination + RAG

### The idea

Artie's argument: an AI reading a nightly copy gives confidently stale answers; CDC fixes it. This is that as a running thing. A second consumer group on the same topic embeds rows and upserts vectors into pgvector. `POST /ask` does retrieval plus one model call. `mode=live` reads the vector table; `mode=batch` reads a snapshot copy the api refreshes on a timer.

Consumer group `bartie-vecwriter`, independent offsets from the writer. Same crash contract: offsets commit only after the pgvector transaction commits. Embedding runs before the transaction, so a crash in between re-embeds on replay.

### Function tree

```text
cmd/vecwriter/main.go
├── embed.New(cfg)                                         nil -> log "disabled" and exit 0
├── metrics.Serve(:9103, false)
└── vecwriter.New(ctx, cfg, emb).Run(ctx)

internal/vecwriter/vecwriter.go
├── New
│   ├── ensureSchema(ctx, pool, emb.Dim())                 CREATE EXTENSION vector; CREATE TABLE bartie_vectors; hnsw index;
│   │                                                      refuses to start if the column's dim differs from the embedder's
│   └── kafka.NewReader{GroupID: "bartie-vecwriter", StartOffset: FirstOffset}
├── Run(ctx)                                               same buffer loop as writer.Run: flush at 200 events or 2s
└── flushAll(ctx)
    ├── batch.Dedupe(evts) per table                       internal/batch
    ├── skip tables not in embeddedColumns                 only animals and observations are embedded
    ├── op d -> queue DELETE by ("table", pk)
    ├── render(ctx, lookups, evt) -> document              document.go
    ├── emb.Embed(ctx, texts, false) in chunks of 64       outside the transaction
    ├── tx: pgx.Batch of INSERT ... ON CONFLICT DO UPDATE + DELETEs
    ├── commit(ctx, msgs, applied, start)                  consumer.CommitMessages, then metrics.RecordFlush
    └── nothing embeddable in the batch -> still commit offsets

internal/vecwriter/document.go
├── embeddedColumns                                        animals: name, species, status, home_watering_hole_id
│                                                          observations: animal_id, watering_hole_id, observed_at, notes
├── render(ctx, lk, evt) -> (document, ok)
│   ├── skipUnchanged(evt, cols)                           update whose embedded column is TOAST-unchanged -> skip
│   ├── animals      "Mosi is a blue wildebeest, status adult, animal id 1. Home: Acacia Pan (watering hole 1)."
│   └── observations "Observation 9001 of Mosi (blue wildebeest, animal 1) at Acacia Pan (watering hole 1) on <ts>: <notes>"
└── lookups.animal(id) / lookups.hole(id)                  read names from the DESTINATION replica tables, cached per flush;
                                                           a miss falls back to "animal 42" / "watering hole 7"

internal/embed/embed.go
├── Embedder interface { Embed(ctx, texts, query) ; Dim() ; Name() }
├── New(cfg)                                               none -> nil | hash | openai | voyage
├── hashEmbedder                                           feature-hashing bag of words + bigrams, L2 normalized; no key needed
├── httpEmbedder                                           POST {model, input} -> data[].embedding; 3 tries on 429/5xx;
│                                                          errors if the model's dim != MINICDC_EMBED_DIM
└── Literal(v)                                             "[0.1,0.2,...]" for $n::vector

internal/llm/llm.go
├── Client interface { Answer(ctx, question, contexts) ; Model() }
├── New(cfg)                                               key set -> claude | no key -> extractive
├── claude.Answer                                          anthropic-sdk-go Messages.New, effort low, records-only system prompt
└── extractive.Answer                                      "Closest record: <top context>"

internal/api/ask.go
├── NewRAG(ctx, destDSN, emb, llm, snapshotEvery)          own pool; starts snapshotLoop
├── Ask(ctx, question, mode)
│   ├── emb.Embed([q], true)
│   ├── SELECT ... FROM bartie_vectors | bartie_vectors_snapshot ORDER BY embedding <=> $1 LIMIT 5
│   ├── llm.Answer(q, texts)
│   └── asOf: live = newest source row's updatedAt; batch = snapshot time, plus staleBy seconds
├── snapshotLoop(ctx)                                      first refresh 20s after start, then every MINICDC_SNAPSHOT_INTERVAL
└── refreshSnapshot(ctx)                                   one tx: CREATE IF NOT EXISTS (LIKE), TRUNCATE, INSERT SELECT, stamp _meta
```

### Data shapes

```sql
CREATE TABLE bartie_vectors (
  "table"             text NOT NULL,
  pk                  jsonb NOT NULL,          -- canonical PK map, e.g. {"observation_id": 9001}
  text                text NOT NULL,           -- the document that was embedded
  embedding           vector(1536) NOT NULL,   -- dimension = MINICDC_EMBED_DIM
  __bartie_commit_ts  timestamptz,
  __bartie_updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("table", pk)
);
CREATE INDEX bartie_vectors_embedding_idx ON bartie_vectors USING hnsw (embedding vector_cosine_ops);
-- bartie_vectors_snapshot: same shape, no index; bartie_vectors_snapshot_meta(id, refreshed_at)
```

```json
// POST /ask
{"q": "where was Mosi last seen? (observation 9001)", "mode": "live"}
->
{
  "question": "...", "mode": "live",
  "asOf": "2026-09-30T04:47:27.694Z", "staleBy": null,
  "answer": "Closest record: Observation 9001 of Mosi (blue wildebeest, animal 1) at Acacia Pan ...",
  "sources": [{"table": "public.observations", "pk": {"observation_id": 9001}, "text": "...", "score": 0.235, "updatedAt": "..."}],
  "model": "extractive (no LLM key configured)", "embedder": "hash",
  "latencyMs": 11, "answeredAt": "..."
}
// mode=batch: asOf is the snapshot time and staleBy is seconds since it
```

### Providers

| Setting | Values | Without a key |
| --- | --- | --- |
| `MINICDC_EMBED_PROVIDER` | `hash` (default), `openai`, `voyage`, `none` | `hash` works with none: keyword overlap, not semantics |
| `MINICDC_EMBED_MODEL`, `MINICDC_EMBED_DIM` | e.g. `text-embedding-3-small`, `1536` | dim is fixed at table creation |
| `MINICDC_LLM_MODEL`, `MINICDC_LLM_API_KEY` | default `claude-opus-5-5` | no key: the top record is returned verbatim |
| `MINICDC_SNAPSHOT_INTERVAL` | default `5m` | |

### Gotchas, as found

- The embedding dimension is baked into the column. Changing provider or dim means dropping `bartie_vectors`; the vecwriter refuses to start on a mismatch rather than failing every insert.
- An update that does not retransmit `notes` (TOAST-unchanged) cannot be re-embedded and does not need to be. It is skipped.
- Deletes carry no `after`; the key is `("table", pk)`, so a delete needs no text.
- Enrichment reads the destination's replica tables. The two consumers are not ordered against each other, so an observation can arrive before its animal; the document then says "animal 42" until the row is next touched. Deleting an animal does not rewrite the documents that mention it.
- `pipeline_usage` must not count the vector tables as replicated tables. `destTables` excludes `bartie_vectors*`.
- With the hash embedder, "which animals are lions?" retrieves poorly and "observation 9001 Mosi" retrieves exactly. Good enough for the live-vs-batch contrast; a real embedding key fixes the rest.
- Embedding failures return an error from the flush, so offsets do not advance. Same posture as a destination outage.

---

## 6c: MCP server

### The idea

artie-mcp is not hand-written tools. Its `server.py` loads a policy contract and FastMCP generates one tool per allowed route of Artie's OpenAPI spec; `plugins/artie/` ships a Claude Code plugin whose skills tell the agent how to sequence them. Bartie does the same over its own API. Because 6a used Artie's paths, the operationIds are Artie's tool names.

### Layout

```text
.claude-plugin/marketplace.json     repo root: `claude plugin marketplace add BABTUNA/Bartie`
mcp/
├── openapi.yaml                    hand-written contract, 7 operations, operationId = tool name
├── server.py                       FastMCP.from_openapi(spec, httpx client -> $BARTIE_API_URL), stdio
├── pyproject.toml, uv.lock         fastmcp 4, httpx, pyyaml
├── tests/
│   ├── test_tools_match.py         skills <-> contract drift test (both directions)
│   └── smoke_client.py             stdio client: list tools, call the six read-only ones against a live api
└── plugins/bartie/
    ├── .claude-plugin/plugin.json
    ├── .mcp.json                   uv run --directory ${CLAUDE_PLUGIN_ROOT}/../.. python server.py
    ├── agents/bartie.md
    └── skills/
        ├── monitoring/SKILL.md     which call answers which question + the bottleneck triage table
        └── verify/SKILL.md         when to pass timeout, how to read a mismatch
```

### Tools

| Tool | Route | Artie has it |
| --- | --- | --- |
| `pipeline_list` | `GET /pipelines` | yes |
| `pipeline_detail` | `GET /pipelines/{uuid}` | yes |
| `pipeline_usage` | `GET /pipelines/{uuid}/usage` | yes, without the three lag fields |
| `pipeline_error_logs` | `GET /pipelines/{uuid}/error-logs` | yes |
| `pipeline_update_status` | `POST /pipelines/{uuid}/status` | yes; the only mutation |
| `pipeline_verify` | `POST /pipelines/{uuid}/verify` | no: their product cannot read the destination |
| `destination_ask` | `POST /ask` | no |

The `/demo/*` routes are deliberately not in the contract. An agent has no business writing to the source.

### The monitoring skill's triage table

```text
readerLagBytes growing, backlog flat, mergeMs flat   -> reader or source: slot blocked, reader down, write burst
readerLagBytes flat, backlog growing, mergeMs flat   -> writer paused or down; check processes.writer and error logs
backlog growing, mergeMs rising                      -> destination is the bottleneck
all flat, latency ~1-2s                              -> healthy; that is the flush-interval floor
mergeMs null, writerReachable false                  -> writer metrics endpoint down; status is unknown, not paused
```

Trends need two calls a minute apart. The skill says so, and stops at diagnosis: it never calls `pipeline_update_status`.

### The drift test

`test_tools_match.py` parses backticked snake_case names out of the skills and the agent prompt and checks them against the operationIds, in both directions: every tool a skill names must exist, and every tool in the contract must be mentioned somewhere. It exists because artie-mcp's monitoring skill kept telling agents there was no error-log tool after `pipeline_error_logs` had shipped in its contract.

### Gotchas, as found

- FastMCP names tools from `mcp_names`, keyed by operationId. Without it the names are derived from paths.
- Stdio subprocesses do not inherit the parent's environment. The smoke client passes `BARTIE_API_URL` explicitly, as the plugin's `.mcp.json` does.
- A response schema using `allOf` (`PipelineDetail`) comes back wrapped as `{"result": {...}}`. Harmless; agents read through it.
- The server is local and stateless. No hosted endpoint, no OAuth; that is Artie's product surface.

---

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

---

## Components

| Piece | State |
| --- | --- |
| `internal/metrics` | new: flush ring, phase, pause flag, JSONL error log, per-process HTTP snapshot |
| `internal/api` | new: control API, demo endpoints, table browser, traffic switch, `/ask` |
| `internal/batch` | new home of `Buffer`, `Dedupe`, `PKKey`, `Representative` (moved from `internal/writer`) |
| `internal/vecwriter`, `internal/embed`, `internal/llm` | new: second consumer, embedders, answerer |
| `internal/verify` | `Compare` and `Converge` return structure; `Run` prints |
| `internal/writer`, `internal/reader` | timing, pause check, phase marker; apply and decode logic untouched |
| `cmd/api`, `cmd/vecwriter` | new binaries |
| `deploy/` | Dockerfile, Caddyfile, compose `live` profile, `.env.example`, `vm/` scripts |
| `scripts/demo-traffic.sh` | new |
| `mcp/`, `.claude-plugin/` | new |

Tests added: `internal/api` (pk fence, rate limiter, window parsing, uuid guard), `internal/metrics` (percentiles, error log round trip, pause), `internal/embed` (ranking, determinism, literal), `internal/vecwriter` (TOAST skip, rendering), `mcp/tests` (drift, smoke).

## Not done

- **Not deployed.** Everything above runs on a laptop. Going live needs a domain for `API_DOMAIN`, a VM running `deploy/vm/bootstrap.sh`, `NEXT_PUBLIC_BARTIE_API_URL` on Vercel, and the site origin in `MINICDC_CORS_ORIGINS`.
- **Keys are optional and unset.** Hash embeddings and extractive answers until `MINICDC_EMBED_API_KEY` and `MINICDC_LLM_API_KEY` are provided.

## Explicitly out of scope

Schema evolution (phase 5a, still deferred), failover slots, multiple pipelines or tenants, a hosted MCP with OAuth, embedding anything beyond the two terra tables, a crash button on the page, latency charts over time.
