# Phase 7: Vector Destination and Live RAG

Goal: a second destination on the same change stream. Every row change becomes an embedding in pgvector, so a question asked of the replicated data reflects a source edit within seconds. Next to it sits a snapshot refreshed on a timer, so the same question can be asked of "live" and "batch" and the answers compared.

This doc describes the code as it is. It builds on the [control API](phase-6-control-api.md), which serves `/ask`, and it does not touch the reader or the writer. `Buffer`, `Dedupe`, and `Representative` moved from `internal/writer` to `internal/batch` so both consumers share them.

Done when (verified locally):

```bash
docker compose -f deploy/docker-compose.yml --profile live up -d --build
curl -s -X POST localhost:8088/demo/poke -d '{"op":"u","table":"public.observations","pk":{"observation_id":9001},"set":{"notes":"... NORTH ridge ..."}}'
curl -s -X POST localhost:8088/ask -d '{"q":"where was Mosi last seen? (observation 9001)","mode":"live"}'    # says north, asOf seconds ago
curl -s -X POST localhost:8088/ask -d '{"q":"where was Mosi last seen? (observation 9001)","mode":"batch"}'   # still the old snapshot
```

```text
Redpanda topic cdc.events ──┬──▶ writer (group bartie-writer) ───────▶ dest pg: replica tables
                            └──▶ vecwriter (group bartie-vecwriter) ─▶ dest pg: bartie_vectors
                                                                              │  copied every 5m
                                                                              ▼
                              api POST /ask ── mode=live ──▶ bartie_vectors   bartie_vectors_snapshot ◀── mode=batch
```

## The idea

Artie's argument: an AI reading a nightly copy gives confidently stale answers; CDC fixes it. This is that as a running thing. A second consumer group on the same topic embeds rows and upserts vectors into pgvector. `POST /ask` does retrieval plus one model call. `mode=live` reads the vector table; `mode=batch` reads a snapshot copy the api refreshes on a timer.

Consumer group `bartie-vecwriter`, independent offsets from the writer. Same crash contract: offsets commit only after the pgvector transaction commits. Embedding runs before the transaction, so a crash in between re-embeds on replay.

## Function tree

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

## Data shapes

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

## Providers

| Setting | Values | Without a key |
| --- | --- | --- |
| `MINICDC_EMBED_PROVIDER` | `hash` (default), `openai`, `voyage`, `none` | `hash` works with none: keyword overlap, not semantics |
| `MINICDC_EMBED_MODEL`, `MINICDC_EMBED_DIM` | e.g. `text-embedding-3-small`, `1536` | dim is fixed at table creation |
| `MINICDC_LLM_MODEL`, `MINICDC_LLM_API_KEY` | default `claude-opus-5-5` | no key: the top record is returned verbatim |
| `MINICDC_SNAPSHOT_INTERVAL` | default `5m` | |

## Gotchas, as found

- The embedding dimension is baked into the column. Changing provider or dim means dropping `bartie_vectors`; the vecwriter refuses to start on a mismatch rather than failing every insert.
- An update that does not retransmit `notes` (TOAST-unchanged) cannot be re-embedded and does not need to be. It is skipped.
- Deletes carry no `after`; the key is `("table", pk)`, so a delete needs no text.
- Enrichment reads the destination's replica tables. The two consumers are not ordered against each other, so an observation can arrive before its animal; the document then says "animal 42" until the row is next touched. Deleting an animal does not rewrite the documents that mention it.
- `pipeline_usage` must not count the vector tables as replicated tables. `destTables` excludes `bartie_vectors*`.
- With the hash embedder, "which animals are lions?" retrieves poorly and "observation 9001 Mosi" retrieves exactly. Good enough for the live-vs-batch contrast; a real embedding key fixes the rest.
- Embedding failures return an error from the flush, so offsets do not advance. Same posture as a destination outage.

## Components

| Piece | State |
| --- | --- |
| `internal/vecwriter` | new: second consumer group, document rendering, pgvector upsert |
| `internal/embed` | new: `hash`, `openai`, `voyage` embedders behind one interface |
| `internal/llm` | new: Claude answerer and a no-key extractive fallback |
| `internal/api/ask.go` | new: retrieval, answer, batch snapshot loop |
| `internal/batch` | new home of `Buffer`, `Dedupe`, `PKKey`, `Representative` (moved from `internal/writer`) |
| `cmd/vecwriter` | new binary; compose service `vecwriter` |
| `deploy/docker-compose.yml` | destination image is `pgvector/pgvector:pg16` |

Tests added: `internal/embed` (ranking, determinism, literal), `internal/vecwriter` (TOAST skip, rendering), `internal/batch` (the dedupe tests, moved).

## Not done

**Keys are optional and unset.** Hash embeddings and extractive answers until `MINICDC_EMBED_API_KEY` and `MINICDC_LLM_API_KEY` are provided.

## Explicitly out of scope

Embedding anything beyond the two terra tables, re-rendering documents when a row they were enriched from changes or is deleted, chunking long text, a general chat UI.
