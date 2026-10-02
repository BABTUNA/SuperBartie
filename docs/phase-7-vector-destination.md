# Phase 7: Vector Destination and Live RAG

Goal: a second destination on the same change stream. Every row change becomes an embedding in pgvector, so a question asked of the copy reflects a source edit within seconds. The writer and reader do not change.

Done when:

```bash
curl -s -X POST localhost:8088/demo/poke -d '{"op":"u","table":"public.observations","pk":{"observation_id":9001},"set":{"notes":"... NORTH ridge ..."}}'
curl -s -X POST localhost:8088/ask -d '{"q":"where was Mosi last seen? (observation 9001)","mode":"live"}'    # says north
curl -s -X POST localhost:8088/ask -d '{"q":"where was Mosi last seen? (observation 9001)","mode":"batch"}'   # still the old answer
```

## The core idea

An AI reading a nightly copy answers with yesterday. To show it, keep two copies and ask both:

```text
Redpanda topic ──┬──▶ writer    (group bartie-writer)    ──▶ replica tables
                 └──▶ vecwriter (group bartie-vecwriter) ──▶ bartie_vectors ──copied every 5m──▶ bartie_vectors_snapshot
                                                                  ▲                                      ▲
                                                           /ask mode=live                         /ask mode=batch
```

The vecwriter is a second consumer group with its own offsets. It follows the writer's rule: commit offsets only after the destination transaction commits. Embedding runs before the transaction, so a crash in between re-embeds on replay.

## Function tree

```text
cmd/vecwriter/main.go
└── vecwriter.New(ctx, cfg, emb).Run(ctx)                 internal/vecwriter/vecwriter.go
    ├── ensureSchema(dim)                                 CREATE EXTENSION vector, table, hnsw index
    └── flushAll(ctx)                                     every 200 events or 2s
        ├── batch.Dedupe(evts)                            internal/batch (moved from internal/writer)
        ├── render(evt) -> document                       internal/vecwriter/document.go
        │   ├── skipUnchanged(evt)                        TOAST-unchanged text: nothing to re-embed
        │   └── lookups.animal(id), lookups.hole(id)      names read from the destination replica tables
        ├── emb.Embed(texts)                              internal/embed, chunks of 64, outside the tx
        ├── tx: upsert (table, pk, text, embedding), delete for op d
        └── consumer.CommitMessages(msgs)                 only after the tx commits

internal/api/ask.go
├── Ask(question, mode)
│   ├── keywordSearch(question)                           rows that share words with the question, rare words count more
│   ├── emb.Embed([question]) + ORDER BY embedding <=> $1 the five nearest vectors
│   ├── merge, keyword hits first, each row once          bartie_vectors or bartie_vectors_snapshot
│   └── llm.Answer(question, texts)                       internal/llm, each row clipped to 700 chars
└── snapshotLoop()                                        TRUNCATE + INSERT SELECT every MINICDC_SNAPSHOT_INTERVAL
```

## Core data shapes

### 1. The document that gets embedded

```text
Observation 9001 of Mosi (blue wildebeest, animal 1) at Acacia Pan (watering hole 1) on <ts>: <notes>
Mosi is a blue wildebeest, status adult, animal id 1. Home: Acacia Pan (watering hole 1).
```

One per row of `observations` and `animals`. The header names the entity so "observation 9001" or "Mosi" retrieves it.

### 2. The vector table

```sql
CREATE TABLE bartie_vectors (
  "table"   text NOT NULL,
  pk        jsonb NOT NULL,          -- {"observation_id": 9001}
  text      text NOT NULL,
  embedding vector(1536) NOT NULL,   -- MINICDC_EMBED_DIM
  __bartie_commit_ts timestamptz, __bartie_updated_at timestamptz,
  PRIMARY KEY ("table", pk)
);
```

### 3. Ask

```json
{"q": "where was Mosi last seen? (observation 9001)", "mode": "live"}
-> {"answer": "...", "mode": "live", "asOf": "2026-09-30T04:47:27Z", "staleBy": null,
    "sources": [{"table": "public.observations", "pk": {"observation_id": 9001}, "text": "...", "score": 0.23}],
    "model": "...", "embedder": "hash"}
```

In batch mode `asOf` is the snapshot time and `staleBy` is seconds since it.

### 4. Providers

| Setting | Values | With no key |
| --- | --- | --- |
| `MINICDC_EMBED_PROVIDER` | `hash` (default), `openai`, `voyage`, `none` | `hash` works: keyword overlap, not semantics |
| `MINICDC_LLM_API_KEY` | an Anthropic key | the top record is returned verbatim |

## Gotchas found while building

- The embedding dimension is baked into the column. The vecwriter refuses to start on a mismatch instead of failing every insert.
- An update that does not resend `notes` (TOAST-unchanged) is skipped: the text did not arrive and did not change.
- The two consumers are not ordered against each other. An observation can arrive before its animal, and the document then says "animal 42" until the row is next touched.
- `pipeline_usage` must not count `bartie_vectors` as a replicated table.
- Vector search alone lost the demo row once the table passed a few thousand rows, and plain word-frequency ranking put long seed rows above it. `/ask` now also runs a keyword search that weights each word by how few rows contain it, so "9001" outweighs "seen", and merges the two. That fixed both "observation 9001" and "which animals are lions?" with the keyword-hash embedder.

## Components

| Piece | What changes |
| --- | --- |
| `internal/vecwriter`, `cmd/vecwriter` | new: second consumer, document rendering, pgvector upsert |
| `internal/embed` | new: `hash`, `openai`, `voyage` behind one interface |
| `internal/llm` | new: Claude answerer and a no-key fallback |
| `internal/api/ask.go` | new: retrieval, answer, snapshot loop |
| `internal/batch` | `Buffer` and `Dedupe` moved here so both consumers share them |
| `deploy/docker-compose.yml` | destination image is `pgvector/pgvector:pg16` |

## Explicitly out of scope

Embedding more than the two terra tables, re-rendering a document when a row it was enriched from changes or is deleted, chunking long text, a general chat UI.
