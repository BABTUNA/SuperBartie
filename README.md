# Super Bartie

Super Bartie is a follow-up to [Bartie](https://github.com/BABTUNA/Bartie), a small Postgres CDC engine I built to understand how [Artie](https://artie.com) works. Bartie reads a Postgres write-ahead log and keeps a copy of the tables in a second database.

This repo keeps that engine as it was and adds the parts around it: an HTTP API, a second destination that stores vectors, an MCP server, and a deployment.

- **Live demo:** [babtuna.vercel.app/super-bartie/live](https://babtuna.vercel.app/super-bartie/live)
- **Both databases side by side:** [babtuna.vercel.app/super-bartie/data](https://babtuna.vercel.app/super-bartie/data)
- **Writeup:** [babtuna.vercel.app/blog/super-bartie](https://babtuna.vercel.app/blog/super-bartie)

### ▶ Demo video

<p align="center">
  <a href="https://www.youtube.com/watch?v=zmyBYuo_dT8">
    <img src="https://img.youtube.com/vi/zmyBYuo_dT8/maxresdefault.jpg" width="640" alt="Watch the Super Bartie demo">
  </a>
</p>

## Architecture

![Super Bartie architecture](docs/assets/architecture.png)

Diagram source: [docs/assets/architecture.drawio](docs/assets/architecture.drawio). The source data is Artie's [terra](https://github.com/artie-labs/terra) demo dataset, run unmodified.

## What it adds

### An HTTP API

`cmd/api` reports on the running pipeline and lets you control it. The URL layout follows Artie's public API.

| Endpoint | What it does |
| --- | --- |
| `GET /pipelines`, `GET /pipelines/{uuid}` | Status, tables, replication slot, and which processes are up |
| `GET /pipelines/{uuid}/usage` | Latency per table, plus reader lag, backlog, and merge time, which show which part is slow |
| `GET /pipelines/{uuid}/error-logs` | What failed and in which process |
| `POST /pipelines/{uuid}/verify` | Compares checksums of every table in both databases |
| `POST /pipelines/{uuid}/status` | Pauses or resumes the writer |
| `POST /ask` | Answers a question from the replicated data |

There is one pipeline and its uuid is `bartie`. More in [docs/phase-6-control-api.md](docs/phase-6-control-api.md).

### A vector destination

`cmd/vecwriter` is a second consumer on the same Redpanda topic. For each changed row in `animals` and `observations` it builds a sentence, embeds it, and stores it in pgvector. It commits Kafka offsets after the database transaction, the same way the writer does.

`POST /ask` finds rows with a keyword search and a vector search, merges them, and has Claude answer from those rows. `mode=live` searches the vector table. `mode=batch` searches a snapshot that refreshes every five minutes, so the same question can be asked of fresh and stale data.

It runs with no API keys. The default embedder is a keyword hash, and without an Anthropic key `/ask` returns the closest row instead of a sentence. More in [docs/phase-7-vector-destination.md](docs/phase-7-vector-destination.md).

### An MCP server

`mcp/` is generated from `mcp/openapi.yaml`, which is how [artie-mcp](https://github.com/artie-labs/artie-mcp) builds its tools. The tool names match Artie's where the endpoints match (`pipeline_list`, `pipeline_detail`, `pipeline_usage`, `pipeline_error_logs`, `pipeline_update_status`). Two are its own: `pipeline_verify` and `destination_ask`.

It installs as a Claude Code plugin with two skills:

```bash
claude plugin marketplace add BABTUNA/SuperBartie
```

```bash
claude plugin install super-bartie@super-bartie
```

Then ask "is my pipeline behind?" It talks to `http://localhost:8088` unless `BARTIE_API_URL` is set. More in [docs/phase-8-mcp.md](docs/phase-8-mcp.md).

## Run it

Needs Docker.

```bash
./deploy/fetch-terra.sh
```

```bash
docker compose -f deploy/docker-compose.yml --profile live up -d --build
```

That starts the two databases, Redpanda, the reader, writer, vecwriter, and API, a script that writes a row every two seconds, and Caddy. The API is at `http://localhost:8088`.

```bash
curl -s localhost:8088/pipelines/bartie/usage
```

```bash
curl -s -X POST 'localhost:8088/pipelines/bartie/verify?timeout=10s'
```

```bash
curl -s -X POST localhost:8088/ask -d '{"q": "which animals are lions?"}'
```

For sentence answers from `/ask`, copy `deploy/.env.example` to `deploy/.env` and set `MINICDC_LLM_API_KEY`.

## Deploy it

It runs on one Ubuntu VM with 4 GB of RAM. As root:

```bash
curl -fsSL https://raw.githubusercontent.com/BABTUNA/SuperBartie/main/deploy/vm/bootstrap.sh | bash
```

That installs Docker and clones the repo to `/opt/superbartie`. Set `API_DOMAIN` and the other values in `/opt/superbartie/deploy/.env`, then run:

```bash
/opt/superbartie/deploy/vm/redeploy.sh
```

Caddy gets the TLS certificate. Only ports 80 and 443 are open. A cron job resets the data every night.

## The engine

The reader, writer, backfill, and `cdcctl verify` come from Bartie unchanged. The [Bartie README](https://github.com/BABTUNA/Bartie) covers how they work, the crash and backfill tests, and the benchmark against batch copies. Those scripts are in `scripts/` here too.

## Docs

One doc per phase in [docs/](docs/), each with a function tree and the data shapes:

| Phase | Doc |
| --- | --- |
| 1 to 5, the engine | [pipeline](docs/phase-1-pipeline.md), [correctness](docs/phase-2-correctness.md), [backfill](docs/phase-4-backfill.md), [benchmarks](docs/phase-5-evolve-bench.md) |
| 6 | [control API and deployment](docs/phase-6-control-api.md) |
| 7 | [vector destination](docs/phase-7-vector-destination.md) |
| 8 | [MCP server](docs/phase-8-mcp.md) |
