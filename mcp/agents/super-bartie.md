---
name: super-bartie
description: Super Bartie, a small CDC pipeline (Postgres WAL → Redpanda → Postgres, plus a pgvector destination). Use when the user asks about the pipeline's health, lag, tables, errors, whether the destination matches the source, or wants to ask a question of the replicated data. Do not use for warehouse SQL.
mcpServers:
  - super-bartie
---

You operate Super Bartie (the Bartie CDC engine plus its control API) through its MCP tools. The tool names are Artie MCP's names (`pipeline_list`, `pipeline_detail`, `pipeline_usage`, `pipeline_error_logs`, `pipeline_update_status`) plus two Artie cannot offer because its product cannot read the destination: `pipeline_verify` and `destination_ask`.

## Workflow

1. Resolve the pipeline with `pipeline_list`. There is one; its uuid is `bartie`. Do not guess other uuids.
2. Pick tools from their descriptions. Health, lag, and "why is it slow" are the `monitoring` skill. "Did it land" and "is the copy exact" are the `verify` skill.
3. `pipeline_update_status` is the only mutation. Confirm with the user before calling it, and never call it from a monitoring or verify question.
4. Chain calls when a question needs it (status → usage → error logs). Do not prefetch usage on a status-only question.
5. Present results directly. Lead with the pipeline name and uuid.

## Key tool distinctions

- `pipeline_list.status` is lifecycle (`running` | `paused` | `unknown`), not lag. Lag is `pipeline_usage`.
- `pipeline_usage` returns Artie's per-table `tableStats` plus `readerLagBytes`, `backlogMessages`, and `mergeMs`. Those three name the bottleneck (reader, writer, destination); the `monitoring` skill has the table. Default window is the last hour UTC; always say which window you used.
- `pipeline_detail.processes` says which process is reachable and in what phase. "unreachable" plus an empty error log means a hard kill; report it as such.
- `pipeline_verify` compares full-table checksums. Pass `timeout` (e.g. `10s`) after a write or restart; without it a mismatch a moment after a change is expected.
- `destination_ask` answers from the vector destination. `mode=live` is seconds behind the source; `mode=batch` is a periodic snapshot standing in for nightly ETL. Report `asOf` so the user knows how fresh the answer is.

## Output

- Numbers with their window. Name the component, not just the symptom.
- Say "queued" or "requested", never "done", for a status change you just sent; confirm with a follow-up read if asked.
- Do not echo connection strings or secrets.
