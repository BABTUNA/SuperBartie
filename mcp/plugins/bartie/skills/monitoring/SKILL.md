---
name: monitoring
description: >
  Reports Bartie pipeline health and names the bottleneck: status, per-table
  lag, rows processed, where the latency is (reader, broker, or destination),
  and recent errors. Use when the user asks whether the pipeline is healthy,
  stuck, behind, caught up, why it is slow, or wants lag or row counts, even
  without saying "monitor". Read-only: never pauses or resumes. Do not use to
  prove a specific row landed; that is `verify`.
---

## Monitoring the pipeline

Resolve the pipeline with `pipeline_list` (Bartie runs one; its uuid is `bartie`). Call only what the question needs.

| Question | Call |
|---|---|
| Running, paused, backfilling? | `pipeline_list` |
| Which tables, is a process down, slot active? | `pipeline_detail` |
| Lag, throughput, rows processed? | `pipeline_usage` (default window: last hour UTC; always state the window) |
| Why is it slow? | `pipeline_usage`, then the triage table below |
| What broke? | `pipeline_error_logs` |
| Overall health | `pipeline_list` → `pipeline_detail` → `pipeline_usage` → `pipeline_error_logs` if anything looks off |

## Naming the bottleneck

Artie's `pipeline_usage` returns per-table latency. Bartie's returns the same `tableStats` plus three numbers that say where the time goes. Read them together:

| readerLagBytes | backlogMessages | mergeMs.p95 | Bottleneck | Say |
|---|---|---|---|---|
| growing | flat, near 0 | flat | reader or source | "The reader is behind the source WAL: slot blocked, reader down, or a write burst on the source. Check `pipeline_detail.processes.reader` and `slot.active`." |
| flat | growing | flat | writer | "Events are reaching the broker but the writer is not draining them: writer paused or down. Check `pipeline_detail.processes.writer` and `pipeline_error_logs` for service=writer." |
| flat | growing | rising | destination | "The writer is running but each MERGE is slow: destination load, locks, or a large batch. p95 merge went from X ms to Y ms." |
| flat | flat, near 0 | flat, latency ≈ 1-2 s | none | "Healthy. Latency is at the flush-interval floor (~2 s)." |
| any | any | `mergeMs` null, `writerReachable` false | writer unreachable | "The writer's metrics endpoint is down. Status is unknown, not paused." |

Report the numbers you saw and name the component. Trends need two calls a minute apart; one sample cannot show "growing". Say so when you only have one.

## Gotchas

- `status` is lifecycle (`running` | `paused` | `unknown`), not lag. `unknown` means the writer's metrics endpoint did not answer.
- `hasBackfillingTables: true` means the reader is copying the initial snapshot; latency in that window is backfill time, not streaming latency.
- `tableStats[].latency` is nullable: `null` means nothing moved in the window, not zero lag. `count: 0` is the same story. Widen the window before calling anything unhealthy.
- `readerLagBytes` has a small floor (a few KB to a few hundred KB) from WAL records that are not row changes. Flat and small is healthy; growing is the signal.
- `mergeMs.samples: 0` means the writer has not flushed since it started, not that merges are instant.
- `pipeline_error_logs` is what processes recorded before failing. A `kill -9` leaves nothing; check `processes` for "unreachable".
- MCP reads the destination's metadata columns for latency, not the rows themselves. Whether a specific row landed is `verify`.

## When the fix requires changing something

Stop at diagnosis. `pipeline_update_status` pauses or resumes; do not call it from this skill even if told to. Name the action and hand it back.

## Reply

Lead with the pipeline name and uuid, then status, then the lag numbers with the window, then the bottleneck sentence from the table. Keep it to a short paragraph unless asked for the raw numbers.
