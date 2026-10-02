---
name: verify
description: >
  Proves the destination matches the source: row counts and a full-content
  checksum per replicated table. Use when the user asks whether data landed,
  whether the copy is exact, whether a crash lost or duplicated anything, or
  after any pause/resume or restart. Read-only. Do not use for lag or
  throughput; that is `monitoring`.
---

## Verifying the copy

Call `pipeline_verify` with `uuid` = `bartie`.

- **After a write or a restart:** pass `timeout: "10s"`. A live pipeline always has events in flight, so a single pass a moment after a change is expected to mismatch. With a timeout the call retries until every table matches or the deadline passes, which is the actual question: did it converge.
- **Steady state:** no timeout. One pass.

## Reading the result

- `match: true`: say so, with the table count and row counts. This is a checksum over every column of every row, not a count. It is the strongest statement the pipeline can make about itself.
- `match: false` after a timeout: report each table with `checksumMatch: false` and its `detail`. `dest not readable` means the destination table does not exist yet (no events seen); a row-count gap means events are still in flight or the writer is down; equal counts with different digests means a value differs. Then call `monitoring`'s reads (`pipeline_detail`, `pipeline_usage`, `pipeline_error_logs`) to say why, and stop.

## Gotchas

- Verify reads both databases directly. It knows nothing about the reader, writer, or broker, which is what makes MATCH trustworthy.
- Metadata columns (`__bartie_commit_ts`, `__bartie_updated_at`) are excluded from the checksum; only source columns are compared.
- Do not loop `pipeline_verify` yourself. Use `timeout`; it already retries every 2 s.
- Never "fix" a mismatch from here. No pause, resume, or writes.
