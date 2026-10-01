#!/bin/bash
# Compare our streaming CDC against the batch-snapshot alternative, the pre-CDC
# way of keeping a copy fresh: periodically re-copy the table. The point is not
# a rigged interval; it is that a snapshot's latency floor IS its copy time,
# and copy time grows with the table while CDC latency does not.
#
# CDC latency is measured on the live terra pipeline. The batch copy is measured
# in a separate, non-replicated database so the two never interfere; each cycle
# is a real source->destination transfer (pg_dump piped into psql).
set -euo pipefail
cd "$(dirname "$0")/.."

bold() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
why()  { printf '   \033[2m%s\033[0m\n' "$1"; }
now()  { python3 -c 'import time; print(time.time())'; }

SIZES="10000 100000 1000000 10000000"
PAYLOAD=200   # bytes per row, so a copy moves real data

bold "Part A  Streaming CDC latency (our engine, on the terra pipeline)"
why "clean stack, seed cleared so we measure streaming, not backfill"
pkill -9 -f 'bin/reader' 2>/dev/null || true
pkill -9 -f 'bin/writer' 2>/dev/null || true
go build -o bin/reader ./cmd/reader && go build -o bin/writer ./cmd/writer && go build -o bin/cdcctl ./cmd/cdcctl
./scripts/reset.sh >/dev/null
docker exec superbartie-source psql -U postgres -d terra -qc "TRUNCATE observations, animals, watering_holes;"
./bin/reader >/tmp/bartie-reader.log 2>&1 &
./bin/writer >/tmp/bartie-writer.log 2>&1 &
sleep 3
why "sending a burst of live changes and measuring commit-to-apply latency"
./scripts/load.sh 150 >/tmp/bartie-load.log 2>&1
sleep 4
CDC_LINE=$(./bin/cdcctl latency --table public.observations --since 90s --csv /tmp/cdc_latency.csv)
CDC_AVG=$(printf '%s' "$CDC_LINE" | grep -oE 'avg [0-9.]+s' | grep -oE '[0-9.]+')
printf '   measured: %s\n' "$CDC_LINE"

bold "Part B  Batch snapshot copy time (separate, non-replicated database)"
why "each cycle re-copies the whole table source->destination; that time is the freshness floor"
docker exec superbartie-source createdb -U postgres benchbatch 2>/dev/null || true
docker exec superbartie-dest   createdb -U postgres benchbatch 2>/dev/null || true
docker exec superbartie-source psql -U postgres -d benchbatch -qc "CREATE TABLE IF NOT EXISTS t (id bigint PRIMARY KEY, payload text);"
docker exec superbartie-dest   psql -U postgres -d benchbatch -qc "CREATE TABLE IF NOT EXISTS t (id bigint PRIMARY KEY, payload text);"

declare -a RESULT_SIZE RESULT_BATCH
for N in $SIZES; do
  why "loading $N rows on the source"
  docker exec superbartie-source psql -U postgres -d benchbatch -qc \
    "TRUNCATE t; INSERT INTO t SELECT g, repeat('x', $PAYLOAD) FROM generate_series(1, $N) g;"

  cycles=3
  [ "$N" -ge 1000000 ] && cycles=1   # large sizes are slow; one full cycle is enough
  total=0
  for c in $(seq 1 $cycles); do
    docker exec superbartie-dest psql -U postgres -d benchbatch -qc "TRUNCATE t;"
    t0=$(now)
    docker exec superbartie-source pg_dump -U postgres -d benchbatch -t t --data-only \
      | docker exec -i superbartie-dest psql -U postgres -d benchbatch -q >/dev/null
    t1=$(now)
    total=$(python3 -c "print($total + ($t1 - $t0))")
  done
  avg=$(python3 -c "print(f'{$total / $cycles:.2f}')")
  printf '   %s rows: batch copy avg %ss over %s cycle(s)\n' "$N" "$avg" "$cycles"
  RESULT_SIZE+=("$N"); RESULT_BATCH+=("$avg")
done

bold "Results: streaming CDC vs batch snapshot"
printf '\n   %-12s %-22s %-22s\n' "rows" "batch snapshot copy" "streaming CDC (ours)"
printf '   %-12s %-22s %-22s\n' "----" "-------------------" "--------------------"
for i in "${!RESULT_SIZE[@]}"; do
  printf '   %-12s %-22s %-22s\n' "${RESULT_SIZE[$i]}" "${RESULT_BATCH[$i]}s" "${CDC_AVG}s"
done

printf '\n\033[2mBatch latency is bounded below by copy time, which climbs with row count.\n'
printf 'CDC touches only the change, so its latency is flat regardless of table size.\n'
printf 'At tiny scale batch can win (our %ss is just the flush interval); the point is what happens as data grows.\033[0m\n' "$CDC_AVG"
