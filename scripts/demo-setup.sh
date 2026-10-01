#!/bin/bash
# One-shot demo setup: clean stage, seeded source, running pipeline, backfill
# done. Every step prints what it runs and why, so it reads clearly on a
# recording. When it says READY, the source is seeded and the destination is a
# live matching copy; you can start demonstrating.
set -euo pipefail
cd "$(dirname "$0")/.."

bold() { printf '\n\033[1m== %s\033[0m\n' "$1"; }        # step heading
why()  { printf '   \033[2m%s\033[0m\n' "$1"; }          # one-line explanation
run()  { printf '   $ %s\n' "$*"; "$@"; }                # show the command, then run it

SRC="docker exec superbartie-source psql -U postgres -d terra -tAc"
DST="docker exec superbartie-dest psql -U postgres -d warehouse -tAc"

bold "1/5  Stop any leftover pipeline processes"
why "old readers hold the replication slot and block a fresh start"
run pkill -9 -f 'bin/reader'  || true
run pkill -9 -f 'bin/writer'  || true
run pkill -9 -f 'exe/reader'  || true
run pkill -9 -f 'exe/writer'  || true
sleep 1

bold "2/5  Reset the stack to a clean, seeded state"
why "destroys the databases and recreates them: source gets the safari seed, destination starts empty"
run ./scripts/reset.sh

bold "3/5  Build the binaries"
why "reader (WAL to Kafka), writer (Kafka to destination), cdcctl (verify + latency)"
run go build -o bin/reader ./cmd/reader
run go build -o bin/writer ./cmd/writer
run go build -o bin/cdcctl ./cmd/cdcctl

bold "4/5  Start the pipeline"
why "a fresh slot triggers a backfill of the seed, then live streaming"
run bash -c './bin/reader > /tmp/bartie-reader.log 2>&1 & echo reader pid $!'
run bash -c './bin/writer > /tmp/bartie-writer.log 2>&1 & echo writer pid $!'

bold "5/5  Wait for the backfill to finish"
why "destination row count should catch up to the source"
src_rows=$($SRC "SELECT count(*) FROM observations")
printf '   source has %s observations\n' "$src_rows"
for i in $(seq 1 60); do
  dst_rows=$($DST "SELECT count(*) FROM observations" 2>/dev/null || echo 0)
  printf '\r   destination has %s ...' "$dst_rows"
  if [ "$dst_rows" = "$src_rows" ]; then
    break
  fi
  sleep 1
done
printf '\n'

if [ "${dst_rows:-0}" = "$src_rows" ]; then
  printf '\n\033[1;32mREADY\033[0m  source and destination both at %s observations, pipeline live.\n' "$src_rows"
  printf 'The reader and writer are running in the background (logs in /tmp/bartie-*.log).\n'
else
  printf '\n\033[1;31mNOT READY\033[0m  backfill did not converge (%s vs %s). Check /tmp/bartie-*.log\n' "${dst_rows:-0}" "$src_rows"
  exit 1
fi
