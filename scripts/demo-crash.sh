#!/bin/bash
# Beat 2 of the demo: kill the writer mid-stream and prove nothing is lost.
# Assumes the pipeline is already running (run scripts/demo-setup.sh first).
# Narrates each step and prints source-vs-destination counts at the moments
# that matter, so the "destination freezes, then catches up, then MATCHES"
# story is visible in a single pane with no watch windows to babysit.
set -euo pipefail
cd "$(dirname "$0")/.."

bold() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
why()  { printf '   \033[2m%s\033[0m\n' "$1"; }
run()  { printf '   $ %s\n' "$*"; "$@"; }

SRC="docker exec superbartie-source psql -U postgres -d terra -tAc"
DST="docker exec superbartie-dest psql -U postgres -d warehouse -tAc"

counts() {
  local s d
  s=$($SRC "SELECT count(*) FROM observations" 2>/dev/null || echo '?')
  d=$($DST "SELECT count(*) FROM observations" 2>/dev/null || echo '?')
  printf '   \033[1msource %s   →   destination %s\033[0m\n' "$s" "$d"
}

ITER="${1:-150}"

bold "1/5  Start a stream of live changes"
why "inserts, updates and deletes flowing into the source in the background"
run bash -c "./scripts/load.sh $ITER > /tmp/bartie-load.log 2>&1 & echo load pid \$!"
sleep 5
why "source and destination climbing together, a second or so apart:"
counts

bold "2/5  Kill the writer, mid-stream"
why "the half that writes to the destination dies; events keep piling up safely in Kafka"
run pkill -9 -f 'bin/writer'
sleep 6
why "destination is now FROZEN while the source keeps moving, that gap is unapplied changes:"
counts

bold "3/5  Restart the writer"
why "it rejoins, replays from its last committed offset, and the merge re-asserts each row"
run bash -c './bin/writer > /tmp/bartie-writer.log 2>&1 & echo writer pid $!'

bold "4/5  Let the load finish and the destination catch up"
why "waiting for the write workload to drain"
while pgrep -f 'scripts/load.sh' >/dev/null 2>&1; do sleep 1; done
sleep 3
counts

bold "5/5  Prove it: checksum both databases"
why "verify compares every row on both sides; it knows nothing about the pipeline"
run ./bin/cdcctl verify --timeout 60s

printf '\n\033[1;32mDONE\033[0m  writer was killed mid-stream and the copy is still exact: nothing lost, nothing duplicated.\n'
