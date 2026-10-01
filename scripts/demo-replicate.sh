#!/bin/bash
# Beat 1 of the demo: watch a single row replicate through an insert, an update,
# and a delete. Assumes the pipeline is running (run scripts/demo-setup.sh
# first). Prints the source and destination tables side by side after each
# change so you can see the row appear, change, and disappear on both databases.
set -euo pipefail
cd "$(dirname "$0")/.."

bold() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
why()  { printf '   \033[2m%s\033[0m\n' "$1"; }
run()  { printf '   $ %s\n' "$*"; "$@"; }

# Unique id per run so repeated demos never collide on the primary key.
ID=$(( 900000 + ($(date +%s) % 90000) ))

SRC_EXEC() { docker exec superbartie-source psql -U postgres -d terra "$@"; }
DST_EXEC() { docker exec superbartie-dest   psql -U postgres -d warehouse "$@"; }

show_source() {
  printf '\n   \033[36mSOURCE  animals (id %s)\033[0m\n' "$ID"
  SRC_EXEC -c "SELECT animal_id, name, status FROM animals WHERE animal_id = $ID;"
}
show_dest() {
  printf '   \033[35mDESTINATION  animals (id %s)\033[0m\n' "$ID"
  DST_EXEC -c "SELECT animal_id, name, status, __bartie_updated_at FROM animals WHERE animal_id = $ID;"
}
settle() { printf '   \033[2m(waiting ~2s for the change to stream through)\033[0m\n'; sleep 3; }

# Make the script safe to re-run without a full reset: stop any stream still
# running from a previous run and clear rows earlier runs created (seed rows
# use small ids, everything demo-generated uses ids >= 100000).
pkill -f 'scripts/load.sh' 2>/dev/null || true
SRC_EXEC -qc "DELETE FROM observations WHERE observation_id >= 100000;" >/dev/null 2>&1 || true
SRC_EXEC -qc "DELETE FROM animals WHERE animal_id >= 100000;" >/dev/null 2>&1 || true
sleep 1

bold "Following one animal, id $ID, through the pipeline"
why "it does not exist yet on either side"
show_source
show_dest

bold "1/3  INSERT on the source"
why "a brand new row"
run SRC_EXEC -c "INSERT INTO animals (animal_id, name, species, home_watering_hole_id, status) VALUES ($ID, 'Simba', 'lion', 1, 'juvenile');"
settle
show_source
show_dest
why "the row arrived in the destination on its own"

bold "2/3  UPDATE on the source"
why "promote Simba from juvenile to adult"
run SRC_EXEC -c "UPDATE animals SET status = 'adult' WHERE animal_id = $ID;"
settle
show_source
show_dest
why "the destination reflects the new status"

bold "3/3  DELETE on the source"
why "remove the row entirely"
run SRC_EXEC -c "DELETE FROM animals WHERE animal_id = $ID;"
settle
show_source
show_dest
why "gone from the destination too"

printf '\n   \033[2m--- basics done: now put it under pressure ---\033[0m\n'

# A fresh id range for the bulk work, distinct from the single-row id above.
B=$(( 1000000 + ($(date +%s) % 500000) ))

counts() {
  local s d
  s=$(docker exec superbartie-source psql -U postgres -d terra -tAc "SELECT count(*) FROM animals" 2>/dev/null || echo '?')
  d=$(docker exec superbartie-dest psql -U postgres -d warehouse -tAc "SELECT count(*) FROM animals" 2>/dev/null || echo '?')
  printf '   \033[1msource %s animals   →   destination %s animals\033[0m\n' "$s" "$d"
}

bold "4/7  Start a continuous background stream"
why "sustained insert/update/delete traffic while we also fire bulk operations"
run bash -c './scripts/load.sh 250 > /tmp/bartie-load.log 2>&1 & echo stream pid $!'

bold "5/7  Bulk INSERT: 500 rows in one statement"
why "not one row at a time, a whole batch at once"
run SRC_EXEC -c "INSERT INTO animals (animal_id, name, species, home_watering_hole_id, status)
                 SELECT g, 'stress-'||g, 'impala', 1, 'adult'
                 FROM generate_series($B, $B+499) g;"
settle
counts

bold "6/7  Bulk UPDATE then bulk DELETE"
why "rewrite 500 rows, then delete half of them, while the stream keeps running"
run SRC_EXEC -c "UPDATE animals SET status = 'collared' WHERE animal_id BETWEEN $B AND $B+499;"
run SRC_EXEC -c "DELETE FROM animals WHERE animal_id BETWEEN $B AND $B+249;"
why "and a mixed transaction for good measure"
run SRC_EXEC -c "BEGIN;
                 INSERT INTO animals (animal_id, name, species, home_watering_hole_id, status) VALUES ($B, 'txn-cub', 'lion', 1, 'newborn');
                 UPDATE animals SET status = 'adult' WHERE animal_id = $((B+300));
                 DELETE FROM animals WHERE animal_id = $((B+301));
                 COMMIT;"
settle
counts

bold "7/7  Let the stream finish, then prove correctness"
why "waiting for the background workload to drain"
while pgrep -f 'scripts/load.sh' >/dev/null 2>&1; do sleep 1; done
sleep 3
counts
run ./bin/cdcctl verify --timeout 60s

printf '\n\033[1;32mDONE\033[0m  single-row and high-volume mixed operations all replicated, and the copy is exact.\n'
