#!/bin/bash
# Readable, steady traffic for the live page: a ranger sighting every couple
# of seconds, an occasional status change on an existing animal, an
# occasional delete of a sighting this script created. Meant to be watched,
# unlike load.sh, which exists to stress the pipeline (huge TOAST notes).
#
# Runs in two places:
#   on a laptop:   ./scripts/demo-traffic.sh [seconds]        (docker compose exec)
#   in compose:    the "traffic" service sets PGHOST and runs it forever
#
# Ids live in their own block (observation_id >= 3e9) so they never collide
# with the seed, load.sh, or the page's demo row. The nightly reset wipes them.
set -euo pipefail

DURATION="${1:-0}"                 # 0 = forever
INTERVAL="${TRAFFIC_INTERVAL:-2}"  # seconds between writes

if [ -n "${PGHOST:-}" ]; then
  run_sql() { psql -q -v ON_ERROR_STOP=1 -U "${PGUSER:-postgres}" -d "${PGDATABASE:-terra}" "$@"; }
  PAUSE_FILE="${TRAFFIC_PAUSE_FILE:-/var/lib/bartie/traffic.paused}"
else
  cd "$(dirname "$0")/../deploy"
  run_sql() { docker compose exec -T source psql -q -v ON_ERROR_STOP=1 -U postgres -d terra "$@"; }
  PAUSE_FILE="${TRAFFIC_PAUSE_FILE:-../data/traffic.paused}"
fi

# The api pauses us by creating this file (POST /demo/traffic {"enabled": false})
# and resumes by removing it. Checked before every write.
#
# Verify also asks us to hold still for a few seconds with a second file, so
# rows in flight can land before it compares. A hold older than a minute is
# ignored, so a crashed api can never stall the traffic for good.
HOLD_FILE="$(dirname "$PAUSE_FILE")/traffic.hold"
held() { [ -n "$(find "$HOLD_FILE" -mmin -1 2>/dev/null)" ]; }
wait_if_paused() {
  while [ -f "$PAUSE_FILE" ] || held; do sleep 1; done
}

places=("the north ridge" "the south bank" "the reed bed" "the shallows" "the acacia line" "the dry channel" "the salt lick" "the far shore")
acts=("drinking" "resting" "grazing" "moving through" "wallowing" "watching the herd" "feeding calves" "sparring")
sizes=("alone" "in a pair" "in a group of 3" "in a group of 5" "with a calf" "in a herd of 12")
statuses=("adult" "collared" "juvenile" "adult" "monitored")

BASE=$(( 3000000000 + ($(date +%s) % 200000) * 10000 ))
i=0
start=$(date +%s)
created=()

while :; do
  i=$((i + 1))
  now=$(date +%s)
  if [ "$DURATION" -gt 0 ] && [ $((now - start)) -ge "$DURATION" ]; then
    break
  fi

  wait_if_paused
  place=${places[RANDOM % ${#places[@]}]}
  act=${acts[RANDOM % ${#acts[@]}]}
  size=${sizes[RANDOM % ${#sizes[@]}]}
  id=$((BASE + i))
  created+=("$id")

  # A sighting of a random seeded animal at a random watering hole, with notes
  # that read like a field log and mention the names, so retrieval works.
  run_sql <<SQL >/dev/null
WITH a AS (SELECT animal_id, name, species FROM animals WHERE animal_id < 100000 ORDER BY random() LIMIT 1),
     w AS (SELECT watering_hole_id, name FROM watering_holes ORDER BY random() LIMIT 1)
INSERT INTO observations (observation_id, animal_id, watering_hole_id, observed_at, notes)
SELECT $id, a.animal_id, w.watering_hole_id, now(),
       format('Ranger log: %s the %s seen %s %s at %s, %s.',
              a.name, replace(a.species::text, '_', ' '), '$act', '$size', w.name, '$place')
FROM a, w;
SQL

  # Every 4th write, flip a seeded animal's status (an UPDATE on animals).
  if (( i % 4 == 0 )); then
    status=${statuses[RANDOM % ${#statuses[@]}]}
    run_sql -c "UPDATE animals SET status = '$status', updated_at = now()
                WHERE animal_id = (SELECT animal_id FROM animals WHERE animal_id < 100000 ORDER BY random() LIMIT 1);" >/dev/null
  fi

  # Every 7th write, retract a sighting from earlier in this run (a DELETE).
  if (( i % 7 == 0 )) && [ "${#created[@]}" -gt 3 ]; then
    victim=${created[RANDOM % (${#created[@]} - 1)]}
    run_sql -c "DELETE FROM observations WHERE observation_id = $victim;" >/dev/null
  fi

  sleep "$INTERVAL"
done
echo "traffic done: $i writes"
