#!/bin/bash
# Wipe the demo back to the seed. Drops every data volume (source, dest,
# broker, error log), which also drops the replication slot, so the reader
# comes up on the fresh-slot path: export snapshot, backfill, then stream.
# That path is exercised every night, on purpose.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "$(date -u +%FT%TZ) nightly reset: down"
docker compose --profile live down -v --remove-orphans
echo "$(date -u +%FT%TZ) nightly reset: up"
docker compose --profile live up -d
echo "$(date -u +%FT%TZ) nightly reset: waiting for api"
for _ in $(seq 1 60); do
  if docker compose --profile live exec -T api curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
    echo "$(date -u +%FT%TZ) nightly reset: done"
    exit 0
  fi
  sleep 2
done
echo "$(date -u +%FT%TZ) nightly reset: api did not come up"
docker compose --profile live ps
exit 1
