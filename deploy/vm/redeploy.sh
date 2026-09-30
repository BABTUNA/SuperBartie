#!/bin/bash
# Pull the latest code and restart the pipeline containers. Data volumes are
# kept: the slot survives, so the reader resumes rather than backfilling.
set -euo pipefail
cd "$(dirname "$0")/../.."

git pull --ff-only
cd deploy
docker compose --profile live up -d --build --remove-orphans
docker compose --profile live ps
