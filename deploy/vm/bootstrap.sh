#!/bin/bash
# First boot of a fresh Ubuntu 24.04 VM (Hetzner CX22 or similar): install
# Docker, clone Bartie, fetch terra, start the live profile, install the
# nightly reset. Run as root:
#
#   curl -fsSL https://raw.githubusercontent.com/BABTUNA/Bartie/main/deploy/vm/bootstrap.sh | bash
#
# Then edit /opt/bartie/deploy/.env (API_DOMAIN at minimum) and run
# /opt/bartie/deploy/vm/redeploy.sh.
set -euo pipefail

REPO="${BARTIE_REPO:-https://github.com/BABTUNA/Bartie.git}"
DIR=/opt/bartie

echo "== docker"
if ! command -v docker >/dev/null; then
  curl -fsSL https://get.docker.com | sh
fi
systemctl enable --now docker

echo "== firewall: ssh, http, https only"
if command -v ufw >/dev/null; then
  ufw allow OpenSSH >/dev/null
  ufw allow 80/tcp >/dev/null
  ufw allow 443/tcp >/dev/null
  ufw --force enable >/dev/null
fi

echo "== clone"
if [ ! -d "$DIR" ]; then
  git clone "$REPO" "$DIR"
fi
cd "$DIR"
./deploy/fetch-terra.sh

if [ ! -f deploy/.env ]; then
  cp deploy/.env.example deploy/.env
  echo "!! edit $DIR/deploy/.env (API_DOMAIN, CORS origins, keys) then run deploy/vm/redeploy.sh"
fi

echo "== nightly reset at 03:00 UTC"
cat > /etc/cron.d/bartie-nightly <<EOF
0 3 * * * root $DIR/deploy/vm/nightly-reset.sh >> /var/log/bartie-nightly.log 2>&1
EOF
chmod 644 /etc/cron.d/bartie-nightly

echo "== up"
./deploy/vm/redeploy.sh
