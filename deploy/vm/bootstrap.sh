#!/bin/bash
# First boot of a fresh Ubuntu 24.04 VM (Hetzner CX22 or similar): install
# Docker, clone Bartie, fetch terra, start the live profile, install the
# nightly reset. Run as root:
#
#   curl -fsSL https://raw.githubusercontent.com/BABTUNA/SuperBartie/main/deploy/vm/bootstrap.sh | bash
#
# The first run installs everything and stops. Edit /opt/superbartie/deploy/.env
# (API_DOMAIN at minimum), then run /opt/superbartie/deploy/vm/redeploy.sh.
set -euo pipefail

REPO="${BARTIE_REPO:-https://github.com/BABTUNA/SuperBartie.git}"
DIR=/opt/superbartie

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

FIRST_RUN=0
if [ ! -f deploy/.env ]; then
  cp deploy/.env.example deploy/.env
  FIRST_RUN=1
fi

echo "== nightly reset at 03:00 UTC"
cat > /etc/cron.d/superbartie-nightly <<EOF
0 3 * * * root $DIR/deploy/vm/nightly-reset.sh >> /var/log/superbartie-nightly.log 2>&1
EOF
chmod 644 /etc/cron.d/superbartie-nightly

# On the first run the .env still has the placeholder domain. Starting now
# would make Caddy ask for a certificate for a name that is not ours, so stop
# here and let the owner fill it in.
if [ "$FIRST_RUN" = 1 ]; then
  echo
  echo "Installed. Two steps left:"
  echo "  1. edit $DIR/deploy/.env  (API_DOMAIN, MINICDC_CORS_ORIGINS, MINICDC_API_TOKEN, optional keys)"
  echo "  2. run  $DIR/deploy/vm/redeploy.sh"
  exit 0
fi

echo "== up"
./deploy/vm/redeploy.sh
