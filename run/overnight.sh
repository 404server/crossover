#!/usr/bin/env bash
# Waits until START (HH:MM, default 23:00) and then runs the campaign with the Mac kept awake,
# so a long run happens at the same time of day as the others. Usage:
#   START=23:00 nohup run/overnight.sh --env kind-prop --alloc alloc-prop.json >> results/kind-prop.log 2>&1 &
cd "$(dirname "$0")"
until [ "$(date +%H:%M)" = "${START:-23:00}" ]; do sleep 20; done
exec caffeinate -dimsu python3 campaign.py "$@"
