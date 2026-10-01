#!/usr/bin/env bash
# Runs the campaign at or after START (HH:MM, default: now) and keeps the Mac awake for the whole time,
# the wait included (an exact-minute check misses the minute if the host sleeps through it). Usage:
#   START=23:00 nohup run/overnight.sh --env kind-prop --alloc alloc-prop.json >> results/kind-prop.log 2>&1 &
cd "$(dirname "$0")"
exec caffeinate -dimsu bash -c '
  target=$(date -j -f "%H:%M" "$0" +%s)
  [ "$target" -lt "$(( $(date +%s) - 60 ))" ] && target=$((target + 86400))  # a past time means tomorrow
  while [ "$(date +%s)" -lt "$target" ]; do sleep 20; done
  exec python3 campaign.py "$@"' "${START:-$(date +%H:%M)}" "$@"
