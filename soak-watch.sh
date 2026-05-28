#!/bin/sh
# decypharr soak-test watch — one-glance health for the fork.N changes.
# Run on the Unraid host: ./soak-watch.sh [since]   (default since=24h)
set -e
SINCE="${1:-24h}"
docker logs decypharr --since "$SINCE" 2>&1 > /tmp/dlog.txt || true

strip() { sed -E 's/\x1b\[[0-9;]*m//g'; }

echo "==== decypharr soak summary (last $SINCE) ===="
echo "-- version --"
grep 'Starting manager' /tmp/dlog.txt | tail -1 | strip || true
echo "-- health --"
curl -s http://localhost:8282/api/health/mount 2>/dev/null | \
  python3 -c 'import sys,json;d=json.load(sys.stdin);print("ready",d["ready"],"degraded",d["degraded"],"cb_open",d["circuit_breakers_open"])' 2>/dev/null \
  || echo "(health endpoint unreachable)"
echo "-- crashes (want 0) --"
grep -ciE 'panic|fatal' /tmp/dlog.txt || true
echo "-- N1 dead-NZB dedup --"
echo "   refusals (good):   $(grep -c 'previously failed' /tmp/dlog.txt || true)"
echo "   post-dl errors:    $(grep -c 'post-download action' /tmp/dlog.txt || true)  (was ~21/12h pre-fix)"
echo "-- N2 warden arr-health --"
echo "   cooling-down:      $(grep -c 'cooling down' /tmp/dlog.txt || true)"
echo "   OLD permanent-skip (want 0): $(grep -c 'rest of session' /tmp/dlog.txt || true)"
echo "   defence cycles:    $(grep -c 'defence cycle complete' /tmp/dlog.txt || true)"
echo "-- ffprobe (fork.7 tail-prewarm) --"
grep -iE 'ffprobe' /tmp/dlog.txt | grep -ivE 'Running ffprobe' | strip | tail -3 || true
echo "-- error/warn aggregate --"
grep -oiE '\| (ERROR|WARN) +\|[^"]*' /tmp/dlog.txt | sed -E 's/\[[0-9;]*m//g; s/[0-9]+/N/g' | sort | uniq -c | sort -rn | head -12 || true
echo "-- resources --"
docker stats decypharr --no-stream --format 'CPU={{.CPUPerc}} MEM={{.MemUsage}} PIDS={{.PIDs}}' || true
echo "==== end (escalate on: any panic, unbounded mem growth, store fails to recover, ffprobe stalls persist, an arr stuck cooling-down forever) ===="
