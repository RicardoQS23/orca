#!/usr/bin/env bash
# Interleaved base/head memory-pressure bench on Omarchy, each arm in a memory-capped user scope
# with a page-cache churner beside it. Env: BENCH_BASE, BENCH_HEAD, BENCH_ROUNDS (2), BENCH_MIN (10).
set -u
kit=$(mktemp -d /tmp/cpw-abkit-XXXX)
cp zz-run-ab.sh zz-checkpoint-pressure.local.test.ts "$kit/"
orig=$(git rev-parse HEAD)
git fetch -q origin
free -m; nproc; uptime
test=src/main/native-chat/agent-session-journal/database/zz-checkpoint-pressure.local.test.ts
churn() { while [ -f "$root/churn" ]; do dd if=/dev/zero of="$root/churn.bin" bs=1M count=2048 conv=fsync status=none; cat "$root/churn.bin" >/dev/null; rm -f "$root/churn.bin"; done; }
round=0
for arm in ${BENCH_ARMS:-base head base head}; do
  round=$((round+1))
  {
    sha=$BENCH_BASE; [ "$arm" = head ] && sha=$BENCH_HEAD
    git checkout -q --detach -f "$sha" || exit 91
    cp "$kit/zz-checkpoint-pressure.local.test.ts" "$test"
    root=/tmp/cpw-ab-$arm-$round; rm -rf "$root"; mkdir -p "$root"; touch "$root/churn"
    echo "== ARM $arm round $round sha $(git rev-parse --short HEAD) $(date +%T) load $(cut -d' ' -f1-3 /proc/loadavg)"
    systemd-run --user --scope -q -p MemoryHigh=${BENCH_MEM_HIGH:-450M} -p MemoryMax=${BENCH_MEM_MAX:-900M} -p MemorySwapMax=2G -- bash -c "$(declare -f churn); root=$root; churn" &
    churner=$!
    systemd-run --user --scope -q -p MemoryHigh=${BENCH_MEM_HIGH:-450M} -p MemoryMax=${BENCH_MEM_MAX:-900M} -p MemorySwapMax=2G -- \
      env BENCH_ROOT=$root BENCH_MIN=${BENCH_MIN:-10} node node_modules/vitest/vitest.mjs run --config config/vitest.config.ts \
      "$test" --testTimeout=10800000 2>&1 | grep -E "BENCH-|Tests |checkpoint worker" | cut -c1-1500
    rm -f "$root/churn"; wait $churner
    awk 'NR%10==0' "$root/result.jsonl" | sed "s/^/SAMPLE $arm$round /"
    rm -rf "$root" "$test"
  }
done
git checkout -q --detach -f "$orig"
rm -rf "$kit"
