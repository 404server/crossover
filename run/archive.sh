#!/usr/bin/env bash
# Packs the raw results for the Zenodo record: one tar per environment (step files are already
# gzipped, so the tars are not compressed again), the campaign logs, a code snapshot, the data
# README and SHA-256 checksums. Smoke tests and environments without a finished cell are skipped.
#   run/archive.sh [output directory, default zenodo/]
cd "$(dirname "$0")/.." || exit 1
out=${1:-zenodo}
mkdir -p "$out"
for d in results/raw/*/; do
  env=$(basename "$d")
  case "$env" in *-smoke|*-truncated|*-partial) continue;; esac
  [ -n "$(find "$d" -name done -print -quit)" ] || continue
  echo "packing $env"
  tar -cf "$out/raw-$env.tar" -C results/raw "$env"
done
tar -czf "$out/logs.tar.gz" results/*.log
git archive -o "$out/code.tar.gz" HEAD
cp results/DATA.md "$out/README.md"
(cd "$out" && shasum -a 256 -- *.tar *.tar.gz README.md > SHA256SUMS)
ls -lh "$out"
