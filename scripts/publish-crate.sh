#!/usr/bin/env bash
# Publish one workspace crate to crates.io, idempotently.
#
# The release job publishes five crates in sequence with no guard, so any
# failure partway leaves the earlier crates published and the run
# unrepeatable: re-running hits "crate version already exists" on crate 1
# and dies before reaching the crate that actually failed. That is how
# v0.4.3 got stuck -- one brand-new crate took down four that were ready.
#
# publish-npm already does the view-then-publish dance. This is the cargo
# equivalent.
#
# Bias: a false MISS (we think it is unpublished when it is not) is safe --
# we attempt the publish and treat "already exists" as success. A false HIT
# is not safe: it would silently skip a crate that never shipped. Sparse
# index propagation lags a successful publish by seconds, so the guard is
# written to fail toward attempting.

set -euo pipefail

crate="${1:?usage: publish-crate.sh <crate-name>}"

version="$(cargo metadata --format-version 1 --no-deps \
  | python3 -c 'import json, sys
meta = json.load(sys.stdin)
name = sys.argv[1]
print(next(p["version"] for p in meta["packages"] if p["name"] == name))' "$crate")"

# crates.io sparse index path: 1/x, 2/xy, 3/x/xyz, else xx/yy/name
name_len="${#crate}"
case "$name_len" in
  1) index_path="1/$crate" ;;
  2) index_path="2/$crate" ;;
  3) index_path="3/${crate:0:1}/$crate" ;;
  *) index_path="${crate:0:2}/${crate:2:2}/$crate" ;;
esac

echo "checking index for $crate@$version"

already_published=no
if body="$(curl -sf --max-time 30 "https://index.crates.io/$index_path")"; then
  if printf '%s' "$body" | python3 -c 'import json, sys
target = sys.argv[1]
for line in sys.stdin:
    line = line.strip()
    if line and json.loads(line)["vers"] == target:
        raise SystemExit(0)
raise SystemExit(1)' "$version"; then
    already_published=yes
  fi
else
  # 404 means the crate has never been published (first release of a new
  # crate); any other curl failure is a transient we treat the same way,
  # because attempting is the safe direction.
  echo "index lookup did not resolve; assuming not published"
fi

if [ "$already_published" = yes ]; then
  echo "$crate@$version already on crates.io, skipping"
  exit 0
fi

echo "publishing $crate@$version"
if ! output="$(cargo publish -p "$crate" 2>&1)"; then
  echo "$output"
  # Lost a race with the index, or the guard read a stale index. The
  # end state is the one we wanted, so do not fail the run.
  if printf '%s' "$output" | grep -q "already exists"; then
    echo "$crate@$version was already published; treating as success"
    exit 0
  fi
  exit 1
fi
echo "$output"
