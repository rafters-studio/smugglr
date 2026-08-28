#!/usr/bin/env bash
# Build westwind.db: apply the committed manifests with `smugglr migrate apply`,
# then load seed.sql.
#
# Usage: ./make.sh [--empty] [output-path]
#   --empty   apply the schema and skip the seed (a sync target that needs the
#             tables and no rows)
#   default output: ./westwind.db next to this script. A relative path is
#   taken from the caller's directory, so `../westwind/make.sh ./local.db`
#   from an example directory builds it there.
#
# Needs `smugglr` on PATH (or SMUGGLR=/path/to/smugglr) and the sqlite3 shell.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
smugglr="${SMUGGLR:-smugglr}"
empty=no
if [ "${1:-}" = "--empty" ]; then empty=yes; shift; fi
out="${1:-$here/westwind.db}"
case "$out" in /*) ;; *) out="$PWD/$out" ;; esac
rm -f "$out"
# migrate apply opens read-write without create; the file must exist.
sqlite3 "$out" "SELECT 1;" >/dev/null
# Tracing goes to a file so a clean run stays quiet and a failed apply still
# shows its reason.
errlog="$(mktemp)"
trap 'rm -f "$errlog"' EXIT
for m in "$here"/migrations/*.json; do
  if ! "$smugglr" migrate apply "$m" --db "$out" 2>"$errlog"; then
    echo "make.sh: migrate apply failed on $(basename "$m"):" >&2
    cat "$errlog" >&2
    exit 1
  fi
done
if [ "$empty" = no ]; then
  sqlite3 "$out" < "$here/seed.sql"
fi
sqlite3 "$out" "SELECT 'customers', count(*) FROM customers UNION ALL SELECT 'orders', count(*) FROM orders UNION ALL SELECT 'order_details', count(*) FROM order_details;"
