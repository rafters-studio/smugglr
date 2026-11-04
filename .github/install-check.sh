#!/usr/bin/env bash
# Runs install.sh against the published release. Each case gets its own empty
# HOME, so the assertions see only what that run left behind.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SANDBOX="$(mktemp -d)"
trap 'rm -rf "${SANDBOX}"' EXIT

OUTPUT=""
RC=0
CASE_HOME=""
CLEAN_PATH="/usr/bin:/bin:/usr/sbin:/sbin"

fail() {
  printf 'FAIL: %s\n--- installer output ---\n%s\n' "$1" "${OUTPUT}" >&2
  exit 1
}

expect_output() {
  case "${OUTPUT}" in
    *"$1"*) ;;
    *) fail "output does not contain '$1'" ;;
  esac
}

# The installer run as `curl ... | bash` runs it: the script arrives on stdin.
run_installer() {
  local name="$1"
  shift
  printf '== %s\n' "${name}"
  CASE_HOME="${SANDBOX}/${name}"
  mkdir -p "${CASE_HOME}"
  RC=0
  OUTPUT="$(env HOME="${CASE_HOME}" PATH="${PATH_FOR_RUN:-${CLEAN_PATH}}" "$@" bash <"${ROOT}/install.sh" 2>&1)" || RC=$?
  printf '%s\n' "${OUTPUT}"
}

expect_installed() {
  local bin="${CASE_HOME}/.local/bin/smugglr"
  [ "${RC}" -eq 0 ] || fail "exit ${RC}"
  [ -x "${bin}" ] || fail "${bin} is not executable"
  "${bin}" sync --help >/dev/null || fail "${bin} sync --help failed"
  [ "$(ls -A "${CASE_HOME}/.local/bin")" = "smugglr" ] || fail "installed more than smugglr: $(ls -A "${CASE_HOME}/.local/bin")"
  expect_output "${bin}"
}

expect_refused() {
  [ "${RC}" -ne 0 ] || fail "exited 0"
  [ -z "$(ls -A "${CASE_HOME}")" ] || fail "left files in HOME: $(ls -AR "${CASE_HOME}")"
}

case "$(uname -s)/$(uname -m)" in
  Linux/x86_64) target="x86_64-unknown-linux-gnu" ;;
  Linux/aarch64) target="aarch64-unknown-linux-gnu" ;;
  Darwin/x86_64) target="x86_64-apple-darwin" ;;
  Darwin/arm64) target="aarch64-apple-darwin" ;;
  *) printf 'FAIL: no expected target for this runner\n' >&2 && exit 1 ;;
esac

run_installer latest
expect_installed
expect_output "smugglr-${target}.tar.gz"
expect_output "${CASE_HOME}/.local/bin is not on your PATH"

run_installer pinned SMUGGLR_VERSION=v0.6.1
expect_installed
expect_output "smugglr v0.6.1"

PATH_FOR_RUN="${SANDBOX}/on-path/.local/bin:${CLEAN_PATH}" run_installer on-path
expect_installed
case "${OUTPUT}" in
  *"not on your PATH"*) fail "gave PATH advice for a directory already on PATH" ;;
esac

# v0.5.0 is a real release, published under the archive names before these.
run_installer no-archive SMUGGLR_VERSION=v0.5.0
expect_refused
expect_output "${target}"

mkdir -p "${SANDBOX}/stub"
# shellcheck disable=SC2016 # the $1 belongs to the stub
printf '#!/bin/sh\ncase "$1" in -s) echo FreeBSD ;; -m) echo riscv64 ;; esac\n' >"${SANDBOX}/stub/uname"
chmod +x "${SANDBOX}/stub/uname"
PATH_FOR_RUN="${SANDBOX}/stub:${CLEAN_PATH}" run_installer unsupported
expect_refused
expect_output "FreeBSD riscv64"

printf 'install.sh: all cases passed for %s\n' "${target}"
