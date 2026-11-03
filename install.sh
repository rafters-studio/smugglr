#!/usr/bin/env bash
# install.sh: installs the smugglr binary from a GitHub release.
#
#   curl -fsSL https://raw.githubusercontent.com/rafters-studio/smugglr/main/install.sh | bash
#   SMUGGLR_VERSION=v0.6.1 bash install.sh
#
# Linux and macOS, x86_64 and aarch64. It writes one file, ~/.local/bin/smugglr,
# and only after the downloaded binary has run from that directory.

set -euo pipefail

REPO="rafters-studio/smugglr"
INSTALL_DIR="${HOME}/.local/bin"

WORK_DIR=""
STAGED=""
cleanup() {
  if [ -n "${WORK_DIR}" ]; then rm -rf "${WORK_DIR}"; fi
  if [ -n "${STAGED}" ]; then rm -f "${STAGED}"; fi
}
trap cleanup EXIT

info() { printf 'smugglr install: %s\n' "$1" >&2; }
fail() {
  printf 'smugglr install: error: %s\n' "$1" >&2
  exit 1
}

# The Rust target triple the release names its archives by.
detect_target() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  # Under Rosetta 2, uname reports x86_64 on arm64 hardware: take the native build.
  if [ "${os}" = "Darwin" ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = "1" ]; then
    arch="aarch64"
  fi
  if [ "${arch}" = "amd64" ]; then arch="x86_64"; fi
  if [ "${arch}" = "arm64" ]; then arch="aarch64"; fi

  case "${os}/${arch}" in
    Linux/x86_64 | Linux/aarch64) TARGET="${arch}-unknown-linux-gnu" ;;
    Darwin/x86_64 | Darwin/aarch64) TARGET="${arch}-apple-darwin" ;;
    *) fail "unsupported platform ${os} ${arch}: releases carry x86_64 and aarch64 for Linux and macOS" ;;
  esac
}

# The tag to install: SMUGGLR_VERSION, or the one the latest release redirects to.
resolve_version() {
  if [ -n "${SMUGGLR_VERSION:-}" ]; then
    case "${SMUGGLR_VERSION}" in
      v*) VERSION="${SMUGGLR_VERSION}" ;;
      *) VERSION="v${SMUGGLR_VERSION}" ;;
    esac
    return
  fi

  local latest
  latest="$(curl --proto '=https' --tlsv1.2 -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/${REPO}/releases/latest")" ||
    fail "could not reach github.com to find the latest release for ${TARGET}"
  VERSION="${latest##*/}"
  case "${VERSION}" in
    v[0-9]*) ;;
    *) fail "could not read the latest release tag from ${latest} for ${TARGET}" ;;
  esac
}

download() {
  local url="$1" output="$2" status
  status="$(curl --proto '=https' --tlsv1.2 -sSL -o "${output}" -w '%{http_code}' "${url}")" ||
    fail "could not download ${url} for ${TARGET}"
  case "${status}" in
    200) ;;
    404) fail "release ${VERSION} has no archive for ${TARGET}: ${url}" ;;
    *) fail "download of ${url} for ${TARGET} answered HTTP ${status}" ;;
  esac
}

path_advice() {
  case ":${PATH}:" in
    *":${INSTALL_DIR}:"*) return ;;
  esac

  info "${INSTALL_DIR} is not on your PATH. To add it:"
  case "$(basename "${SHELL:-bash}")" in
    fish) info "  fish_add_path ${INSTALL_DIR}" ;;
    zsh) info "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.zshrc" ;;
    bash) info "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.bashrc" ;;
    *) info "  export PATH=\"${INSTALL_DIR}:\$PATH\"" ;;
  esac
  info "then open a new shell."
}

main() {
  command -v curl >/dev/null 2>&1 || fail "curl is required"

  detect_target
  resolve_version

  local archive="smugglr-${TARGET}.tar.gz"
  local url="https://github.com/${REPO}/releases/download/${VERSION}/${archive}"
  info "downloading ${archive} from release ${VERSION}"

  WORK_DIR="$(mktemp -d)"
  download "${url}" "${WORK_DIR}/${archive}"
  tar -xzf "${WORK_DIR}/${archive}" -C "${WORK_DIR}" ||
    fail "could not unpack ${archive} for ${TARGET}"
  [ -f "${WORK_DIR}/smugglr" ] || fail "${archive} for ${TARGET} does not contain smugglr"

  # Staged beside its destination, so the mv below is a rename: the verify runs
  # where the binary will live, and a failure leaves any earlier install alone.
  mkdir -p "${INSTALL_DIR}"
  STAGED="${INSTALL_DIR}/.smugglr.install.$$"
  cp "${WORK_DIR}/smugglr" "${STAGED}"
  chmod +x "${STAGED}"
  # The binary has no --version until #601.
  "${STAGED}" sync --help >/dev/null 2>&1 </dev/null ||
    fail "the ${TARGET} binary from release ${VERSION} did not run 'smugglr sync --help'; nothing was installed"
  mv -f "${STAGED}" "${INSTALL_DIR}/smugglr"
  STAGED=""

  info "installed smugglr ${VERSION} to ${INSTALL_DIR}/smugglr"
  path_advice
}

# Last, so a truncated `curl | bash` runs nothing.
main
