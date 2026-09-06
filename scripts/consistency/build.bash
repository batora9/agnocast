#!/usr/bin/env bash
set -euo pipefail
#
# Build the metadata-consistency harness against one or both Agnocast backends.
#
#   scripts/consistency/build.bash              # both backends
#   scripts/consistency/build.bash kmod
#   scripts/consistency/build.bash daemon
#
# Installs into the same ws/<backend>/ trees as scripts/bench/build.bash.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../bench/common.bash
source "${SCRIPT_DIR}/../bench/common.bash"

BACKENDS=()
CLEAN=false
BUILD_TYPE=Release

while [[ $# -gt 0 ]]; do
  case "$1" in
  kmod | daemon) BACKENDS+=("$1"); shift ;;
  --clean) CLEAN=true; shift ;;
  --debug) BUILD_TYPE=RelWithDebInfo; shift ;;
  -h | --help) sed -n '3,11p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) die "unknown option: $1" ;;
  esac
done
[[ ${#BACKENDS[@]} -gt 0 ]] || BACKENDS=(kmod daemon)

[[ -d "${AGNOCAST_ROOT}/src/agnocastlib" ]] ||
  die "AGNOCAST_ROOT does not look like an Agnocast checkout: ${AGNOCAST_ROOT}"

[[ -n "${ROS_DISTRO:-}" ]] || die "ROS is not sourced. Run: source /opt/ros/humble/setup.bash"

HEAPHOOK_SRC="${AGNOCAST_ROOT}/agnocast_heaphook/target/release/libagnocast_heaphook.so"
echo "==> Building agnocast_heaphook"
(cd "${AGNOCAST_ROOT}/agnocast_heaphook" && cargo build --release)
[[ -f "${HEAPHOOK_SRC}" ]] || die "heaphook build produced no ${HEAPHOOK_SRC}"

consistency_pub_bin() {
  echo "$(install_dir "$1")/agnocast_consistency/lib/agnocast_consistency/consistency_publisher"
}

build_backend() {
  local backend="$1"
  local ws build install extra_cmake=()

  ws="$(ws_dir "${backend}")"
  build="$(build_dir "${backend}")"
  install="$(install_dir "${backend}")"

  echo ""
  echo "============================================================"
  echo " Building consistency harness: ${backend} ($(backend_label "${backend}"))"
  echo "============================================================"

  if [[ "${CLEAN}" == "true" ]]; then
    rm -rf "${build}" "${install}"
  fi
  mkdir -p "${ws}"

  if [[ "${backend}" == "kmod" ]]; then
    echo "==> Building agnocast kernel module"
    (cd "${AGNOCAST_ROOT}/agnocast_kmod" && make)
  else
    echo "==> Building agnocast_daemon"
    (cd "${AGNOCAST_ROOT}/agnocast_daemon" && make CMAKE_ARGS=-DAGNOCAST_BENCH_TIMING=ON)
    extra_cmake+=(-DAGNOCAST_USE_DAEMON=ON)
  fi

  echo "==> colcon build (${backend})"
  colcon build \
    --base-paths "${AGNOCAST_ROOT}/src" \
    --build-base "${build}" \
    --install-base "${install}" \
    --packages-up-to agnocast_consistency \
    --symlink-install \
    --cmake-args \
    "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}" \
    -DAGNOCAST_BENCH_TIMING=ON \
    "${extra_cmake[@]}"

  install -D -m 0755 "${HEAPHOOK_SRC}" "$(heaphook_lib "${backend}")"
  echo "==> ${backend}: $(consistency_pub_bin "${backend}")"
}

for backend in "${BACKENDS[@]}"; do
  build_backend "${backend}"
done

echo ""
echo "============================================================"
echo " Build complete: ${BACKENDS[*]}"
echo "============================================================"
