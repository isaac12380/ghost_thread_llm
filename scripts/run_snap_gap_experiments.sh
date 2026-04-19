#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
  cat <<'EOF' >&2
Usage: run_snap_gap_experiments.sh <smt_core0> <smt_core1> [repeat]

Download/convert the retained SNAP graphs, then run GAP baseline, HOMP,
SWPF, and HTPF variants.

Env:
  SNAP_GAP_FIXED_SOURCE=1   Reuse one detected source for bfs/bc/sssp
  SNAP_GAP_WARMUP_RUNS=1    Warm-up runs before the measured run
  SNAP_GAP_RUN_NAME=<tag>   Override output directory name
EOF
  exit 1
fi

smt_core0="$1"
smt_core1="$2"
repeat="${3:-1}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gap_dir="${repo_root}/gap"
out_root="${gap_dir}/output/snap"
raw_dir="${gap_dir}/benchmark/graphs/raw/snap"
graph_dir="${gap_dir}/benchmark/graphs/snap"
base_url="http://sparse-files.engr.tamu.edu/MM/SNAP"
timestamp="${SNAP_GAP_RUN_NAME:-$(date +%Y%m%d-%H%M%S)}"
out_dir="${out_root}/runs/${timestamp}"
fixed_source_enabled="${SNAP_GAP_FIXED_SOURCE:-1}"
warmup_runs="${SNAP_GAP_WARMUP_RUNS:-1}"

declare -a kernels=("bfs" "bc" "cc" "pr" "sssp" "tc")
declare -a graphs=("web-Stanford" "web-Google" "amazon0312" "roadNet-PA")
compile_flags=(-std=c++11 -pthread -O3 -Wall -w)

# Default HTPF CLI args used when we do not have a workload-specific setting.
# Note: in these binaries, -q is interpreted as (serialize_threshold - unserialize_threshold).
default_htpf_args=(-p 16 -o 64 -j 8 -q 4)

htpf_args_for_case() {
  local kernel="$1"
  local graph="$2"
  local class="$3"

  case "${kernel}:${graph}" in
    # Second-round tuning based on 20260418-171528 -> 20260420-001859:
    # keep the larger bfs settings where they helped, but revert web-Google.
    bfs:web-Stanford|bfs:amazon0312|bfs:roadNet-PA)
      echo "-p 500 -o 300 -j 128 -q 10"
      ;;
    bfs:web-Google)
      printf '%s ' "${default_htpf_args[@]}"
      echo
      ;;

    # cc improves on Stanford/Google with the WEB defaults, but amazon0312 regressed.
    cc:web-Stanford|cc:web-Google)
      echo "-p 800 -o 150 -j 130 -q 50"
      ;;
    cc:amazon0312|cc:roadNet-PA)
      printf '%s ' "${default_htpf_args[@]}"
      echo
      ;;

    # sssp remains the worst line item after two rounds, so push the larger WEB
    # settings a bit further on the heavy web-like graphs.
    sssp:web-Stanford|sssp:amazon0312)
      echo "-p 14 -o 160 -j 64 -q 10"
      ;;
    sssp:web-Google|sssp:roadNet-PA)
      printf '%s ' "${default_htpf_args[@]}"
      echo
      ;;

    # pr still regresses broadly with the default 16/64/8/4. Use a looser outer
    # sync policy to reduce coordination overhead and let the PF thread jump ahead.
    pr:web-Stanford|pr:web-Google|pr:amazon0312|pr:roadNet-PA)
      echo "-p 64 -o 256 -j 64 -q 32"
      ;;

    # tc ROADU path has an explicit !TUNING default.
    tc:roadNet-PA)
      echo "-p 8 -o 23 -j 7 -q 5"
      ;;
    # tc web workloads are still clearly below baseline with the default policy.
    # Try a wider sync window without perturbing amazon0312, which already wins.
    tc:web-Stanford|tc:web-Google)
      echo "-p 32 -o 128 -j 16 -q 16"
      ;;

    *)
      case "${kernel}:${class}" in
        *)
          printf '%s ' "${default_htpf_args[@]}"
          echo
          ;;
      esac
      ;;
  esac
}

class_for_graph() {
  case "$1" in
    roadNet-PA) echo "road" ;;
    web-Stanford|web-Google|amazon0312) echo "web" ;;
    *)
      echo "Unsupported SNAP graph: $1" >&2
      exit 1
      ;;
  esac
}

graph_suffix_for_kernel() {
  case "$1" in
    sssp) echo ".wsg" ;;
    tc) echo "U.sg" ;;
    *) echo ".sg" ;;
  esac
}

prepare_graph_inputs() {
  local graph archive extract_dir mtx

  mkdir -p "${raw_dir}" "${graph_dir}"
  cd "${gap_dir}"
  g++ -std=c++11 -pthread -O3 -Wall -w "src/converter.cc" -o converter

  for graph in "${graphs[@]}"; do
    archive="${raw_dir}/${graph}.tar.gz"
    extract_dir="${raw_dir}/${graph}"
    mtx="${extract_dir}/${graph}.mtx"

    if [[ ! -s "${archive}" ]]; then
      echo "Downloading ${graph}.tar.gz"
      curl --fail --location --retry 3 --connect-timeout 15 \
        -o "${archive}" "${base_url}/${graph}.tar.gz"
    else
      echo "Using cached archive ${archive}"
    fi

    if [[ ! -f "${mtx}" ]]; then
      echo "Extracting ${graph}.tar.gz"
      tar -xzf "${archive}" -C "${raw_dir}"
    else
      echo "Using extracted matrix ${mtx}"
    fi

    if [[ ! -f "${mtx}" ]]; then
      echo "Expected matrix file not found: ${mtx}" >&2
      exit 1
    fi

    if [[ ! -f "${graph_dir}/${graph}.sg" ]]; then
      ./converter -f "${mtx}" -b "${graph_dir}/${graph}.sg"
    fi

    if [[ ! -f "${graph_dir}/${graph}.wsg" ]]; then
      ./converter -f "${mtx}" -wb "${graph_dir}/${graph}.wsg"
    fi

    if [[ ! -f "${graph_dir}/${graph}U.sg" ]]; then
      ./converter -sf "${mtx}" -b "${graph_dir}/${graph}U.sg"
    fi
  done

  echo
  echo "Prepared SNAP graph inputs in ${graph_dir}:"
  ls -lh "${graph_dir}"
}

macro_for_kernel_graph() {
  local kernel="$1"
  local class="$2"

  local macro
  macro="$(printf '%s' "${class}" | tr '[:lower:]' '[:upper:]')"
  if [[ "${kernel}" == "tc" ]]; then
    macro="${macro}U"
  fi
  printf '%s\n' "${macro}"
}

detect_source() {
  local bin="$1"
  local graph_path="$2"
  local source

  source="$(
    taskset -c "${smt_core0}" "./${bin}" -f "${graph_path}" -n 1 -l 2>/dev/null \
      | awk '/^Source:/{print $2; exit}'
  )"

  if [[ -z "${source}" ]]; then
    echo "Failed to detect source for ${graph_path}" >&2
    exit 1
  fi

  printf '%s\n' "${source}"
}

kernel_uses_source() {
  case "$1" in
    bfs|bc|sssp) return 0 ;;
    *) return 1 ;;
  esac
}

run_variant() {
  local cpu_list="$1"
  local logfile="$2"
  shift 2
  local -a cmd=("$@")
  local warm

  for ((warm=0; warm < warmup_runs; warm++)); do
    taskset -c "${cpu_list}" "${cmd[@]}" > /dev/null 2>&1
  done

  taskset -c "${cpu_list}" "${cmd[@]}" > "${logfile}" 2>&1
}

run_case() {
  local kernel="$1"
  local graph="$2"
  local trials="$3"

  local class
  local graph_suffix
  local macro
  local graph_path
  local base_bin="${kernel}"
  local omp_bin="${kernel}-omp"
  local swpf_bin="${kernel}-swpf"
  local tpf_bin="${kernel}_tpf"
  local htpf_arg_str=""
  local -a htpf_args=()
  local fixed_source=""
  local -a source_args=()
  local -a common_args=()

  class="$(class_for_graph "${graph}")"
  graph_suffix="$(graph_suffix_for_kernel "${kernel}")"
  macro="$(macro_for_kernel_graph "${kernel}" "${class}")"
  graph_path="benchmark/graphs/snap/${graph}${graph_suffix}"

  if [[ "${kernel}" == "tc" ]]; then
    trials=1
  fi

  echo
  echo "=== ${kernel} on ${graph} (${class}) ==="
  htpf_arg_str="$(htpf_args_for_case "${kernel}" "${graph}" "${class}")"
  # shellcheck disable=SC2206
  htpf_args=(${htpf_arg_str})
  echo "HTPF args: ${htpf_arg_str}"

  g++ "${compile_flags[@]}" "src/${kernel}.cc" -o "${base_bin}"
  if kernel_uses_source "${kernel}" && [[ "${fixed_source_enabled}" != "0" ]]; then
    fixed_source="$(detect_source "${base_bin}" "${graph_path}")"
    source_args=(-r "${fixed_source}")
    printf 'source=%s\n' "${fixed_source}" > "${out_dir}/${kernel}-${graph}-source.txt"
    echo "Using fixed ${kernel} source ${fixed_source}"
  fi
  common_args=(-f "${graph_path}" -n "${trials}" "${source_args[@]}")
  run_variant "${smt_core0}" "${out_dir}/${kernel}-${graph}-baseline.txt" \
    "./${base_bin}" "${common_args[@]}"
  rm -f "${base_bin}"

  g++ "${compile_flags[@]}" -fopenmp -DOMP -DNT=2 "src/${kernel}.cc" -o "${omp_bin}"
  run_variant "${smt_core0},${smt_core1}" "${out_dir}/${kernel}-${graph}-homp.txt" \
    "./${omp_bin}" "${common_args[@]}"
  rm -f "${omp_bin}"

  g++ "${compile_flags[@]}" -DSWPF "src/${kernel}.cc" -o "${swpf_bin}"
  run_variant "${smt_core0}" "${out_dir}/${kernel}-${graph}-swpf.txt" \
    "./${swpf_bin}" "${common_args[@]}"
  rm -f "${swpf_bin}"

  g++ "${compile_flags[@]}" -DTUNING -DHTPF "-D${macro}" "src/${kernel}_tpf.cc" -o "${tpf_bin}"
  run_variant "${smt_core0},${smt_core1}" "${out_dir}/${kernel}-${graph}-htpf.txt" \
    "./${tpf_bin}" "${common_args[@]}" "${htpf_args[@]}"
  rm -f "${tpf_bin}"
}

mkdir -p "${out_dir}"
ln -sfn "runs/${timestamp}" "${out_root}/latest"
echo "SNAP GAP run: repeat=${repeat} out=${out_dir}"
prepare_graph_inputs

cd "${gap_dir}"

for kernel in "${kernels[@]}"; do
  for graph in "${graphs[@]}"; do
    run_case "${kernel}" "${graph}" "${repeat}"
  done
done

echo
echo "Finished. Logs are in ${out_dir}"
echo "Latest symlink: ${out_root}/latest"
