#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
  cat <<'EOF' >&2
Usage:
  run_snap_gap_experiments.sh <smt_core0> <smt_core1> [repeat]

This script downloads/converts four SuiteSparse SNAP graphs and runs
baseline, HOMP, SWPF, and HTPF variants for GAP kernels on them.

Outputs are written under gap/output/snap/runs/<timestamp>/.

Optional environment variables:
  SNAP_GAP_FIXED_SOURCE=1        Detect one source per graph for source-based
                                 kernels (bfs/bc/sssp) and pass -r so all
                                 trials/variants use the same source
  SNAP_GAP_WARMUP_RUNS=1         Number of warm-up executions per variant before
                                 capturing the measured log
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
  local class="$3"
  local graph_suffix="$4"
  local macro="$5"
  local trials="$6"

  local graph_path="benchmark/graphs/snap/${graph}${graph_suffix}"
  local base_bin="${kernel}"
  local omp_bin="${kernel}-omp"
  local swpf_bin="${kernel}-swpf"
  local tpf_bin="${kernel}_tpf"
  local compile_flags="-std=c++11 -pthread -O3 -Wall -w"
  local fixed_source=""
  local -a source_args=()
  local -a common_args=()

  if [[ "${kernel}" == "tc" ]]; then
    trials=1
  fi

  echo
  echo "=== ${kernel} on ${graph} (${class}) ==="

  g++ ${compile_flags} "src/${kernel}.cc" -o "${base_bin}"
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

  g++ ${compile_flags} -fopenmp -DOMP -DNT=2 "src/${kernel}.cc" -o "${omp_bin}"
  run_variant "${smt_core0},${smt_core1}" "${out_dir}/${kernel}-${graph}-homp.txt" \
    "./${omp_bin}" "${common_args[@]}"
  rm -f "${omp_bin}"

  g++ ${compile_flags} -DSWPF "src/${kernel}.cc" -o "${swpf_bin}"
  run_variant "${smt_core0}" "${out_dir}/${kernel}-${graph}-swpf.txt" \
    "./${swpf_bin}" "${common_args[@]}"
  rm -f "${swpf_bin}"

  g++ ${compile_flags} -DTUNING -DHTPF "-D${macro}" "src/${kernel}_tpf.cc" -o "${tpf_bin}"
  run_variant "${smt_core0},${smt_core1}" "${out_dir}/${kernel}-${graph}-htpf.txt" \
    "./${tpf_bin}" "${common_args[@]}" -p 16 -o 64 -j 8 -q 4
  rm -f "${tpf_bin}"
}

mkdir -p "${out_dir}"
ln -sfn "runs/${timestamp}" "${out_root}/latest"
prepare_graph_inputs

cd "${gap_dir}"

for kernel in "${kernels[@]}"; do
  for graph in "${graphs[@]}"; do
    class="$(class_for_graph "${graph}")"
    suffix="$(graph_suffix_for_kernel "${kernel}")"
    macro="$(macro_for_kernel_graph "${kernel}" "${class}")"
    run_case "${kernel}" "${graph}" "${class}" "${suffix}" "${macro}" "${repeat}"
  done
done

echo
echo "Finished. Logs are in ${out_dir}"
echo "Latest symlink: ${out_root}/latest"
