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
  SNAP_GAP_SOURCE_POLICY=empirical-htpf|default
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
source_policy="${SNAP_GAP_SOURCE_POLICY:-empirical-htpf}"

declare -a kernels=("bfs" "bc" "cc" "pr" "sssp" "tc")
declare -a graphs=("web-Stanford" "web-Google" "amazon0312" "roadNet-PA")
compile_flags=(-std=c++11 -pthread -O3 -Wall -w)

# Default HTPF CLI args used when we do not have a workload-specific setting.
# Note: in these binaries, -q is interpreted as
# (serialize_threshold - unserialize_threshold).
default_htpf_args=(-p 16 -o 64 -j 8 -q 4)

htpf_args_for_case() {
  local kernel="$1"
  local graph="$2"

  case "${kernel}:${graph}" in
    # Imported from the best known tuning results in this repo:
    # - gap/output/snap/tuned/htpf_tuning_best.csv
    bfs:web-Stanford) echo "-p 1400 -o 800 -j 0 -q 30" ;;
    bfs:web-Google)   echo "-p 16 -o 64 -j 8 -q 4" ;;
    bfs:amazon0312)   echo "-p 1400 -o 800 -j 64 -q 15" ;;
    bfs:roadNet-PA)   echo "-p 120 -o 1000 -j 32 -q 10" ;;

    bc:web-Stanford)  echo "-p 200 -o 600 -j 0 -q 10" ;;
    bc:web-Google)    echo "-p 100 -o 600 -j 0 -q 10" ;;
    bc:amazon0312)    echo "-p 160 -o 600 -j 32 -q 10" ;;
    bc:roadNet-PA)    echo "-p 500 -o 80 -j 0 -q 10" ;;

    cc:web-Stanford)  echo "-p 800 -o 1600 -j 200 -q 50" ;;
    cc:web-Google)    echo "-p 800 -o 600 -j 400 -q 50" ;;
    cc:amazon0312)    echo "-p 18 -o 18 -j 30 -q 18" ;;
    cc:roadNet-PA)    echo "-p 800 -o 300 -j 180 -q 50" ;;

    pr:web-Stanford)  echo "-p 20 -o 140 -j 0 -q 5" ;;
    pr:web-Google)    echo "-p 40 -o 50 -j 60 -q 5" ;;
    pr:amazon0312)    echo "-p 16 -o 64 -j 8 -q 4" ;;
    pr:roadNet-PA)    echo "-p 20 -o 140 -j 60 -q 5" ;;

    sssp:web-Stanford) echo "-p 16 -o 64 -j 8 -q 4" ;;
    sssp:web-Google)   echo "-p 26 -o 32 -j 18 -q 7" ;;
    sssp:amazon0312)   echo "-p 600 -o 120 -j 32 -q 10" ;;
    sssp:roadNet-PA)   echo "-p 1000 -o 120 -j 0 -q 10" ;;

    tc:web-Stanford)  echo "-p 16 -o 64 -j 0 -q 2" ;;
    tc:web-Google)    echo "-p 10 -o 80 -j 0 -q 8" ;;
    tc:amazon0312)    echo "-p 20 -o 80 -j 14 -q 8" ;;
    tc:roadNet-PA)    echo "-p 60 -o 160 -j 7 -q 50" ;;

    *)
      printf '%s ' "${default_htpf_args[@]}"
      echo
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

preferred_source_for_case() {
  local kernel="$1"
  local graph="$2"

  case "${kernel}:${graph}" in
    # Only include sources that were validated by direct source sweep.
    bfs:web-Google)   echo "685695" ;;
    bc:web-Google)    echo "685695" ;;
    bc:web-Stanford)  echo "266154" ;;
    *)
      return 1
      ;;
  esac
}

choose_source() {
  local kernel="$1"
  local graph="$2"
  local bin="$3"
  local graph_path="$4"
  local source=""

  case "${source_policy}" in
    empirical-htpf)
      if source="$(preferred_source_for_case "${kernel}" "${graph}")"; then
        echo "Using empirical HTPF-aware source ${source} for ${kernel}-${graph}" >&2
        printf '%s\n' "${source}"
        return 0
      fi
      ;;
    default)
      ;;
    *)
      echo "Unsupported SNAP_GAP_SOURCE_POLICY=${source_policy}" >&2
      exit 1
      ;;
  esac

  detect_source "${bin}" "${graph_path}"
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

extract_average_time() {
  local logfile="$1"

  awk '/^Average Time:/{print $3; exit}' "${logfile}"
}

emit_result_summary() {
  local runtime_csv="${out_dir}/runtime.csv"
  local speedup_csv="${out_dir}/speedup.csv"
  local kernel
  local graph
  local workload
  local baseline_log
  local homp_log
  local swpf_log
  local htpf_log
  local baseline_time
  local homp_time
  local swpf_time
  local htpf_time
  local homp_speedup
  local swpf_speedup
  local htpf_speedup

  printf 'workload,baseline,homp,swpf,htpf\n' > "${runtime_csv}"
  printf 'workload,baseline,homp,swpf,htpf\n' > "${speedup_csv}"

  for kernel in "${kernels[@]}"; do
    for graph in "${graphs[@]}"; do
      workload="${kernel}-${graph}"
      baseline_log="${out_dir}/${workload}-baseline.txt"
      homp_log="${out_dir}/${workload}-homp.txt"
      swpf_log="${out_dir}/${workload}-swpf.txt"
      htpf_log="${out_dir}/${workload}-htpf.txt"

      baseline_time="$(extract_average_time "${baseline_log}")"
      homp_time="$(extract_average_time "${homp_log}")"
      swpf_time="$(extract_average_time "${swpf_log}")"
      htpf_time="$(extract_average_time "${htpf_log}")"

      if [[ -z "${baseline_time}" || -z "${homp_time}" || -z "${swpf_time}" || -z "${htpf_time}" ]]; then
        echo "Skipping summary for ${workload}: missing Average Time in one or more logs" >&2
        continue
      fi

      homp_speedup="$(awk -v b="${baseline_time}" -v t="${homp_time}" 'BEGIN{printf "%.10f", b / t}')"
      swpf_speedup="$(awk -v b="${baseline_time}" -v t="${swpf_time}" 'BEGIN{printf "%.10f", b / t}')"
      htpf_speedup="$(awk -v b="${baseline_time}" -v t="${htpf_time}" 'BEGIN{printf "%.10f", b / t}')"

      printf '%s,%s,%s,%s,%s\n' \
        "${workload}" "${baseline_time}" "${homp_time}" "${swpf_time}" "${htpf_time}" \
        >> "${runtime_csv}"
      printf '%s,1.0000000000,%s,%s,%s\n' \
        "${workload}" "${homp_speedup}" "${swpf_speedup}" "${htpf_speedup}" \
        >> "${speedup_csv}"
    done
  done
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
  htpf_arg_str="$(htpf_args_for_case "${kernel}" "${graph}")"
  # shellcheck disable=SC2206
  htpf_args=(${htpf_arg_str})
  echo "HTPF args: ${htpf_arg_str}"

  g++ "${compile_flags[@]}" "src/${kernel}.cc" -o "${base_bin}"
  if kernel_uses_source "${kernel}" && [[ "${fixed_source_enabled}" != "0" ]]; then
    fixed_source="$(choose_source "${kernel}" "${graph}" "${base_bin}" "${graph_path}")"
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

emit_result_summary

echo
echo "Finished. Logs are in ${out_dir}"
echo "Latest symlink: ${out_root}/latest"
echo "Runtime summary: ${out_dir}/runtime.csv"
echo "Speedup summary: ${out_dir}/speedup.csv"
