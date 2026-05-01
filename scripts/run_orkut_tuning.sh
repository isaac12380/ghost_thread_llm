#!/usr/bin/env bash

set -euo pipefail

extract_average() {
  awk '/^Average Time:/{print $3; exit}' "$1"
}

extract_trials() {
  awk '/^Trial Time:/{printf "%s%s", (count++ ? "," : ""), $3} END{print ""}' "$1"
}

extract_sources() {
  awk '/^Source:/{printf "%s%s", (count++ ? "," : ""), $2} END{if (count == 0) print "-"; else print ""}' "$1"
}

record() {
  printf '%s\n' "$*" >> "${commands_txt}"
}

run_one() {
  local cpu_list="$1"
  local logfile="$2"
  shift 2
  record "taskset -c ${cpu_list} $*"
  taskset -c "${cpu_list}" "$@" | tee "${logfile}"
}

append_sync_rows() {
  local variant="$1"
  local logfile="$2"
  awk -v variant="${variant}" '
    BEGIN {
      source = "-"
      too_fast = "-"
      too_slow = "-"
      serial_end = "-"
    }
    /^Source:/ { source = $2 }
    /^sync trace counters:/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^source=/) { split($i, a, "="); source = a[2] }
        else if ($i ~ /^too_fast=/) { split($i, a, "="); too_fast = a[2] }
        else if ($i ~ /^too_slow=/) { split($i, a, "="); too_slow = a[2] }
        else if ($i ~ /^serial_end=/) { split($i, a, "="); serial_end = a[2] }
      }
    }
    /^Trial Time:/ {
      print variant "," source "," too_fast "," too_slow "," serial_end "," $3
      source = "-"
      too_fast = "-"
      too_slow = "-"
      serial_end = "-"
    }
  ' "${logfile}" >> "${sync_csv}"
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gap_dir="${repo_root}/gap"
source_root="${gap_dir}/src"

graph_default="${gap_dir}/benchmark/graphs/com-Orkut.sg"
if [[ -f "${graph_default}" ]]; then
  graph="${GT_GRAPH:-${graph_default}}"
else
  graph="${GT_GRAPH:-/tmp/com-Orkut.sg}"
fi

repeat="${GT_REPEAT:-3}"
source_node="${GT_SOURCE:-2818235}"
single_cpu="${GT_SINGLE_CPU:-2}"
smt_cpus="${GT_SMT_CPUS:-2,3}"
output_root="${GT_OUTPUT_ROOT:-${gap_dir}/output/orkut_tuning}"
timestamp="$(date +%Y%m%d-%H%M%S)"

base_src="${source_root}/bfs.cc"
htpf_src="${source_root}/bfs_tpf.cc"
graph_name="$(basename "${graph}")"
graph_tag="${graph_name%.*}"
batch_root="${output_root}/bfs_${graph_tag}_n${repeat}_r${source_node}_${timestamp}"
bin_dir="${batch_root}/bin"
mkdir -p "${batch_root}" "${bin_dir}"

commands_txt="${batch_root}/commands.txt"
summary_csv="${batch_root}/summary.csv"
sync_csv="${batch_root}/htpf_sync.csv"
build_meta="${batch_root}/build_meta.txt"

base_bin="${bin_dir}/bfs_baseline"
omp_bin="${bin_dir}/bfs_homp"
base_log="${batch_root}/bfs_baseline.log"
omp_log="${batch_root}/bfs_homp.log"

if [[ ! -f "${graph}" ]]; then
  echo "Graph not found: ${graph}" >&2
  exit 1
fi

if [[ ! -f "${base_src}" || ! -f "${htpf_src}" ]]; then
  echo "Missing BFS sources under: ${source_root}" >&2
  exit 1
fi

: > "${commands_txt}"
{
  echo "graph=${graph}"
  echo "repeat=${repeat}"
  echo "source_node=${source_node}"
  echo "single_cpu=${single_cpu}"
  echo "smt_cpus=${smt_cpus}"
  echo "base_src=${base_src}"
  echo "htpf_src=${htpf_src}"
  echo "variant_matrix=5 macros x BEST on/off = 10 variants"
} > "${build_meta}"

common_args=(-f "${graph}" -n "${repeat}" -r "${source_node}" -l)

record "g++ -std=c++11 -pthread -O3 -Wall -w ${base_src} -o ${base_bin}"
g++ -std=c++11 -pthread -O3 -Wall -w "${base_src}" -o "${base_bin}"

record "g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w -DOMP -DNT=2 ${base_src} -o ${omp_bin}"
g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w -DOMP -DNT=2 "${base_src}" -o "${omp_bin}"

run_one "${single_cpu}" "${base_log}" "${base_bin}" "${common_args[@]}"
run_one "${smt_cpus}" "${omp_log}" "${omp_bin}" "${common_args[@]}"

base_avg="$(extract_average "${base_log}")"
omp_avg="$(extract_average "${omp_log}")"

printf 'variant,macro,best,average_time,speedup_vs_base,p,o,j,q,trial_times,sources,log_file,compile_flags\n' > "${summary_csv}"
printf 'baseline,-,-,%s,1.0000000000,-,-,-,-,"%s","%s",%s,"-"\n' \
  "${base_avg}" \
  "$(extract_trials "${base_log}")" \
  "$(extract_sources "${base_log}")" \
  "$(basename "${base_log}")" \
  >> "${summary_csv}"
printf 'homp,-,-,%s,%s,-,-,-,-,"%s","%s",%s,"-DOMP -DNT=2"\n' \
  "${omp_avg}" \
  "$(awk -v base="${base_avg}" -v cur="${omp_avg}" 'BEGIN{printf "%.10f", base / cur}')" \
  "$(extract_trials "${omp_log}")" \
  "$(extract_sources "${omp_log}")" \
  "$(basename "${omp_log}")" \
  >> "${summary_csv}"

printf 'variant,source,too_fast,too_slow,serial_end,trial_time\n' > "${sync_csv}"

variants=(
  "KRON:0:200:300:64:10"
  "KRON:1:200:300:64:10"
  "TWITTER:0:100:300:64:30"
  "TWITTER:1:100:300:64:30"
  "URAND:0:3:100:7:10"
  "URAND:1:3:100:7:10"
  "ROAD:0:120:1000:16:10"
  "ROAD:1:120:1000:16:10"
  "WEB:0:700:800:128:30"
  "WEB:1:700:800:128:30"
)

for spec in "${variants[@]}"; do
  IFS=: read -r graph_macro best_flag htpf_p htpf_o htpf_j htpf_q <<< "${spec}"
  macro_lower="$(printf '%s' "${graph_macro}" | tr '[:upper:]' '[:lower:]')"
  best_tag="nobest"
  compile_flags=(-DOMP)
  if [[ "${best_flag}" == "1" ]]; then
    best_tag="best"
    compile_flags+=(-DBEST)
  fi
  compile_flags+=(-DTUNING -DHTPF "-D${graph_macro}")

  variant_name="${macro_lower}_${best_tag}"
  htpf_bin="${bin_dir}/bfs_htpf_${variant_name}"
  htpf_log="${batch_root}/bfs_htpf_${variant_name}.log"
  htpf_flag_string="${compile_flags[*]}"

  record "g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w ${htpf_flag_string} ${htpf_src} -o ${htpf_bin}"
  g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w "${compile_flags[@]}" "${htpf_src}" -o "${htpf_bin}"

  run_one "${smt_cpus}" "${htpf_log}" "${htpf_bin}" "${common_args[@]}" -p "${htpf_p}" -o "${htpf_o}" -j "${htpf_j}" -q "${htpf_q}"

  htpf_avg="$(extract_average "${htpf_log}")"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s","%s",%s,"%s"\n' \
    "${variant_name}" \
    "${graph_macro}" \
    "${best_flag}" \
    "${htpf_avg}" \
    "$(awk -v base="${base_avg}" -v cur="${htpf_avg}" 'BEGIN{printf "%.10f", base / cur}')" \
    "${htpf_p}" \
    "${htpf_o}" \
    "${htpf_j}" \
    "${htpf_q}" \
    "$(extract_trials "${htpf_log}")" \
    "$(extract_sources "${htpf_log}")" \
    "$(basename "${htpf_log}")" \
    "${htpf_flag_string}" \
    >> "${summary_csv}"

  append_sync_rows "${variant_name}" "${htpf_log}"
done

echo "batch_root=${batch_root}"
echo "summary=${summary_csv}"
echo "htpf_sync=${sync_csv}"
