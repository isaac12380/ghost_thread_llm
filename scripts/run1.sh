#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF' >&2
Usage: run1.sh [options]

Options:
  --benchmark <name>    Benchmark name, e.g. bc
  --graph <path>        Graph path
  --repeat <n>          Trial count, default 3
  --source <node>       Optional fixed source (-r)
  --single-cpu <cpu>    Baseline CPU, default 6
  --smt-cpus <list>     OMP/HTPF CPU list, default 6,7
  --graph-macro <name>  Override graph macro for HTPF, e.g. KRON
  --p <value>           HTPF sync_frequency, default 80
  --o <value>           HTPF serialize_threshold, default 80
  --j <value>           HTPF skip_offset, default 32
  --q <value>           HTPF unserialize gap, default 10
  --output-root <dir>   Output root
  --no-log              Do not pass -l
  --help                Show help
EOF
  exit 1
}

sanitize() {
  printf '%s' "$1" | tr '/[:space:]' '__' | tr -cs '[:alnum:]_.-' '_'
}

infer_graph_macro() {
  local path_lower
  path_lower="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "${path_lower}" in
    *kron*) echo "KRON" ;;
    *twitter*|*orkut*|*livejournal*|*pokec*) echo "TWITTER" ;;
    *road*) echo "ROAD" ;;
    *web*|*google*|*stanford*|*amazon*) echo "WEB" ;;
    *)
      echo "Could not infer graph macro from path: $1" >&2
      echo "Use --graph-macro explicitly." >&2
      exit 1
      ;;
  esac
}

extract_average() {
  awk '/^Average Time:/{print $3; exit}' "$1"
}

extract_trials() {
  awk '/^Trial Time:/{printf "%s%s", (count++ ? "," : ""), $3} END{print ""}' "$1"
}

extract_sources() {
  awk '/^Source:/{printf "%s%s", (count++ ? "," : ""), $2} END{if (count == 0) print "-"; else print ""}' "$1"
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gap_dir="${repo_root}/gap"

benchmark="bc"
graph="${gap_dir}/benchmark/graphs/kron.sg"
repeat="3"
source_node=""
single_cpu="6"
smt_cpus="6,7"
graph_macro=""
htpf_p="80"
htpf_o="80"
htpf_j="32"
htpf_q="10"
output_root="${gap_dir}/output/run1"
log_flag=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --benchmark) benchmark="${2:-}"; shift 2 ;;
    --graph) graph="${2:-}"; shift 2 ;;
    --repeat) repeat="${2:-}"; shift 2 ;;
    --source) source_node="${2:-}"; shift 2 ;;
    --single-cpu) single_cpu="${2:-}"; shift 2 ;;
    --smt-cpus) smt_cpus="${2:-}"; shift 2 ;;
    --graph-macro) graph_macro="${2:-}"; shift 2 ;;
    --p) htpf_p="${2:-}"; shift 2 ;;
    --o) htpf_o="${2:-}"; shift 2 ;;
    --j) htpf_j="${2:-}"; shift 2 ;;
    --q) htpf_q="${2:-}"; shift 2 ;;
    --output-root) output_root="${2:-}"; shift 2 ;;
    --no-log) log_flag=0; shift ;;
    --help|-h) usage ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
done

if [[ ! -f "${graph}" ]]; then
  echo "Graph not found: ${graph}" >&2
  exit 1
fi

if [[ -z "${graph_macro}" ]]; then
  graph_macro="$(infer_graph_macro "${graph}")"
fi

base_src="${gap_dir}/src/${benchmark}.cc"
htpf_src="${gap_dir}/src/${benchmark}_tpf.cc"
if [[ ! -f "${base_src}" ]]; then
  echo "Benchmark source not found: ${base_src}" >&2
  exit 1
fi
if [[ ! -f "${htpf_src}" ]]; then
  echo "HTPF source not found: ${htpf_src}" >&2
  exit 1
fi

graph_tag="$(sanitize "$(basename "${graph}" .sg)")"
timestamp="$(date +%Y%m%d-%H%M%S)"
source_tag="randsrc"
if [[ -n "${source_node}" ]]; then
  source_tag="r${source_node}"
fi
out_dir="${output_root}/${benchmark}_${graph_tag}_n${repeat}_${source_tag}_p${htpf_p}_o${htpf_o}_j${htpf_j}_q${htpf_q}_${timestamp}"
bin_dir="${out_dir}/bin"
mkdir -p "${out_dir}" "${bin_dir}"

base_bin="${bin_dir}/${benchmark}_base"
omp_bin="${bin_dir}/${benchmark}_omp"
htpf_bin="${bin_dir}/${benchmark}_htpf"

base_log="${out_dir}/${benchmark}_base.log"
omp_log="${out_dir}/${benchmark}_omp.log"
htpf_log="${out_dir}/${benchmark}_htpf.log"
summary_csv="${out_dir}/summary.csv"
sync_csv="${out_dir}/htpf_sync.csv"
commands_txt="${out_dir}/commands.txt"

common_args=(-f "${graph}" -n "${repeat}")
if [[ -n "${source_node}" ]]; then
  common_args+=(-r "${source_node}")
fi
if [[ "${log_flag}" == "1" ]]; then
  common_args+=(-l)
fi

htpf_args=(-p "${htpf_p}" -o "${htpf_o}" -j "${htpf_j}" -q "${htpf_q}")

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

: > "${commands_txt}"
record "g++ -std=c++11 -pthread -O3 -Wall -w ${base_src} -o ${base_bin}"
g++ -std=c++11 -pthread -O3 -Wall -w "${base_src}" -o "${base_bin}"

record "g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w -DOMP -DNT=2 ${base_src} -o ${omp_bin}"
g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w -DOMP -DNT=2 "${base_src}" -o "${omp_bin}"

record "g++ -std=c++11 -pthread -O3 -Wall -w -DTUNING -DHTPF -D${graph_macro} ${htpf_src} -o ${htpf_bin}"
g++ -std=c++11 -pthread -O3 -Wall -w -DTUNING -DHTPF "-D${graph_macro}" "${htpf_src}" -o "${htpf_bin}"

run_one "${single_cpu}" "${base_log}" "${base_bin}" "${common_args[@]}"
run_one "${smt_cpus}" "${omp_log}" "${omp_bin}" "${common_args[@]}"
run_one "${smt_cpus}" "${htpf_log}" "${htpf_bin}" "${common_args[@]}" "${htpf_args[@]}"

base_avg="$(extract_average "${base_log}")"

printf 'variant,average_time,speedup_vs_base,trial_times,sources,log_file\n' > "${summary_csv}"
for variant in base omp htpf; do
  case "${variant}" in
    base) logfile="${base_log}" ;;
    omp) logfile="${omp_log}" ;;
    htpf) logfile="${htpf_log}" ;;
  esac
  avg="$(extract_average "${logfile}")"
  speedup="$(awk -v base="${base_avg}" -v cur="${avg}" 'BEGIN{printf "%.10f", base / cur}')"
  printf '%s,%s,%s,"%s","%s",%s\n' \
    "${variant}" \
    "${avg}" \
    "${speedup}" \
    "$(extract_trials "${logfile}")" \
    "$(extract_sources "${logfile}")" \
    "$(basename "${logfile}")" \
    >> "${summary_csv}"
done

printf 'source,too_fast,too_slow,serial_end,trial_time\n' > "${sync_csv}"
awk '
  /^Source:/ { source = $2 }
  /^sync trace counters:/ {
    for (i = 1; i <= NF; i++) {
      if ($i ~ /^too_fast=/) { split($i, a, "="); too_fast = a[2] }
      else if ($i ~ /^too_slow=/) { split($i, a, "="); too_slow = a[2] }
      else if ($i ~ /^serial_end=/) { split($i, a, "="); serial_end = a[2] }
    }
  }
  /^Trial Time:/ { print source "," too_fast "," too_slow "," serial_end "," $3 }
' "${htpf_log}" >> "${sync_csv}"

echo "out_dir=${out_dir}"
echo "summary=${summary_csv}"
echo "htpf_sync=${sync_csv}"
