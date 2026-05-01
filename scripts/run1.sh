#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF' >&2
Usage: run1.sh [options]

Options:
  --benchmark <name>    Benchmark name: bc | bfs | cc | pr | sssp | tc
  --graph <path>        Graph path; defaults follow eval figure6 per benchmark
  --repeat <n>          Trial count, default 3
  --source <node>       Optional fixed source (-r)
  --source-root <dir>   Source tree root that contains <bench>.cc and <bench>_tpf.cc
  --single-cpu <cpu>    Baseline CPU, default 6
  --smt-cpus <list>     OMP/HTPF CPU list, default 6,7
  --graph-macro <name>  Override graph macro for HTPF, e.g. KRON
  --p <value>           HTPF sync_frequency; default follows eval figure6
  --o <value>           HTPF serialize_threshold; default follows eval figure6
  --j <value>           HTPF skip_offset; default follows eval figure6
  --q <value>           HTPF unserialize gap; default follows eval figure6
  --output-root <dir>   Output root
  --log                 Pass -l to binaries
  --no-log              Do not pass -l
  --help                Show help
EOF
  exit 1
}

sanitize() {
  printf '%s' "$1" | tr '/[:space:]' '__' | tr -cs '[:alnum:]_.-' '_'
}

infer_graph_macro() {
  local benchmark_name="$1"
  local graph_path="$2"
  local path_lower
  path_lower="$(printf '%s' "${graph_path}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${benchmark_name}" == "tc" ]]; then
    case "${path_lower}" in
      *twitteru*) echo "TWITTERU"; return 0 ;;
      *roadu*) echo "ROADU"; return 0 ;;
      *webu*) echo "WEBU"; return 0 ;;
      *kron*) echo "KRON"; return 0 ;;
      *urand*) echo "URAND"; return 0 ;;
      *)
        echo "Could not infer tc graph macro from path: ${graph_path}" >&2
        echo "Use --graph-macro explicitly." >&2
        exit 1
        ;;
    esac
  fi

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

default_graph_path() {
  local benchmark_name="$1"
  case "${benchmark_name}" in
    sssp) printf '%s\n' "${gap_dir}/benchmark/graphs/kron.wsg" ;;
    *) printf '%s\n' "${gap_dir}/benchmark/graphs/kron.sg" ;;
  esac
}

normalize_graph_name() {
  local graph_path="$1"
  local graph_name
  graph_name="$(basename "${graph_path}")"
  graph_name="${graph_name%.*}"
  printf '%s\n' "${graph_name}"
}

canonicalize_graph_name() {
  local graph_name_lower="$1"
  case "${graph_name_lower}" in
    *-trainu) printf '%s\n' "${graph_name_lower%-trainu}u" ;;
    *-train) printf '%s\n' "${graph_name_lower%-train}" ;;
    *) printf '%s\n' "${graph_name_lower}" ;;
  esac
}

canonicalize_graph_name_for_params() {
  local benchmark_name="$1"
  local graph_name_lower="$2"
  graph_name_lower="$(canonicalize_graph_name "${graph_name_lower}")"
  if [[ "${benchmark_name}" == "tc" ]]; then
    case "${graph_name_lower}" in
      kronu) graph_name_lower="kron" ;;
      twitteru) graph_name_lower="twitter" ;;
      urandu) graph_name_lower="urand" ;;
      roadu) graph_name_lower="road" ;;
      webu) graph_name_lower="web" ;;
    esac
  fi
  printf '%s\n' "${graph_name_lower}"
}

set_default_htpf_params() {
  local benchmark_name="$1"
  local graph_name_lower="$2"
  graph_name_lower="$(canonicalize_graph_name_for_params "${benchmark_name}" "${graph_name_lower}")"
  case "${benchmark_name}" in
    bfs)
      case "${graph_name_lower}" in
        kron) htpf_p="200"; htpf_o="300"; htpf_j="64"; htpf_q="10" ;;
        twitter) htpf_p="100"; htpf_o="300"; htpf_j="64"; htpf_q="30" ;;
        urand) htpf_p="3"; htpf_o="100"; htpf_j="7"; htpf_q="10" ;;
        road) htpf_p="120"; htpf_o="1000"; htpf_j="16"; htpf_q="10" ;;
        web) htpf_p="700"; htpf_o="800"; htpf_j="128"; htpf_q="30" ;;
        *) return 1 ;;
      esac
      ;;
    bc)
      case "${graph_name_lower}" in
        kron) htpf_p="80"; htpf_o="80"; htpf_j="32"; htpf_q="10" ;;
        twitter) htpf_p="80"; htpf_o="150"; htpf_j="32"; htpf_q="10" ;;
        urand) htpf_p="2"; htpf_o="60"; htpf_j="8"; htpf_q="1" ;;
        road) htpf_p="500"; htpf_o="80"; htpf_j="0"; htpf_q="30" ;;
        web) htpf_p="200"; htpf_o="600"; htpf_j="0"; htpf_q="10" ;;
        *) return 1 ;;
      esac
      ;;
    cc)
      case "${graph_name_lower}" in
        kron) htpf_p="800"; htpf_o="800"; htpf_j="400"; htpf_q="50" ;;
        twitter) htpf_p="800"; htpf_o="600"; htpf_j="400"; htpf_q="50" ;;
        urand) htpf_p="18"; htpf_o="18"; htpf_j="15"; htpf_q="3" ;;
        road) htpf_p="800"; htpf_o="600"; htpf_j="400"; htpf_q="50" ;;
        web) htpf_p="800"; htpf_o="150"; htpf_j="90"; htpf_q="50" ;;
        *) return 1 ;;
      esac
      ;;
    sssp)
      case "${graph_name_lower}" in
        kron) htpf_p="600"; htpf_o="120"; htpf_j="32"; htpf_q="10" ;;
        twitter) htpf_p="1000"; htpf_o="120"; htpf_j="32"; htpf_q="10" ;;
        urand) htpf_p="7"; htpf_o="60"; htpf_j="21"; htpf_q="11" ;;
        web) htpf_p="13"; htpf_o="32"; htpf_j="18"; htpf_q="7" ;;
        *) return 1 ;;
      esac
      ;;
    pr)
      case "${graph_name_lower}" in
        kron) htpf_p="20"; htpf_o="140"; htpf_j="30"; htpf_q="5" ;;
        twitter) htpf_p="10"; htpf_o="140"; htpf_j="30"; htpf_q="15" ;;
        urand) htpf_p="20"; htpf_o="100"; htpf_j="30"; htpf_q="5" ;;
        road) htpf_p="20"; htpf_o="100"; htpf_j="60"; htpf_q="5" ;;
        web) htpf_p="10"; htpf_o="100"; htpf_j="30"; htpf_q="5" ;;
        *) return 1 ;;
      esac
      ;;
    tc)
      case "${graph_name_lower}" in
        kron) htpf_p="60"; htpf_o="800"; htpf_j="20"; htpf_q="50" ;;
        twitter) htpf_p="30"; htpf_o="800"; htpf_j="11"; htpf_q="50" ;;
        urand) htpf_p="1"; htpf_o="13"; htpf_j="3"; htpf_q="5" ;;
        road) htpf_p="60"; htpf_o="800"; htpf_j="20"; htpf_q="50" ;;
        web) htpf_p="20"; htpf_o="80"; htpf_j="7"; htpf_q="8" ;;
        *) return 1 ;;
      esac
      ;;
    *)
      return 1
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
source_root="${gap_dir}/src"

benchmark="bc"
graph=""
repeat="3"
source_node=""
single_cpu="6"
smt_cpus="6,7"
graph_macro=""
htpf_p=""
htpf_o=""
htpf_j=""
htpf_q=""
output_root="${gap_dir}/output/run1"
log_flag=0
graph_set=0
p_set=0
o_set=0
j_set=0
q_set=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --benchmark) benchmark="${2:-}"; shift 2 ;;
    --graph) graph="${2:-}"; graph_set=1; shift 2 ;;
    --repeat) repeat="${2:-}"; shift 2 ;;
    --source) source_node="${2:-}"; shift 2 ;;
    --source-root) source_root="${2:-}"; shift 2 ;;
    --single-cpu) single_cpu="${2:-}"; shift 2 ;;
    --smt-cpus) smt_cpus="${2:-}"; shift 2 ;;
    --graph-macro) graph_macro="${2:-}"; shift 2 ;;
    --p) htpf_p="${2:-}"; p_set=1; shift 2 ;;
    --o) htpf_o="${2:-}"; o_set=1; shift 2 ;;
    --j) htpf_j="${2:-}"; j_set=1; shift 2 ;;
    --q) htpf_q="${2:-}"; q_set=1; shift 2 ;;
    --output-root) output_root="${2:-}"; shift 2 ;;
    --log) log_flag=1; shift ;;
    --no-log) log_flag=0; shift ;;
    --help|-h) usage ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
done

case "${benchmark}" in
  bc|bfs|cc|pr|sssp|tc) ;;
  *)
    echo "Unsupported benchmark: ${benchmark}" >&2
    usage
    ;;
esac

if [[ "${graph_set}" == "0" ]]; then
  graph="$(default_graph_path "${benchmark}")"
fi

if [[ ! -f "${graph}" ]]; then
  echo "Graph not found: ${graph}" >&2
  exit 1
fi

graph_name="$(normalize_graph_name "${graph}")"
graph_name_lower="$(printf '%s' "${graph_name}" | tr '[:upper:]' '[:lower:]')"

if [[ "${p_set}" == "0" || "${o_set}" == "0" || "${j_set}" == "0" || "${q_set}" == "0" ]]; then
  user_htpf_p="${htpf_p}"
  user_htpf_o="${htpf_o}"
  user_htpf_j="${htpf_j}"
  user_htpf_q="${htpf_q}"
  if ! set_default_htpf_params "${benchmark}" "${graph_name_lower}"; then
    echo "No eval-aligned default HTPF parameters for benchmark=${benchmark}, graph=${graph_name}" >&2
    echo "Pass --p/--o/--j/--q explicitly." >&2
    exit 1
  fi
  if [[ "${p_set}" == "1" ]]; then htpf_p="${user_htpf_p}"; fi
  if [[ "${o_set}" == "1" ]]; then htpf_o="${user_htpf_o}"; fi
  if [[ "${j_set}" == "1" ]]; then htpf_j="${user_htpf_j}"; fi
  if [[ "${q_set}" == "1" ]]; then htpf_q="${user_htpf_q}"; fi
fi

if [[ -z "${graph_macro}" ]]; then
  graph_macro="$(infer_graph_macro "${benchmark}" "${graph}")"
fi

base_src="${source_root}/${benchmark}.cc"
htpf_src="${source_root}/${benchmark}_tpf.cc"
if [[ ! -f "${base_src}" ]]; then
  echo "Benchmark source not found: ${base_src}" >&2
  exit 1
fi
if [[ ! -f "${htpf_src}" ]]; then
  echo "HTPF source not found: ${htpf_src}" >&2
  exit 1
fi

graph_tag="$(sanitize "${graph_name}")"
timestamp="$(date +%Y%m%d-%H%M%S)"
source_tag="randsrc"
if [[ -n "${source_node}" ]]; then
  source_tag="r${source_node}"
fi
out_dir="${output_root}/${benchmark}_${graph_tag}_n${repeat}_${source_tag}_p${htpf_p}_o${htpf_o}_j${htpf_j}_q${htpf_q}_${timestamp}"
bin_dir="${out_dir}/bin"
mkdir -p "${out_dir}" "${bin_dir}"

base_bin="${bin_dir}/${benchmark}_baseline"
omp_bin="${bin_dir}/${benchmark}_homp"
htpf_bin="${bin_dir}/${benchmark}_htpf"

base_log="${out_dir}/${benchmark}_baseline.log"
omp_log="${out_dir}/${benchmark}_homp.log"
htpf_log="${out_dir}/${benchmark}_htpf.log"
summary_csv="${out_dir}/summary.csv"
sync_csv="${out_dir}/htpf_sync.csv"
commands_txt="${out_dir}/commands.txt"
source_manifest="${out_dir}/source_manifest.txt"
binary_manifest="${out_dir}/binary_manifest.txt"
build_meta="${out_dir}/build_meta.txt"

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
{
  echo "benchmark=${benchmark}"
  echo "graph=${graph}"
  echo "graph_macro=${graph_macro}"
  echo "repeat=${repeat}"
  echo "source_node=${source_node:--}"
  echo "single_cpu=${single_cpu}"
  echo "smt_cpus=${smt_cpus}"
  echo "source_root=${source_root}"
  echo "base_src=${base_src}"
  echo "htpf_src=${htpf_src}"
  echo "log_flag=${log_flag}"
  echo "htpf_args=${htpf_args[*]}"
} > "${build_meta}"

sha256sum "${base_src}" "${htpf_src}" > "${source_manifest}"

record "g++ -std=c++11 -pthread -O3 -Wall -w ${base_src} -o ${base_bin}"
g++ -std=c++11 -pthread -O3 -Wall -w "${base_src}" -o "${base_bin}"

record "g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w -DOMP -DNT=2 ${base_src} -o ${omp_bin}"
g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w -DOMP -DNT=2 "${base_src}" -o "${omp_bin}"

if [[ "${benchmark}" == "bfs" ]]; then
  record "g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w -DOMP -DBEST -DTUNING -DHTPF -D${graph_macro} ${htpf_src} -o ${htpf_bin}"
  g++ -std=c++11 -pthread -fopenmp -O3 -Wall -w -DOMP -DBEST -DTUNING -DHTPF "-D${graph_macro}" "${htpf_src}" -o "${htpf_bin}"
else
  record "g++ -std=c++11 -pthread -O3 -Wall -w -DTUNING -DHTPF -D${graph_macro} ${htpf_src} -o ${htpf_bin}"
  g++ -std=c++11 -pthread -O3 -Wall -w -DTUNING -DHTPF "-D${graph_macro}" "${htpf_src}" -o "${htpf_bin}"
fi

sha256sum "${base_bin}" "${omp_bin}" "${htpf_bin}" > "${binary_manifest}"

run_one "${single_cpu}" "${base_log}" "${base_bin}" "${common_args[@]}"
run_one "${smt_cpus}" "${omp_log}" "${omp_bin}" "${common_args[@]}"
run_one "${smt_cpus}" "${htpf_log}" "${htpf_bin}" "${common_args[@]}" "${htpf_args[@]}"

base_avg="$(extract_average "${base_log}")"

printf 'variant,average_time,speedup_vs_base,trial_times,sources,log_file\n' > "${summary_csv}"
for variant in baseline homp htpf; do
  case "${variant}" in
    baseline) logfile="${base_log}" ;;
    homp) logfile="${omp_log}" ;;
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
    print source "," too_fast "," too_slow "," serial_end "," $3
    source = "-"
    too_fast = "-"
    too_slow = "-"
    serial_end = "-"
  }
' "${htpf_log}" >> "${sync_csv}"

echo "out_dir=${out_dir}"
echo "summary=${summary_csv}"
echo "htpf_sync=${sync_csv}"
