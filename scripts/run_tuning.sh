#!/usr/bin/env bash
set -euo pipefail

SUDO_PASSWORD="${GT_SUDO_PASSWORD:-Yda362300}"
SUDO_TICKET_READY=0

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
GAP_DIR="${REPO_ROOT}/gap"

WORKLOAD="${GT_WORKLOAD:-sssp}"
GRAPH=""
SINGLE_BIN=""
OMP_BIN=""
HTPF_BIN=""

REPEAT="${GT_REPEAT:-5}"
SOURCE="${GT_SOURCE:-2818235}"
SSSP_DELTA="${GT_SSSP_DELTA:-1}"
BC_ITERS="${GT_BC_ITERS:-1}"

SINGLE_CPU="${GT_SINGLE_CPU:-6}"
SMT_CPUS="${GT_SMT_CPUS:-6,7}"
BACKGROUND_CPUS="${GT_BACKGROUND_CPUS:-0-5,8-19}"
AUTO_EVICT="${GT_AUTO_EVICT:-0}"

HTPF_P="${GT_HTPF_P:-300}"
HTPF_O="${GT_HTPF_O:-64}"
HTPF_J="${GT_HTPF_J:-8}"
HTPF_Q="${GT_HTPF_Q:-4}"

PERF_EVENTS="${GT_PERF_EVENTS:-cache-references,cache-misses}"

usage() {
  cat <<'EOF'
Usage: run_tuning.sh [options]

Options:
  -w <workload> Workload: bfs | sssp | bc | bc_web | cc | pr | pr_web
  -f <graph>    Graph path
  -n <repeat>   Repeat count
  -r <source>   Fixed source node
  -d <delta>    SSSP delta parameter
  -i <iters>    BC iteration count
  -p <value>    HTPF sync_frequency
  -o <value>    HTPF serialize_threshold
  -j <value>    HTPF skip_offset
  -q <value>    HTPF unserialize gap
  -P <prefix>   Output prefix override
  -h            Show help
EOF
}

sanitize_component() {
  printf '%s' "$1" | tr '/[:space:]' '__' | tr -cs '[:alnum:]_.-' '_'
}

CLI_OUT_PREFIX=""
while getopts ":w:f:n:r:d:i:p:o:j:q:P:h" opt; do
  case "${opt}" in
    w) WORKLOAD="${OPTARG}" ;;
    f) GRAPH="${OPTARG}" ;;
    n) REPEAT="${OPTARG}" ;;
    r) SOURCE="${OPTARG}" ;;
    d) SSSP_DELTA="${OPTARG}" ;;
    i) BC_ITERS="${OPTARG}" ;;
    p) HTPF_P="${OPTARG}" ;;
    o) HTPF_O="${OPTARG}" ;;
    j) HTPF_J="${OPTARG}" ;;
    q) HTPF_Q="${OPTARG}" ;;
    P) CLI_OUT_PREFIX="${OPTARG}" ;;
    h)
      usage
      exit 0
      ;;
    :)
      echo "Option -${OPTARG} requires an argument." >&2
      usage >&2
      exit 1
      ;;
    \?)
      echo "Unknown option: -${OPTARG}" >&2
      usage >&2
      exit 1
      ;;
  esac
done
shift $((OPTIND - 1))

if [[ "$#" -gt 0 ]]; then
  echo "Unexpected arguments: $*" >&2
  usage >&2
  exit 1
fi

case "${WORKLOAD}" in
  bfs)
    DEFAULT_GRAPH="${GT_GRAPH:-/tmp/com-Orkut.sg}"
    DEFAULT_SINGLE_BIN="${GT_SINGLE_BIN:-${GAP_DIR}/bfs_tpf_plain}"
    DEFAULT_OMP_BIN="${GT_OMP_BIN:-${GAP_DIR}/bfs_tpf_plain}"
    DEFAULT_HTPF_BIN="${GT_HTPF_BIN:-${GAP_DIR}/bfs_htpf}"
    ;;
  sssp)
    DEFAULT_GRAPH="${GT_GRAPH:-/tmp/com-Orkut.wsg}"
    DEFAULT_SINGLE_BIN="${GT_SINGLE_BIN:-${GAP_DIR}/sssp_tpf_plain}"
    DEFAULT_OMP_BIN="${GT_OMP_BIN:-${GAP_DIR}/sssp_tpf_plain}"
    DEFAULT_HTPF_BIN="${GT_HTPF_BIN:-${GAP_DIR}/sssp_htpf}"
    ;;
  bc)
    DEFAULT_GRAPH="${GT_GRAPH:-/tmp/com-Orkut.sg}"
    DEFAULT_SINGLE_BIN="${GT_SINGLE_BIN:-${GAP_DIR}/bc_tpf_plain}"
    DEFAULT_OMP_BIN="${GT_OMP_BIN:-${GAP_DIR}/bc_tpf_plain}"
    DEFAULT_HTPF_BIN="${GT_HTPF_BIN:-${GAP_DIR}/bc_htpf}"
    ;;
  bc_web)
    DEFAULT_GRAPH="${GT_GRAPH:-/tmp/com-Orkut.sg}"
    DEFAULT_SINGLE_BIN="${GT_SINGLE_BIN:-${GAP_DIR}/bc_tpf_plain_web}"
    DEFAULT_OMP_BIN="${GT_OMP_BIN:-${GAP_DIR}/bc_tpf_plain_web}"
    DEFAULT_HTPF_BIN="${GT_HTPF_BIN:-${GAP_DIR}/bc_htpf_web}"
    ;;
  cc)
    DEFAULT_GRAPH="${GT_GRAPH:-/tmp/com-Orkut.sg}"
    DEFAULT_SINGLE_BIN="${GT_SINGLE_BIN:-${GAP_DIR}/cc_tpf_plain}"
    DEFAULT_OMP_BIN="${GT_OMP_BIN:-${GAP_DIR}/cc_tpf_plain}"
    DEFAULT_HTPF_BIN="${GT_HTPF_BIN:-${GAP_DIR}/cc_htpf}"
    ;;
  pr)
    DEFAULT_GRAPH="${GT_GRAPH:-/tmp/com-Orkut.sg}"
    DEFAULT_SINGLE_BIN="${GT_SINGLE_BIN:-${GAP_DIR}/pr_tpf_plain}"
    DEFAULT_OMP_BIN="${GT_OMP_BIN:-${GAP_DIR}/pr_tpf_plain}"
    DEFAULT_HTPF_BIN="${GT_HTPF_BIN:-${GAP_DIR}/pr_htpf}"
    ;;
  pr_web)
    DEFAULT_GRAPH="${GT_GRAPH:-/tmp/com-Orkut.sg}"
    DEFAULT_SINGLE_BIN="${GT_SINGLE_BIN:-${GAP_DIR}/pr_tpf_plain_web}"
    DEFAULT_OMP_BIN="${GT_OMP_BIN:-${GAP_DIR}/pr_tpf_plain_web}"
    DEFAULT_HTPF_BIN="${GT_HTPF_BIN:-${GAP_DIR}/pr_htpf_web}"
    ;;
  *)
    echo "Unsupported workload: ${WORKLOAD}. Use bfs, sssp, bc, bc_web, cc, pr, or pr_web." >&2
    exit 1
    ;;
esac

if [[ -z "${GRAPH}" ]]; then
  GRAPH="${DEFAULT_GRAPH}"
fi
SINGLE_BIN="${DEFAULT_SINGLE_BIN}"
OMP_BIN="${DEFAULT_OMP_BIN}"
HTPF_BIN="${DEFAULT_HTPF_BIN}"

GRAPH_BASENAME="$(basename -- "${GRAPH}")"
GRAPH_TAG_RAW="${GRAPH_BASENAME%.*}"
GRAPH_TAG="$(sanitize_component "${GRAPH_TAG_RAW}")"
WORKLOAD_TAG="$(sanitize_component "${WORKLOAD}")"
DEFAULT_OUT_PREFIX="${GRAPH_TAG}_${WORKLOAD_TAG}_n${REPEAT}_r${SOURCE}"
if [[ "${WORKLOAD}" == "sssp" ]]; then
  DEFAULT_OUT_PREFIX="${DEFAULT_OUT_PREFIX}_d${SSSP_DELTA}"
fi
if [[ "${WORKLOAD}" == "bc" || "${WORKLOAD}" == "bc_web" ]]; then
  DEFAULT_OUT_PREFIX="${DEFAULT_OUT_PREFIX}_i${BC_ITERS}"
fi
DEFAULT_OUT_PREFIX="${DEFAULT_OUT_PREFIX}_p${HTPF_P}_o${HTPF_O}_j${HTPF_J}_q${HTPF_Q}"
OUT_PREFIX="${CLI_OUT_PREFIX:-${GT_OUT_PREFIX:-${DEFAULT_OUT_PREFIX}}}"
OUT_TIMESTAMP="${GT_OUT_TIMESTAMP:-$(date +%Y%m%d-%H%M%S)}"
OUT_TAG="${GT_OUT_TAG:-${OUT_PREFIX}-${OUT_TIMESTAMP}}"
OUT_DIR="${GT_OUT_DIR:-${GAP_DIR}/output/tuning/${OUT_TAG}}"

mkdir -p "${OUT_DIR}"

SUMMARY_CSV="${OUT_DIR}/summary.csv"
COMMANDS_TXT="${OUT_DIR}/commands.txt"

declare -A PERF_CSV_BY_VARIANT=()
declare -A SYNC_CSV_BY_VARIANT=()
declare -A CACHE_REFS_BY_VARIANT=()
declare -A CACHE_MISSES_BY_VARIANT=()
declare -A CACHE_MISS_RATE_BY_VARIANT=()
declare -A AVG_TIME_BY_VARIANT=()
declare -A MEDIAN_TIME_BY_VARIANT=()
declare -A TRIMMED_GEOMEAN_TIME_BY_VARIANT=()

ensure_sudo_ticket() {
  if [[ "${SUDO_TICKET_READY}" == "1" ]]; then
    return
  fi
  printf '%s\n' "${SUDO_PASSWORD}" | sudo -S -p '' -v
  SUDO_TICKET_READY=1
}

sudo_run() {
  ensure_sudo_ticket
  sudo -n "$@"
}

need_path() {
  local path="$1"
  if [[ ! -e "${path}" ]]; then
    echo "Missing required path: ${path}" >&2
    exit 1
  fi
}

pin_runner_off_target() {
  taskset -pc "${BACKGROUND_CPUS}" $$ >/dev/null || true
  if [[ "${PPID:-0}" =~ ^[0-9]+$ ]] && [[ "${PPID}" -gt 1 ]]; then
    taskset -pc "${BACKGROUND_CPUS}" "${PPID}" >/dev/null || true
  fi
}

capture_target_threads() {
  ps -eLo user,pid,tid,psr,pcpu,comm,args --sort=psr --no-headers \
    | awk -v targets="${SMT_CPUS}" -v current_user="${USER}" '
        BEGIN {
          n = split(targets, cpus, ",");
          for (i = 1; i <= n; i++) wanted[cpus[i]] = 1;
        }
        ($4 in wanted) && ($1 == current_user) && ($7 !~ /^\[/) { print }
      '
}

capture_target_processes() {
  capture_target_threads | awk '!seen[$2]++ { print }'
}

pid_affinity_list() {
  local pid="$1"
  taskset -pc "${pid}" 2>/dev/null | awk -F': ' 'END { gsub(/[[:space:]]/, "", $2); print $2 }'
}

cpu_list_hits_targets() {
  local cpu_list="$1"
  awk -v cpu_list="${cpu_list}" -v targets="${SMT_CPUS}" '
    BEGIN {
      nt = split(targets, t, ",");
      for (i = 1; i <= nt; i++) wanted[t[i]] = 1;

      np = split(cpu_list, parts, ",");
      for (i = 1; i <= np; i++) {
        if (parts[i] ~ /-/) {
          split(parts[i], bounds, "-");
          for (cpu = bounds[1]; cpu <= bounds[2]; cpu++) {
            if (cpu in wanted) {
              print 1;
              exit;
            }
          }
        } else if (parts[i] in wanted) {
          print 1;
          exit;
        }
      }
      print 0;
    }
  '
}

capture_blocking_processes() {
  local line pid affinity
  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    pid="$(awk '{print $2}' <<< "${line}")"
    affinity="$(pid_affinity_list "${pid}")"
    if [[ -n "${affinity}" ]] && [[ "$(cpu_list_hits_targets "${affinity}")" == "1" ]]; then
      printf '%s | affinity=%s\n' "${line}" "${affinity}"
    fi
  done < <(capture_target_processes)
}

capture_blocking_pids() {
  capture_blocking_processes | awk '{print $2}' | sort -u
}

maybe_evict_target_threads() {
  if [[ "${AUTO_EVICT}" != "1" ]]; then
    return
  fi

  local attempt pid
  local pids=()
  for attempt in 1 2 3; do
    mapfile -t pids < <(capture_blocking_pids)
    if [[ "${#pids[@]}" -eq 0 ]]; then
      return
    fi

    for pid in "${pids[@]}"; do
      taskset -apc "${BACKGROUND_CPUS}" "${pid}" >/dev/null 2>&1 \
        || sudo_run taskset -apc "${BACKGROUND_CPUS}" "${pid}" >/dev/null 2>&1 \
        || true
    done

    sleep 1
  done
}

assert_target_clear() {
  pin_runner_off_target
  maybe_evict_target_threads

  local listing
  listing="$(capture_blocking_processes || true)"
  if [[ -n "${listing}" ]]; then
    echo "Target CPUs ${SMT_CPUS} still have user-space threads pinned on them." >&2
    echo "${listing}" >&2
    echo "System/root/kernel threads are ignored. Clear these user threads manually, or rerun with GT_AUTO_EVICT=1." >&2
    exit 1
  fi
}

perf_counter_value() {
  local perf_csv="$1"
  local event_name="$2"
  awk -F, -v event_name="${event_name}" '
    index($3, event_name) > 0 {
      gsub(/[[:space:]]/, "", $1);
      if ($1 == "" || $1 ~ /^</)
        next;
      sum += $1;
      found = 1;
    }
    END {
      if (found)
        printf "%.0f\n", sum;
    }
  ' "${perf_csv}"
}

parse_average_time() {
  local stdout_log="$1"
  awk '
    /^Average Time:/ {
      print $3;
      found = 1;
      exit;
    }
    END {
      if (!found) exit 1;
    }
  ' "${stdout_log}"
}

parse_median_time() {
  local stdout_log="$1"
  awk '
    /^Trial Time:/ { print $3; count++ }
    END {
      if (count == 0) exit 1;
    }
  ' "${stdout_log}" \
    | sort -n \
    | awk '
        {
          values[NR] = $1;
        }
        END {
          if (NR == 0) exit 1;
          if (NR % 2 == 1) {
            printf "%.5f\n", values[(NR + 1) / 2];
          } else {
            printf "%.5f\n", (values[NR / 2] + values[NR / 2 + 1]) / 2.0;
          }
        }
      '
}

parse_trimmed_geomean_time() {
  local stdout_log="$1"
  awk '
    /^Trial Time:/ { print $3 }
  ' "${stdout_log}" \
    | sort -n \
    | awk '
        {
          values[++n] = $1;
        }
        END {
          if (n == 0) exit 1;

          start = 1;
          end = n;
          if (n >= 3) {
            start = 2;
            end = n - 1;
          }

          count = 0;
          sum_log = 0;
          for (i = start; i <= end; i++) {
            if (values[i] <= 0) exit 1;
            sum_log += log(values[i]);
            count++;
          }

          if (count == 0) exit 1;
          printf "%.5f\n", exp(sum_log / count);
        }
      '
}

collect_case_stats() {
  local variant="$1"
  local stdout_log="$2"
  local perf_csv="$3"
  local sync_csv="$4"

  local refs misses miss_rate avg_time median_time trimmed_geomean_time
  refs="$(perf_counter_value "${perf_csv}" "cache-references")"
  misses="$(perf_counter_value "${perf_csv}" "cache-misses")"
  miss_rate="NA"

  if [[ -n "${refs}" && -n "${misses}" && "${refs}" != "<notcounted>" && "${refs}" != "0" ]]; then
    miss_rate="$(
      awk -v misses="${misses}" -v refs="${refs}" '
        BEGIN {
          if (refs == 0) {
            print "NA";
          } else {
            printf "%.6f", misses / refs;
          }
        }
      '
    )"
  fi

  avg_time="$(parse_average_time "${stdout_log}")"
  median_time="$(parse_median_time "${stdout_log}")"
  trimmed_geomean_time="$(parse_trimmed_geomean_time "${stdout_log}")"

  PERF_CSV_BY_VARIANT["${variant}"]="${perf_csv}"
  SYNC_CSV_BY_VARIANT["${variant}"]="${sync_csv}"
  CACHE_REFS_BY_VARIANT["${variant}"]="${refs:-NA}"
  CACHE_MISSES_BY_VARIANT["${variant}"]="${misses:-NA}"
  CACHE_MISS_RATE_BY_VARIANT["${variant}"]="${miss_rate}"
  AVG_TIME_BY_VARIANT["${variant}"]="${avg_time}"
  MEDIAN_TIME_BY_VARIANT["${variant}"]="${median_time}"
  TRIMMED_GEOMEAN_TIME_BY_VARIANT["${variant}"]="${trimmed_geomean_time}"

  if [[ -n "${sync_csv}" ]]; then
    echo "[${variant}] sync trace: ${sync_csv}"
  fi
  echo "[${variant}] avg-time=${avg_time}s median-time=${median_time}s trimmed-geomean=${trimmed_geomean_time}s cache-references=${refs:-NA} cache-misses=${misses:-NA} miss-rate=${miss_rate}"
}

ratio_or_na() {
  local base="$1"
  local current="$2"

  if [[ -z "${base}" || -z "${current}" ]]; then
    echo "NA"
    return
  fi

  awk -v base="${base}" -v current="${current}" '
    BEGIN {
      if (current == 0) {
        print "NA";
      } else {
        printf "%.6f", base / current;
      }
    }
  '
}

write_summary() {
  printf 'variant,perf_csv,sync_csv,cache_references,cache_misses,cache_miss_rate,avg_time_s,median_time_s,trimmed_geomean_time_s,speedup_vs_single_avg,speedup_vs_single_geomean,speedup_vs_single_median\n' > "${SUMMARY_CSV}"

  local variant speedup_avg speedup_geomean speedup_median
  for variant in single omp htpf; do
    speedup_avg="$(
      ratio_or_na "${AVG_TIME_BY_VARIANT[single]:-}" "${AVG_TIME_BY_VARIANT[$variant]:-}"
    )"
    speedup_geomean="$(
      ratio_or_na "${TRIMMED_GEOMEAN_TIME_BY_VARIANT[single]:-}" "${TRIMMED_GEOMEAN_TIME_BY_VARIANT[$variant]:-}"
    )"
    speedup_median="$(
      ratio_or_na "${MEDIAN_TIME_BY_VARIANT[single]:-}" "${MEDIAN_TIME_BY_VARIANT[$variant]:-}"
    )"

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "${variant}" \
      "${PERF_CSV_BY_VARIANT[$variant]:-NA}" \
      "${SYNC_CSV_BY_VARIANT[$variant]:-}" \
      "${CACHE_REFS_BY_VARIANT[$variant]:-NA}" \
      "${CACHE_MISSES_BY_VARIANT[$variant]:-NA}" \
      "${CACHE_MISS_RATE_BY_VARIANT[$variant]:-NA}" \
      "${AVG_TIME_BY_VARIANT[$variant]:-NA}" \
      "${MEDIAN_TIME_BY_VARIANT[$variant]:-NA}" \
      "${TRIMMED_GEOMEAN_TIME_BY_VARIANT[$variant]:-NA}" \
      "${speedup_avg}" \
      "${speedup_geomean}" \
      "${speedup_median}" \
      >> "${SUMMARY_CSV}"
  done
}

print_command() {
  local label="$1"
  shift
  printf '[%s] ' "${label}" | tee -a "${COMMANDS_TXT}"
  printf '%q ' "$@" | tee -a "${COMMANDS_TXT}"
  printf '\n' | tee -a "${COMMANDS_TXT}"
}

run_case() {
  local variant="$1"
  local cpu_list="$2"
  local omp_threads="$3"
  local binary="$4"
  shift 4
  local -a app_args=("$@")

  assert_target_clear

  local stdout_log="${OUT_DIR}/${variant}.log"
  local perf_csv="${OUT_DIR}/${variant}.perf.csv"
  local cmd_txt="${OUT_DIR}/${variant}.cmd"
  local sync_csv=""
  local -a workload_args=()

  local -a env_args=(
    "OMP_NUM_THREADS=${omp_threads}"
    "OMP_PROC_BIND=true"
  )

  if [[ "${variant}" == "htpf" ]]; then
    sync_csv="${OUT_DIR}/htpf_sync_branch.csv"
    env_args+=("HTPF_SYNC_BRANCH_TRACE=${sync_csv}")
  fi

  workload_args=(
    -f "${GRAPH}"
    -n "${REPEAT}"
    -r "${SOURCE}"
    -l
  )

  if [[ "${WORKLOAD}" == "sssp" ]]; then
    workload_args+=(-d "${SSSP_DELTA}")
  fi
  if [[ "${WORKLOAD}" == "bc" || "${WORKLOAD}" == "bc_web" ]]; then
    workload_args+=(-i "${BC_ITERS}")
  fi

  local -a cmd=(
    env
    "${env_args[@]}"
    perf stat
    -x,
    -e "${PERF_EVENTS}"
    -o "${perf_csv}"
    taskset -c "${cpu_list}"
    "${binary}"
    "${workload_args[@]}"
    "${app_args[@]}"
  )

  print_command "${variant}" sudo "${cmd[@]}"
  printf '%q ' "${cmd[@]}" > "${cmd_txt}"
  printf '\n' >> "${cmd_txt}"

  sudo_run "${cmd[@]}" |& tee "${stdout_log}"
  collect_case_stats "${variant}" "${stdout_log}" "${perf_csv}" "${sync_csv}"
}

need_path "${GRAPH}"
need_path "${SINGLE_BIN}"
need_path "${OMP_BIN}"
need_path "${HTPF_BIN}"

if ! command -v perf >/dev/null 2>&1; then
  echo "perf is not installed." >&2
  exit 1
fi

if ! command -v taskset >/dev/null 2>&1; then
  echo "taskset is not installed." >&2
  exit 1
fi

ensure_sudo_ticket

: > "${COMMANDS_TXT}"

echo "Output directory: ${OUT_DIR}"
echo "Output prefix: ${OUT_PREFIX}"
echo "Output timestamp: ${OUT_TIMESTAMP}"
echo "Workload: ${WORKLOAD}"
echo "Graph: ${GRAPH}"
echo "Fixed source: ${SOURCE}"
echo "Repeat count: ${REPEAT}"
if [[ "${WORKLOAD}" == "sssp" ]]; then
  echo "SSSP delta: ${SSSP_DELTA}"
fi
if [[ "${WORKLOAD}" == "bc" || "${WORKLOAD}" == "bc_web" ]]; then
  echo "BC iterations: ${BC_ITERS}"
fi
echo "Single-thread CPU: ${SINGLE_CPU}"
echo "SMT CPUs: ${SMT_CPUS}"
echo "HTPF params: -p ${HTPF_P} -o ${HTPF_O} -j ${HTPF_J} -q ${HTPF_Q}"
echo

run_case "single" "${SINGLE_CPU}" "1" "${SINGLE_BIN}"
run_case "omp" "${SMT_CPUS}" "2" "${OMP_BIN}"
run_case "htpf" "${SMT_CPUS}" "2" "${HTPF_BIN}" \
  -p "${HTPF_P}" \
  -o "${HTPF_O}" \
  -j "${HTPF_J}" \
  -q "${HTPF_Q}"

write_summary

echo
echo "Summary written to ${SUMMARY_CSV}"
echo "Commands written to ${COMMANDS_TXT}"
