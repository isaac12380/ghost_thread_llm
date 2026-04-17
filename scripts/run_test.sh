#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF' >&2
Usage: run_test.sh [options]

Build or run the compiler-driven BFS demo with GhostThreading clang++.

  --mode <run|compile-only|emit-ir-only>   Default: run
  --graph <name>                           Default: web-Stanford
  --repeat <n>                             Default: 1
  --cpu-list <list>                        Optional taskset list, e.g. 0,48
  --output-tag <tag>                       Default: timestamp
  --compiler <path>                        Override GhostThreading clang++
  --help                                   Show this message

Advanced overrides:
  GT_EVAL_ROOT, GT_GAP_DIR, GT_SOURCE_FILE,
  GT_CLANG, GT_OMP_INCLUDE_DIR, GT_OMP_LIB_DIR,
  GT_THPOOL_C, GT_THPOOL_INCLUDE_DIR, GT_DEBUG=1
EOF
  exit 1
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_eval_root="/home/dean/workspace/ghost-threading-eval-micro2025"
eval_root="${GT_EVAL_ROOT:-${default_eval_root}}"
gap_dir="${GT_GAP_DIR:-${eval_root}/gap}"
default_compiler="/home/dean/workspace/GhostThreadingCompiler/build-nix/bin/clang++"
default_omp_include="/home/dean/workspace/GhostThreadingCompiler/build-nix/projects/openmp/runtime/src"
default_source="${gap_dir}/src/bfs_compiler_demo.cc"
default_thpool_c="${eval_root}/hpc/thpool/thpool.c"
default_thpool_include="${eval_root}/hpc/thpool"

mode="run"
graph="web-Stanford"
repeat="1"
binary_name="bfs-compiler-gt"
compiler="${GT_CLANG:-${default_compiler}}"
output_tag="$(date +%Y%m%d-%H%M%S)"
cpu_list=""
fixed_source=""
source_file="${GT_SOURCE_FILE:-${default_source}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      mode="${2:-}"
      shift 2
      ;;
    --graph)
      graph="${2:-}"
      shift 2
      ;;
    --repeat)
      repeat="${2:-}"
      shift 2
      ;;
    --compiler)
      compiler="${2:-}"
      shift 2
      ;;
    --output-tag)
      output_tag="${2:-}"
      shift 2
      ;;
    --cpu-list)
      cpu_list="${2:-}"
      shift 2
      ;;
    --source)
      fixed_source="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

case "${mode}" in
  run|compile-only|emit-ir-only) ;;
  *)
    echo "Unsupported mode: ${mode}" >&2
    usage
    ;;
esac

case "${graph}" in
  roadNet-PA)
    graph_macro="ROAD"
    ;;
  web-Stanford|web-Google|amazon0312)
    graph_macro="WEB"
    ;;
  *)
    echo "Unsupported graph: ${graph}" >&2
    usage
    ;;
esac

graph_path="${gap_dir}/benchmark/graphs/small/${graph}.sg"
if [[ ! -f "${graph_path}" ]]; then
  echo "Small graph not found: ${graph_path}" >&2
  exit 1
fi

if [[ ! -f "${source_file}" ]]; then
  echo "Compiler demo source not found: ${source_file}" >&2
  exit 1
fi

if [[ ! -x "${compiler}" ]]; then
  echo "GhostThreading clang++ not found: ${compiler}" >&2
  exit 1
fi

omp_include_dir="${GT_OMP_INCLUDE_DIR:-${default_omp_include}}"
if [[ ! -f "${omp_include_dir}/omp.h" ]]; then
  omp_include_dir="$(find /nix/store -name omp.h 2>/dev/null | rg 'openmp' | head -n 1 | xargs -r dirname)"
fi

omp_lib_dir="${GT_OMP_LIB_DIR:-}"
if [[ -z "${omp_lib_dir}" ]]; then
  omp_lib_dir="$(find /nix/store -name libomp.so 2>/dev/null | head -n 1 | xargs -r dirname)"
fi

thpool_c="${GT_THPOOL_C:-${default_thpool_c}}"
thpool_include_dir="${GT_THPOOL_INCLUDE_DIR:-${default_thpool_include}}"

compiler_build="$(cd "$(dirname "${compiler}")/.." && pwd)"
compiler_cache="${compiler_build}/CMakeCache.txt"
gcc_wrapper_nix=""
gcc_toolchain=""
glibc_nix=""
gcc_lib_dir=""
toolchain_flags=()

if [[ -f "${compiler_cache}" ]]; then
  linker_path="$(sed -n 's#^CMAKE_LINKER:FILEPATH=##p' "${compiler_cache}" | head -n 1)"
  if [[ -n "${linker_path}" ]]; then
    gcc_wrapper_nix="$(cd "$(dirname "${linker_path}")/.." && pwd)"
  fi
fi

if [[ -n "${gcc_wrapper_nix}" && -f "${gcc_wrapper_nix}/nix-support/orig-cc" ]]; then
  gcc_toolchain="$(head -n 1 "${gcc_wrapper_nix}/nix-support/orig-cc")"
fi

if [[ -n "${gcc_wrapper_nix}" && -f "${gcc_wrapper_nix}/nix-support/orig-libc" ]]; then
  glibc_nix="$(head -n 1 "${gcc_wrapper_nix}/nix-support/orig-libc")"
fi

if [[ -n "${gcc_wrapper_nix}" && -f "${gcc_wrapper_nix}/nix-support/cc-ldflags" ]]; then
  gcc_lib_dir="$(tr ' ' '\n' < "${gcc_wrapper_nix}/nix-support/cc-ldflags" | sed -n 's#^-L##p' | head -n 1)"
fi

if [[ -n "${gcc_toolchain}" ]]; then
  toolchain_flags+=("--gcc-toolchain=${gcc_toolchain}")
fi

if [[ -n "${gcc_wrapper_nix}" ]]; then
  toolchain_flags+=("-B${gcc_wrapper_nix}/bin")
fi

if [[ -n "${glibc_nix}" ]]; then
  toolchain_flags+=(
    "-B${glibc_nix}/lib"
    "-L${glibc_nix}/lib"
    "-Wl,-rpath,${glibc_nix}/lib"
    "-Wl,-dynamic-linker,${glibc_nix}/lib/ld-linux-x86-64.so.2"
  )
fi

if [[ -n "${gcc_lib_dir}" ]]; then
  toolchain_flags+=(
    "-L${gcc_lib_dir}"
    "-Wl,-rpath,${gcc_lib_dir}"
  )
fi

if [[ ! -f "${omp_include_dir}/omp.h" ]]; then
  echo "Could not find omp.h. Set GT_OMP_INCLUDE_DIR explicitly." >&2
  exit 1
fi

if [[ "${mode}" != "emit-ir-only" ]] && [[ -z "${omp_lib_dir}" || ! -f "${omp_lib_dir}/libomp.so" ]]; then
  echo "Could not find libomp.so. Set GT_OMP_LIB_DIR explicitly." >&2
  exit 1
fi

if [[ "${mode}" != "emit-ir-only" ]] && [[ ! -f "${thpool_c}" ]]; then
  echo "Could not find thpool.c: ${thpool_c}" >&2
  exit 1
fi

if [[ "${mode}" != "emit-ir-only" ]] && [[ ! -f "${thpool_include_dir}/thpool.h" ]]; then
  echo "Could not find thpool headers under: ${thpool_include_dir}" >&2
  exit 1
fi

out_root="${gap_dir}/output/small/compiler_gt"
out_dir="${out_root}/runs/${output_tag}"
mkdir -p "${out_dir}"
ln -sfn "runs/${output_tag}" "${out_root}/latest"

binary_path="${gap_dir}/${binary_name}"
metadata_ir="${out_dir}/bfs_${graph}_metadata.ll"
gt_ir="${out_dir}/bfs_${graph}_gt.ll"
build_log="${out_dir}/build.log"
run_log="${out_dir}/run.log"

compile_flags=(
  -O3 -w -Wall -std=c++11
  -fopenmp
  -mllvm -ghostthreading=true
  -DGHOSTPF
  "-D${graph_macro}"
  "-I${omp_include_dir}"
)

link_flags=(
  "-I${thpool_include_dir}"
  "-L${omp_lib_dir}"
  "-Wl,-rpath,${omp_lib_dir}"
)

run_cmd=("${binary_path}" -f "${graph_path}" -n "${repeat}")
if [[ -n "${fixed_source}" ]]; then
  run_cmd+=(-r "${fixed_source}")
fi

echo "Compiler GT GAP: ${mode} ${graph}"
echo "  compiler: ${compiler}"
echo "  source:   ${source_file}"
echo "  output:   ${out_dir}"

if [[ "${GT_DEBUG:-0}" != "0" ]]; then
  cat <<EOF
  gap dir:    ${gap_dir}
  omp include:${omp_include_dir}
  omp lib:    ${omp_lib_dir:-<not-needed>}
  gcc wrap:   ${gcc_wrapper_nix:-<not-detected>}
  gcc tc:     ${gcc_toolchain:-<not-detected>}
  glibc:      ${glibc_nix:-<not-detected>}
  gcc lib:    ${gcc_lib_dir:-<not-detected>}
EOF
fi

echo

if [[ "${mode}" == "emit-ir-only" ]]; then
  {
    echo "[1/2] Emitting metadata-only IR"
    (
      cd "${gap_dir}"
      "${compiler}" \
        "${toolchain_flags[@]}" \
        -O0 -w -Wall -std=c++11 \
        -fopenmp \
        "-I${omp_include_dir}" \
        -DGHOSTPF "-D${graph_macro}" \
        -Xclang -disable-llvm-passes \
        -S -emit-llvm \
        "${source_file}" \
        -o "${metadata_ir}"
    )

    echo "[2/2] Emitting pass-transformed IR"
    (
      cd "${gap_dir}"
      "${compiler}" \
        "${toolchain_flags[@]}" \
        "${compile_flags[@]}" \
        -S -emit-llvm \
        "${source_file}" \
        -o "${gt_ir}"
    )
  } 2>&1 | tee "${build_log}"

  cat <<EOF

Finished IR emission.
  metadata IR: ${metadata_ir}
  gt IR:       ${gt_ir}
  build log:   ${build_log}
EOF
  exit 0
fi

{
  echo "[1/2] Building ${binary_name}"
  (
    cd "${gap_dir}"
    "${compiler}" \
      "${toolchain_flags[@]}" \
      "${compile_flags[@]}" \
      "${source_file}" \
      "${thpool_c}" \
      "${link_flags[@]}" \
      -o "${binary_path}"
  )

  echo "[2/2] Build complete"
} 2>&1 | tee "${build_log}"

if [[ "${mode}" == "compile-only" ]]; then
  cat <<EOF

Finished compile-only mode.
  binary:    ${binary_path}
  build log: ${build_log}
EOF
  exit 0
fi

echo "[run] ${run_cmd[*]}" | tee "${run_log}"
if [[ -n "${cpu_list}" ]]; then
  taskset -c "${cpu_list}" "${run_cmd[@]}" 2>&1 | tee -a "${run_log}"
else
  "${run_cmd[@]}" 2>&1 | tee -a "${run_log}"
fi

cat <<EOF

Finished run mode.
  binary:    ${binary_path}
  build log: ${build_log}
  run log:   ${run_log}
EOF
