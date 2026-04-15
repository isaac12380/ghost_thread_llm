Ghost Threading minimal local reproduction tree.

This repository keeps only:
- GAP source code
- ghost-threaded GAP variants
- the Nix toolchain shell
- one runnable script for the retained SNAP graph workflow

Requirements:
- x86-64 Linux on Intel with SMT and `serialize` support
- `gcc/g++`, `make`, `python3`, `curl`, `tar`
- optional: Nix, if the host toolchain is too old to assemble `serialize`

If the host toolchain is too old, use the Nix shell:
```sh
NIX_CONFIG="experimental-features = nix-command flakes" nix develop path:.
```

The only retained run entrypoint is:
```sh
./scripts/run_snap_gap_experiments.sh <smt_core0> <smt_core1> [repeat]
```

Example:
```sh
NIX_CONFIG="experimental-features = nix-command flakes" nix develop path:. -c \
  ./scripts/run_snap_gap_experiments.sh 0 48 3
```

What this script does:
1. downloads four SNAP graphs: `web-Stanford`, `web-Google`, `amazon0312`, `roadNet-PA`
2. builds `gap/converter`
3. converts the downloaded `.mtx` files into GAP graph inputs under `gap/benchmark/graphs/snap/`
4. runs `bfs`, `bc`, `cc`, `pr`, `sssp`, and `tc`
5. runs four variants for each workload: `baseline`, `homp`, `swpf`, `htpf`
6. writes raw logs under `gap/output/snap/runs/<timestamp>/`

Optional environment variables:
- `SNAP_GAP_FIXED_SOURCE=1`
- `SNAP_GAP_WARMUP_RUNS=1`
- `SNAP_GAP_RUN_NAME=<timestamp-or-tag>`

This minimal tree intentionally does not track plotting, tuning, figure
reproduction, or external tool directories.
