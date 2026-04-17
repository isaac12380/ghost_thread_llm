Ghost Threading minimal local reproduction tree.

This repository keeps only:
- GAP source code
- ghost-threaded GAP variants
- one runnable script for the retained SNAP graph workflow

## Host prerequisites

- Linux x86-64
- Intel CPU
- SMT / Hyper-Threading enabled
- CPU supports `serialize`

## Run

```sh
./scripts/run_snap_gap_experiments.sh <smt_core0> <smt_core1> [repeat]
```

Example:

```sh
./scripts/run_snap_gap_experiments.sh 0 48 1
```

The script:
1. downloads four SNAP graphs
2. builds `gap/converter`
3. converts the downloaded `.mtx` files into GAP graph inputs
4. runs `bfs`, `bc`, `cc`, `pr`, `sssp`, and `tc`
5. runs four variants for each workload: `baseline`, `homp`, `swpf`, `htpf`
6. writes raw logs under `gap/output/snap/runs/<timestamp>/`
