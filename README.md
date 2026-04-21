Ghost Threading minimal local reproduction tree.

This repository keeps only:
- GAP source code
- ghost-threaded GAP variants
- the current runnable workflow for the latest four large SNAP graphs
- a legacy retained SNAP workflow for older comparisons

## Host prerequisites

- Linux x86-64
- Intel CPU
- SMT / Hyper-Threading enabled
- CPU supports `serialize`

## Run

Primary workflow:

```sh
./scripts/run_snap_gap_latest4.sh <smt_core0> <smt_core1> [repeat]
```

Example:

```sh
./scripts/run_snap_gap_latest4.sh 0 48 1
```

The script:
1. downloads the current four large graphs:
   `com-Orkut`, `com-LiveJournal`, `soc-Pokec`, `soc-LiveJournal1`
2. builds `gap/converter`
3. converts the downloaded edge lists into GAP graph inputs
4. runs `bfs`, `bc`, `cc`, `pr`, `sssp`, and `tc`
5. runs four variants for each workload: `baseline`, `homp`, `swpf`, `htpf`
6. writes raw logs under `gap/output/snap/latest4/runs/<timestamp>/`

The latest results symlink is:

```sh
gap/output/snap/latest4/latest
```

## Legacy workflow

The older four-graph SNAP workflow is still available for comparison:

```sh
./scripts/run_snap_gap_experiments.sh <smt_core0> <smt_core1> [repeat]
```

It uses:

- `web-Stanford`
- `web-Google`
- `amazon0312`
- `roadNet-PA`
