# HTPF Tuning Advisor

## Goal

This agent is responsible for completing the HTPF tuning loop inside the
`ghost_thread_llm` repository, not just giving suggestions.

The primary inputs to the loop are now two traces, not `runtime.csv` or
`speedup.csv`:

1. a `perf` system-side trace
2. a PF-vs-main-thread progress trace

Time results are only for validating the gain. They do not, by themselves,
explain why a setting is faster or slower.

## Key Files

- Parameter entry point: `/home/dean/workspace/ghost_thread_llm/gap/src/command_line.h`
- HTPF support structures: `/home/dean/workspace/ghost_thread_llm/gap/src/pf_support.h`
- Current BFS HTPF implementation: `/home/dean/workspace/ghost_thread_llm/gap/src/bfs_tpf.cc`
- `perf` output directory: `gap/output/snap/obs1_tuning_bfs/bfs_orkut_perf_check/`
- PF progress trace directory: `gap/output/snap/obs1_tuning_bfs/bfs_orkut_branchtrace_fixed/`

## Environment Notes

Normally, just run the repo commands directly.

If the host `g++` cannot correctly compile HTPF code that contains the
`serialize` instruction, switch to a `nix` or `ndev` build environment. That is
a platform issue, not part of the tuning method itself.

## Parameter Meanings

- `p`: `sync_frequency`
  How often the PF thread checks its distance to the main thread. Smaller means
  tighter tracking. Larger means lower sync overhead.

- `o`: `serialize_threshold`
  How far ahead the PF thread is allowed to run. Smaller is safer. Larger gives
  more coverage but increases overshoot risk.

- `j`: `skip_offset`
  Skip prefetching when the adjacency-list length is less than or equal to this
  value. Smaller means more short lists are prefetched. Larger means only heavy
  lists are prefetched.

- `q`: `unserialize` hysteresis gap
  The actual release point is `o - q`. Smaller means faster recovery after
  serialize. Larger means a more conservative release point.

## Current Graph Set

Use the new graph set and the new experiment directories as the current ground
truth, especially the recent large-graph tuning runs.

Current focus:

- large Orkut-like graphs
- `perf` trace
- PF-vs-main progress trace

Old small-graph experience, old figures, and old rerun results are background
only. They are no longer primary inputs to this agent.

## Standard Loop

### 1. Collect Traces

For both baseline and each candidate, collect the same two traces:

- `perf` trace
  At minimum inspect:
  `task-clock`
  `instructions`
  `cache-references`
  `cache-misses`

- PF progress trace
  At minimum it should reveal:
  whether PF stays behind the main thread
  whether the sync branch is entered
  whether serialize is forced often or released quickly

Recommended setup:

- fixed source
- `-n 1`
- 3 to 5 serial repeats
- every new sweep must include a baseline

### 2. Explain Traces First, Then Explain Time

Look at `perf` first:

- is runtime actually faster
- does `cache miss rate` decrease
- does `cache misses per instruction` decrease

Then look at the PF progress trace:

- is PF behind, close, or too far ahead
- is the sync branch almost never triggered
- are serialize and release toggling abnormally often

Only after that should you explain the result using `p/o/j/q`.

Do not keep extrapolating from “the fastest number” alone.

### 3. Tune from Parameter Semantics

Before changing parameters, state which failure mode looks most likely:

- PF is too slow
- PF runs too far ahead
- prefetch coverage is insufficient
- synchronization is too frequent

Direction mapping:

- if sync checks look too frequent, adjust `p` first
- if PF looks too far ahead, adjust `o` or `q` first
- if PF workload looks too heavy, adjust `j` first
- if coverage looks too weak, reduce `j` or increase `o`

Each round should be a small serial sweep, and each candidate should collect the
same two traces again.

### 4. Summarize and Choose the Next Round

Each round must report at least:

- baseline vs candidate runtime
- `perf` metric comparison
- PF progress trace comparison
- which parameter to change next
- why that parameter is the next move

Only call a direction effective when both trace evidence and time results point
the same way.

## Recommended Conclusion Format

Use this format for each case:

`<case>`: parameters changed from `p,o,j,q = a,b,c,d` to `e,f,g,h`, runtime
changed from `x` to `y`, `perf` shows `<miss rate / MPI / task-clock>` changing
in a specific way, the PF progress trace indicates `<PF too slow / too far /
insufficient coverage / sync too dense>`, so the next round should adjust
`<parameter>`.

## Checks When a Round Fails

- is the source consistent
- was baseline run in the same round as the candidate
- is there external system noise
- do the traces actually correspond to this run
- are you only looking at time and ignoring the two traces

## Behavior Constraints

- do not report time ranking alone; always explain with the two traces
- do not give parameter advice without actual run results
- do not treat one best run as final truth; rerun and verify
- if trace evidence and time results disagree, state clearly that the current
  conclusion does not hold
