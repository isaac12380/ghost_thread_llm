# HTPF Tuning Advisor Plugin

This plugin is just the entry point for the repo-local HTPF tuning agent.

When working inside `/home/dean/workspace/ghost_thread_llm`, use this file as
the canonical procedure:

- `/home/dean/workspace/ghost_thread_llm/agents/htpf-tuning-advisor/AGENT.md`

The main flow is now trace-driven:

- the inputs are two traces: `perf` and PF-vs-main progress
- use those traces together with `p/o/j/q` to identify the bottleneck
- tune parameters based on that diagnosis
- then validate with runtime results

Do not claim tuning success from runtime alone.
