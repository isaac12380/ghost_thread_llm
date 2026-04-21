// Copyright (c) 2015, The Regents of the University of California (Regents)
// See LICENSE.txt for license details

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "omp.h"

#include "benchmark.h"
#include "bitmap.h"
#include "builder.h"
#include "command_line.h"
#include "graph.h"
#include "platform_atomics.h"
#include "pvector.h"
#include "sliding_queue.h"
#include "timer.h"
#include "pf_support.h"


/*
GAP Benchmark Suite
Kernel: Breadth-First Search (BFS)
Author: Scott Beamer

Will return parent array for a BFS traversal from a source vertex

This BFS implementation makes use of the Direction-Optimizing approach [1].
It uses the alpha and beta parameters to determine whether to switch search
directions. For representing the frontier, it uses a SlidingQueue for the
top-down approach and a Bitmap for the bottom-up approach. To reduce
false-sharing for the top-down approach, thread-local QueueBuffer's are used.

To save time computing the number of edges exiting the frontier, this
implementation precomputes the degrees in bulk at the beginning by storing
them in the parent array as negative numbers. Thus, the encoding of parent is:
  parent[x] < 0 implies x is unvisited and parent[x] = -out_degree(x)
  parent[x] >= 0 implies x been visited

[1] Scott Beamer, Krste Asanović, and David Patterson. "Direction-Optimizing
    Breadth-First Search." International Conference on High Performance
    Computing, Networking, Storage and Analysis (SC), Salt Lake City, Utah,
    November 2012.
*/


#define ORDER_READ memory_order_relaxed
#define ORDER_WRITE memory_order_relaxed

#define BOUND 5

// #define HTPF
// #define OMP
// #define TIME

// #define LOOP1
// #define LOOP2

// #define ROAD

#ifdef HTPF_EPISODE_TRACE_LIGHT
#define EPISODE_TRACE_ONLY(stmt) do { stmt; } while (0)
#else
#define EPISODE_TRACE_ONLY(stmt) do { } while (0)
#endif

#ifdef HTPF_HEAVY_TRACE
#define HEAVY_TRACE_ONLY(stmt) do { stmt; } while (0)
#else
#define HEAVY_TRACE_ONLY(stmt) do { } while (0)
#endif

#ifdef URAND
#define FIRST
#endif 

#if defined(KRON) || defined(TWITTER) || defined(WEB) || defined(ROAD) 
#define INNER
#endif 

#ifdef TIME
#define TOTAL_ITER 2820221534
#define FREQ 1000
uint32_t stamp_counter, array_counter; 
std::chrono::_V2::system_clock::time_point time_array[TOTAL_ITER/FREQ+100]; 

#if defined(HTPF)
  #if defined(KRON)
    #define OUTPUT "timestamp/bfs.htpf.kron.csv"
  #elif defined(TWITTER)
    #define OUTPUT "timestamp/bfs.htpf.twitter.csv"
  #elif defined(URAND)
    #define OUTPUT "timestamp/bfs.htpf.urand.csv"
  #elif defined(ROAD)
    #define OUTPUT "timestamp/bfs.htpf.road.csv"
  #elif defined(WEB)
    #define OUTPUT "timestamp/bfs.htpf.web.csv"
  #endif 
#elif defined(OMP)
  #if defined(KRON)
    #define OUTPUT "timestamp/bfs.homp.kron.csv"
  #elif defined(TWITTER)
    #define OUTPUT "timestamp/bfs.homp.twitter.csv"
  #elif defined(URAND)
    #define OUTPUT "timestamp/bfs.homp.urand.csv"
  #elif defined(ROAD)
    #define OUTPUT "timestamp/bfs.homp.road.csv"
  #elif defined(WEB)
    #define OUTPUT "timestamp/bfs.homp.web.csv"
  #endif 
#else
  #if defined(KRON)
    #define OUTPUT "timestamp/bfs.kron.csv"
  #elif defined(TWITTER)
    #define OUTPUT "timestamp/bfs.twitter.csv"
  #elif defined(URAND)
    #define OUTPUT "timestamp/bfs.urand.csv"
  #elif defined(ROAD)
    #define OUTPUT "timestamp/bfs.road.csv"
  #elif defined(WEB)
    #define OUTPUT "timestamp/bfs.web.csv"
  #endif 
#endif 
#endif // TIME 

using namespace std;

TimeDiff histogram; 
OMPSyncAtomic time_diff(2); 
HyperParam_PfT hyper_param; 
bool g_pf_sync_branch_trace_enabled = false;
const char *g_pf_sync_branch_trace_path = nullptr;
size_t g_pf_sync_main_ge_j = 0;
size_t g_pf_sync_force_serialize = 0;
size_t g_pf_sync_release_serialize = 0;

void ResetPfSyncBranchTrace() {
  g_pf_sync_branch_trace_path = getenv("HTPF_SYNC_BRANCH_TRACE");
  g_pf_sync_branch_trace_enabled =
      g_pf_sync_branch_trace_path != nullptr &&
      g_pf_sync_branch_trace_path[0] != '\0';
  g_pf_sync_main_ge_j = 0;
  g_pf_sync_force_serialize = 0;
  g_pf_sync_release_serialize = 0;
}

void DumpPfSyncBranchTrace() {
  if (!g_pf_sync_branch_trace_enabled)
    return;
  ofstream out(g_pf_sync_branch_trace_path);
  out << "main_ge_j,force_serialize,release_serialize\n";
  out << g_pf_sync_main_ge_j << "," << g_pf_sync_force_serialize << ","
      << g_pf_sync_release_serialize << "\n";
}

struct PfEpisodeTrace {
  struct Episode {
    size_t entry_pf_j;
    size_t entry_main_j;
    size_t entry_diff;
    size_t exit_pf_j;
    size_t exit_main_j;
    size_t exit_diff;
    size_t serialize_len;
    size_t free_window_to_next_periodic;
  };

  bool enabled = false;
  bool configured = false;
  bool episode_open = false;
  string output_path;
  vector<Episode> episodes;
  size_t open_entry_pf_j = 0;
  size_t open_entry_main_j = 0;
  size_t open_entry_diff = 0;

  void InitFromEnv() {
    if (configured)
      return;
    configured = true;
    const char *env = getenv("HTPF_SYNC_EPISODES");
    if (env == nullptr || env[0] == '\0')
      return;
    enabled = true;
    output_path = env;
  }

  void Reset() {
    InitFromEnv();
    if (!enabled)
      return;
    episode_open = false;
    episodes.clear();
    open_entry_pf_j = 0;
    open_entry_main_j = 0;
    open_entry_diff = 0;
  }

  void OnTransition(size_t pf_j, size_t main_j, bool serialize_before,
                    bool serialize_after, const HyperParam_PfT &param) {
    if (!enabled)
      return;
    size_t diff = pf_j >= main_j ? pf_j - main_j : 0;
    if (!serialize_before && serialize_after) {
      episode_open = true;
      open_entry_pf_j = pf_j;
      open_entry_main_j = main_j;
      open_entry_diff = diff;
      return;
    }
    if (serialize_before && !serialize_after && episode_open) {
      Episode ep;
      ep.entry_pf_j = open_entry_pf_j;
      ep.entry_main_j = open_entry_main_j;
      ep.entry_diff = open_entry_diff;
      ep.exit_pf_j = pf_j;
      ep.exit_main_j = main_j;
      ep.exit_diff = diff;
      ep.serialize_len = pf_j - open_entry_pf_j;
      ep.free_window_to_next_periodic =
          param.sync_frequency - (pf_j % param.sync_frequency);
      episodes.push_back(ep);
      episode_open = false;
    }
  }

  void Dump() const {
    if (!enabled)
      return;
    ofstream out(output_path);
    out << "entry_pf_j,entry_main_j,entry_diff,exit_pf_j,exit_main_j,exit_diff,serialize_len,free_window_to_next_periodic\n";
    for (const auto &ep : episodes) {
      out << ep.entry_pf_j << "," << ep.entry_main_j << "," << ep.entry_diff
          << "," << ep.exit_pf_j << "," << ep.exit_main_j << "," << ep.exit_diff
          << "," << ep.serialize_len << "," << ep.free_window_to_next_periodic << "\n";
    }
  }
};

PfEpisodeTrace pf_episode_trace;

struct PfIssueTrace {
  static constexpr size_t kBinWidth = 16;
  static constexpr size_t kMaxDiff = 4096;

  bool enabled = false;
  bool configured = false;
  string output_path;
  vector<uint64_t> diff_bins = vector<uint64_t>(kMaxDiff / kBinWidth, 0);
  uint64_t overflow = 0;
  uint64_t total_prefetches = 0;
  uint64_t slow_prefetches = 0;

  void InitFromEnv() {
    if (configured)
      return;
    configured = true;
    const char *env = getenv("HTPF_PREFETCH_ISSUE_TRACE");
    if (env == nullptr || env[0] == '\0')
      return;
    enabled = true;
    output_path = env;
  }

  void Reset() {
    InitFromEnv();
    if (!enabled)
      return;
    fill(diff_bins.begin(), diff_bins.end(), 0);
    overflow = 0;
    total_prefetches = 0;
    slow_prefetches = 0;
  }

  void Record(size_t main_j, size_t pf_j) {
    if (!enabled)
      return;
    total_prefetches++;
    if (main_j >= pf_j) {
      slow_prefetches++;
      return;
    }
    size_t diff = pf_j - main_j;
    if (diff >= kMaxDiff) {
      overflow++;
    } else {
      diff_bins[diff / kBinWidth]++;
    }
  }

  void Dump() const {
    if (!enabled)
      return;
    ofstream out(output_path);
    out << "metric,value\n";
    out << "total_prefetches," << total_prefetches << "\n";
    out << "slow_prefetches," << slow_prefetches << "\n";
    out << "overflow_prefetches," << overflow << "\n";
    out << "\n";
    out << "bin_lo,bin_hi,count\n";
    for (size_t idx = 0; idx < diff_bins.size(); idx++) {
      size_t lo = idx * kBinWidth;
      size_t hi = lo + kBinWidth - 1;
      out << lo << "," << hi << "," << diff_bins[idx] << "\n";
    }
    out << kMaxDiff << ",inf," << overflow << "\n";
  }
};

PfIssueTrace pf_issue_trace;

struct PfConsumeTrace {
  static constexpr size_t kBinWidth = 16;
  static constexpr size_t kMaxLead = 4096;

  bool enabled = false;
  bool configured = false;
  string output_path;
  uint32_t sample_shift = 8;
  vector<atomic<uint64_t>> first_pf_j_plus1;
  vector<atomic<uint64_t>> last_pf_j_plus1;
  vector<uint64_t> first_lead_bins = vector<uint64_t>(kMaxLead / kBinWidth, 0);
  vector<uint64_t> last_lead_bins = vector<uint64_t>(kMaxLead / kBinWidth, 0);
  uint64_t first_overflow = 0;
  uint64_t last_overflow = 0;
  uint64_t sampled_prefetch_records = 0;
  uint64_t sampled_consumes = 0;
  uint64_t no_prefetch_record = 0;
  uint64_t first_late_or_same = 0;
  uint64_t last_late_or_same = 0;

  void InitFromEnv() {
    if (configured)
      return;
    configured = true;
    const char *env = getenv("HTPF_PREFETCH_CONSUME_TRACE");
    if (env == nullptr || env[0] == '\0')
      return;
    enabled = true;
    output_path = env;
    const char *shift_env = getenv("HTPF_PREFETCH_CONSUME_SAMPLE_SHIFT");
    if (shift_env != nullptr && shift_env[0] != '\0') {
      int parsed = atoi(shift_env);
      if (parsed >= 0 && parsed <= 20)
        sample_shift = static_cast<uint32_t>(parsed);
    }
  }

  void ResetSummary() {
    InitFromEnv();
    if (!enabled)
      return;
    fill(first_lead_bins.begin(), first_lead_bins.end(), 0);
    fill(last_lead_bins.begin(), last_lead_bins.end(), 0);
    first_overflow = 0;
    last_overflow = 0;
    sampled_prefetch_records = 0;
    sampled_consumes = 0;
    no_prefetch_record = 0;
    first_late_or_same = 0;
    last_late_or_same = 0;
  }

  void PrepareStorage(size_t num_nodes) {
    if (!enabled)
      return;
    if (first_pf_j_plus1.size() != num_nodes) {
      first_pf_j_plus1 = vector<atomic<uint64_t>>(num_nodes);
      last_pf_j_plus1 = vector<atomic<uint64_t>>(num_nodes);
    }
  }

  void BeginRun() {
    if (!enabled)
      return;
    for (auto &slot : first_pf_j_plus1)
      slot.store(0, memory_order_relaxed);
    for (auto &slot : last_pf_j_plus1)
      slot.store(0, memory_order_relaxed);
  }

  bool Sample(NodeID v) const {
    if (!enabled)
      return false;
    if (sample_shift == 0)
      return true;
    const uint64_t mask = (1ULL << sample_shift) - 1;
    const uint64_t mixed =
        static_cast<uint64_t>(static_cast<uint32_t>(v)) * 11400714819323198485ULL;
    return (mixed & mask) == 0;
  }

  void RecordPrefetch(NodeID v, size_t pf_j) {
    if (!enabled || !Sample(v))
      return;
    sampled_prefetch_records++;
    const uint64_t encoded = static_cast<uint64_t>(pf_j + 1);
    uint64_t expected = 0;
    first_pf_j_plus1[v].compare_exchange_strong(expected, encoded, memory_order_relaxed);
    last_pf_j_plus1[v].store(encoded, memory_order_relaxed);
  }

  void RecordConsume(NodeID v, size_t main_j) {
    if (!enabled || !Sample(v))
      return;
    sampled_consumes++;
    const uint64_t first_pf = first_pf_j_plus1[v].load(memory_order_relaxed);
    const uint64_t last_pf = last_pf_j_plus1[v].load(memory_order_relaxed);
    first_pf_j_plus1[v].store(0, memory_order_relaxed);
    last_pf_j_plus1[v].store(0, memory_order_relaxed);
    if (first_pf == 0) {
      no_prefetch_record++;
      return;
    }
    const size_t first_j = static_cast<size_t>(first_pf - 1);
    if (first_j >= main_j) {
      first_late_or_same++;
    } else {
      const size_t lead = main_j - first_j;
      if (lead >= kMaxLead) {
        first_overflow++;
      } else {
        first_lead_bins[lead / kBinWidth]++;
      }
    }
    if (last_pf == 0) {
      return;
    }
    const size_t last_j = static_cast<size_t>(last_pf - 1);
    if (last_j >= main_j) {
      last_late_or_same++;
    } else {
      const size_t lead = main_j - last_j;
      if (lead >= kMaxLead) {
        last_overflow++;
      } else {
        last_lead_bins[lead / kBinWidth]++;
      }
    }
  }

  void Dump() const {
    if (!enabled)
      return;
    ofstream out(output_path);
    out << "metric,value\n";
    out << "sample_shift," << sample_shift << "\n";
    out << "sample_rate,1/" << (1U << sample_shift) << "\n";
    out << "sampled_prefetch_records," << sampled_prefetch_records << "\n";
    out << "sampled_consumes," << sampled_consumes << "\n";
    out << "no_prefetch_record," << no_prefetch_record << "\n";
    out << "first_late_or_same," << first_late_or_same << "\n";
    out << "last_late_or_same," << last_late_or_same << "\n";
    out << "first_overflow_consumes," << first_overflow << "\n";
    out << "last_overflow_consumes," << last_overflow << "\n";
    out << "\n";
    out << "kind,bin_lo,bin_hi,count\n";
    for (size_t idx = 0; idx < first_lead_bins.size(); idx++) {
      size_t lo = idx * kBinWidth;
      size_t hi = lo + kBinWidth - 1;
      out << "first," << lo << "," << hi << "," << first_lead_bins[idx] << "\n";
    }
    out << "first," << kMaxLead << ",inf," << first_overflow << "\n";
    for (size_t idx = 0; idx < last_lead_bins.size(); idx++) {
      size_t lo = idx * kBinWidth;
      size_t hi = lo + kBinWidth - 1;
      out << "last," << lo << "," << hi << "," << last_lead_bins[idx] << "\n";
    }
    out << "last," << kMaxLead << ",inf," << last_overflow << "\n";
  }
};

PfConsumeTrace pf_consume_trace;

struct PfSiteCounterTrace {
  bool enabled = false;
  bool configured = false;
  string output_path;
  uint64_t td_parent_prefetch = 0;
  uint64_t td_prefetch_end = 0;
  uint64_t td_adj_v64_prefetch = 0;
  uint64_t bu_get_bit_then_pf = 0;
  uint64_t bu_prefetch_bit = 0;

  void InitFromEnv() {
    if (configured)
      return;
    configured = true;
    const char *env = getenv("HTPF_SOFTPREFETCH_SITE_TRACE");
    if (env == nullptr || env[0] == '\0')
      return;
    enabled = true;
    output_path = env;
  }

  void Reset() {
    InitFromEnv();
    if (!enabled)
      return;
    td_parent_prefetch = 0;
    td_prefetch_end = 0;
    td_adj_v64_prefetch = 0;
    bu_get_bit_then_pf = 0;
    bu_prefetch_bit = 0;
  }

  void Dump() const {
    if (!enabled)
      return;
    ofstream out(output_path);
    out << "metric,value\n";
    out << "td_parent_prefetch," << td_parent_prefetch << "\n";
    out << "td_prefetch_end," << td_prefetch_end << "\n";
    out << "td_adj_v64_prefetch," << td_adj_v64_prefetch << "\n";
    out << "bu_get_bit_then_pf," << bu_get_bit_then_pf << "\n";
    out << "bu_prefetch_bit," << bu_prefetch_bit << "\n";
  }
};

PfSiteCounterTrace pf_site_counter_trace;

void PrefetchThread1_urand(const Graph *g, NodeID *parent, const Bitmap *front) { 
  #ifdef TUNING
  HyperParam_PfT hyperparam = hyper_param; 
  #else
  // HyperParam_PfT hyperparam = {.sync_frequency = 1, .skip_offset = 24, .serialize_threshold = 90, .unserialize_threshold = 66}; // beijing
  HyperParam_PfT hyperparam = {.sync_frequency = 1, .skip_offset = 13, .serialize_threshold = 60, .unserialize_threshold = 50}; 
  #endif 
  bool serialize_flag = false; 
  for (NodeID u=0; u < g->num_nodes(); u++) { 
    if (parent[u] < 0) {
      for (NodeID *v = g->in_neigh(u).begin(); v < g->in_neigh(u).end(); v++) { 
        HEAVY_TRACE_ONLY(pf_site_counter_trace.bu_get_bit_then_pf++);
        if (front->get_bit_then_pf(*v)) { 
          break;
        }
      }
    }
    if (serialize_flag)
      asm volatile (
        ".rept 30\n\t" 
        "serialize\n\t" 
        ".endr" 
      );
    
    sync<NodeID>(u, 1, u, false, time_diff, 0, ORDER_READ, serialize_flag, hyperparam); 
  }
}

#if defined(HTPF) && defined(FIRST)
void PrefetchThread1_kron_twitter(const Graph *g, NodeID *parent, const Bitmap *front) { // 80% coverage 
  // HyperParam_PfT hyper_param_kron_twitter = {.sync_frequency = 20, .skip_offset = 50, 
  //   .serialize_threshold = 200, .unserialize_threshold = 50}; 
  HyperParam_PfT hyper_param_kron_twitter = hyper_param; 
  bool serialize_flag = false; 
  bool prefetch = true; 
  for (NodeID u=0; u < g->num_nodes(); u++) { 
    if (parent[u] < 0) {
      prefetch = g->in_neigh(u).end() - g->in_neigh(u).begin() > 64 ? true : false; 
      for (NodeID *v = g->in_neigh(u).begin(); v < g->in_neigh(u).end(); v++) { 
        if (v +64 < g->in_neigh(u).end() && prefetch) {
          HEAVY_TRACE_ONLY(pf_site_counter_trace.td_adj_v64_prefetch++);
          __builtin_prefetch(v + 64); 
        }
        if (front->get_bit(*v)) { 
          if (prefetch) {
            HEAVY_TRACE_ONLY(pf_site_counter_trace.bu_prefetch_bit++);
            front->prefetch_bit(*v); 
          }
          break;
        }
      }
    }
    if (serialize_flag)
      asm volatile (
        ".rept 30\n\t" 
        "serialize\n\t" 
        ".endr" 
      );
    sync<NodeID>(u, 1, u, false, time_diff, 0, ORDER_READ, serialize_flag, hyper_param_kron_twitter); 
  }
}
#endif 

int64_t BUStep(const Graph &g, pvector<NodeID> &parent, Bitmap &front,
               Bitmap &next) { // 80% coverage 
  int64_t awake_count = 0;
  next.reset();
  #if defined(HTPF) && defined(URAND) && !defined(BEST)
  thread PF(PrefetchThread1_urand, &g, parent.begin(), &front); 
  #elif defined(HTPF) && defined(INNER) && defined(FIRST)
  thread PF(PrefetchThread1_kron_twitter, &g, parent.begin(), &front); 
  #endif 
  #pragma omp parallel for reduction(+ : awake_count) schedule(dynamic, 1024)
  for (NodeID u=0; u < g.num_nodes(); u++) { 
    #if defined(HTPF) && defined(FIRST) && !defined(BEST)
    time_diff.set(0, (size_t) u, ORDER_WRITE); 
    #endif 
    if (parent[u] < 0) {
      for (NodeID v : g.in_neigh(u)) {
        #if defined(TIME) && defined(LOOP1)
        #ifdef OMP
        if (omp_get_thread_num() == 0) {
        #endif // OMP
        if (stamp_counter % FREQ == 0) {
          time_array[array_counter] = chrono::high_resolution_clock::now(); 
          array_counter+=1; 
        }
        stamp_counter+=1; 
        #ifdef OMP
        }
        #endif // OMP 
        #endif // TIME
        if (front.get_bit(v)) { 
          parent[u] = v;
          awake_count++;
          next.set_bit(u);
          break;
        }
      }
    }
  }
  #if defined(HTPF) && defined(FIRST) && !defined(BEST)
  PF.join(); // wait PF thread 
  #endif 
  return awake_count;
}

void PrefetchThread2_urand(const SlidingQueue<NodeID> *queue, const Graph *g, 
  const NodeID *parent) {
  #ifdef TUNING
  HyperParam_PfT hyperparam = hyper_param; 
  #else
  HyperParam_PfT hyperparam = {.sync_frequency = 15, .skip_offset = 5, 
                               .serialize_threshold = 16, .unserialize_threshold = 15}; 
  #endif 
  bool serialize_flag = false; 
  for (auto q_iter = queue->begin(); q_iter < queue->end(); q_iter++) { // 192380 iter per invoc 
    NodeID u = *q_iter;
    for (NodeID *v = g->out_neigh(u).begin(); v < g->out_neigh(u).end(); v++) { 
      __builtin_prefetch(&parent[*v]); 
    }
    if (serialize_flag)
      asm volatile (
        ".rept 50\n\t" 
        "serialize\n\t" 
        ".endr" 
      );
    sync<NodeID*>(q_iter, (int)sizeof(NodeID), (size_t) q_iter, false, time_diff, 1, ORDER_READ, serialize_flag, hyperparam); 
  }
}

void PrefetchThread2_kron_twitter(const SlidingQueue<NodeID> *queue, const Graph *g, 
  const NodeID *parent) {
  #ifdef TUNING
  HyperParam_PfT hyperparam = hyper_param; 
  #else
  // HyperParam_PfT hyper_param_kron_twitter = {.sync_frequency = 1, .skip_offset = 1, 
  //   .serialize_threshold = 10, .unserialize_threshold = 5}; // normal 
  HyperParam_PfT hyperparam = {.sync_frequency = 500, .skip_offset = 128, 
                               .serialize_threshold = 300, .unserialize_threshold = 290}; // membw for kron 
  #endif 
  bool serialize_flag = false; 
  bool prefetch = true; 
  size_t j = 0; 
  for (auto q_iter = queue->begin(); q_iter < queue->end(); q_iter++) { 
    NodeID u = *q_iter;
    prefetch = g->out_neigh(u).end() - g->out_neigh(u).begin() > hyperparam.skip_offset ? true : false; 
    #if defined(ROAD) || defined(WEB)
    HEAVY_TRACE_ONLY(pf_site_counter_trace.td_prefetch_end++);
    g->out_neigh(u).prefetch_end(); 
    #endif 
    for (NodeID *v = g->out_neigh(u).begin(); v < g->out_neigh(u).end(); v++) { 
      if (prefetch) {
        #if defined(KRON) || defined(MEMBW)
        #ifndef ROAD
        if (v + 64 < g->out_neigh(u).end()) {
          HEAVY_TRACE_ONLY(pf_site_counter_trace.td_adj_v64_prefetch++);
          __builtin_prefetch(v + 64); // prefetch this for kron even in non-membw condition 
        }
        #else 
        __builtin_prefetch(v); // for road 
        #endif 
        #endif 

        #ifndef ROAD
        HEAVY_TRACE_ONLY(pf_issue_trace.Record(time_diff.read(1, ORDER_READ), j));
        HEAVY_TRACE_ONLY(pf_consume_trace.RecordPrefetch(*v, j));
        HEAVY_TRACE_ONLY(pf_site_counter_trace.td_parent_prefetch++);
        __builtin_prefetch(&parent[*v]); // not prefetch for road 
        #endif  
      }
      if (serialize_flag)
        asm volatile (
          ".rept 10\n\t" 
          "serialize\n\t" 
          ".endr" 
        );
        // asm volatile ("serialize\n\t"); 
      /*-----inner sync-----*/
      if (j % hyperparam.sync_frequency == 0 || serialize_flag) {
        size_t main_j = time_diff.read(1, ORDER_READ); 
        bool serialize_before = serialize_flag;
        // histogram.insert_into_atomic_histogram(main_j, j); 
        if (main_j >= j) {
          if (g_pf_sync_branch_trace_enabled)
            g_pf_sync_main_ge_j++;
          serialize_flag = false; 
        } else if (j - main_j > hyperparam.serialize_threshold) {
          if (g_pf_sync_branch_trace_enabled)
            g_pf_sync_force_serialize++;
          serialize_flag = true; 
        } else if (j - main_j < hyperparam.unserialize_threshold) {
          if (g_pf_sync_branch_trace_enabled)
            g_pf_sync_release_serialize++;
          serialize_flag = false; 
        } 
      }
      j++; 
      /*-----inner sync-----*/
    }
  }
}

int64_t TDStep(const Graph &g, pvector<NodeID> &parent,
               SlidingQueue<NodeID> &queue) { // 13% coverage 
  int64_t scout_count = 0;
  size_t main_j = 0;
  HEAVY_TRACE_ONLY(pf_consume_trace.BeginRun());
  // #pragma omp parallel
  // {
    #ifdef HTPF
    #ifdef URAND
    thread PF(PrefetchThread2_urand, &queue, &g, parent.begin()); 
    #else
    thread PF(PrefetchThread2_kron_twitter, &queue, &g, parent.begin()); 
    time_diff.set(1, 0, ORDER_WRITE); 
    #endif 
    #endif 
    QueueBuffer<NodeID> lqueue(queue);
    // #pragma omp for reduction(+ : scout_count) nowait
    for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++) {
      NodeID u = *q_iter;
      #if defined(HTPF) && defined(URAND)
      time_diff.set(1, (size_t) q_iter, ORDER_WRITE); 
      #endif 
      for (NodeID v : g.out_neigh(u)) { 
        #if defined(TIME) && defined(LOOP2)
        #ifdef OMP
        if (omp_get_thread_num() == 0) {
        #endif // OMP
        if (stamp_counter % FREQ == 0) {
          time_array[array_counter] = chrono::high_resolution_clock::now(); 
          array_counter+=1; 
        }
        stamp_counter+=1; 
        #ifdef OMP
        }
        #endif // OMP 
        #endif // TIME
        NodeID curr_val = parent[v]; 
        if (curr_val < 0) {
          HEAVY_TRACE_ONLY(pf_consume_trace.RecordConsume(v, main_j));
          if (compare_and_swap(parent[v], curr_val, u)) {
            lqueue.push_back(v);
            scout_count += -curr_val;
          }
        }
        #if defined(HTPF) && !defined(URAND)
        time_diff.add(1, 1, ORDER_WRITE); 
        #endif 
        main_j++;
      }
    }
    lqueue.flush();
    #ifdef HTPF
    PF.join(); 
    #endif 
  // }
  return scout_count;
}


void QueueToBitmap(const SlidingQueue<NodeID> &queue, Bitmap &bm) {
  #pragma omp parallel for
  for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++) {
    NodeID u = *q_iter;
    bm.set_bit_atomic(u);
  }
}

void BitmapToQueue(const Graph &g, const Bitmap &bm,
                   SlidingQueue<NodeID> &queue) {
  #pragma omp parallel
  {
    QueueBuffer<NodeID> lqueue(queue);
    #pragma omp for nowait
    for (NodeID n=0; n < g.num_nodes(); n++)
      if (bm.get_bit(n))
        lqueue.push_back(n);
    lqueue.flush();
  }
  queue.slide_window();
}

pvector<NodeID> InitParent(const Graph &g) {
  pvector<NodeID> parent(g.num_nodes());
  #pragma omp parallel for
  for (NodeID n=0; n < g.num_nodes(); n++)
    parent[n] = g.out_degree(n) != 0 ? -g.out_degree(n) : -1;
  return parent;
}

pvector<NodeID> DOBFS(const Graph &g, NodeID source, bool logging_enabled = false,
                      int alpha = 15, int beta = 18) {
  if (logging_enabled)
    PrintStep("Source", static_cast<int64_t>(source));
  Timer t;
  t.Start();
  pvector<NodeID> parent = InitParent(g);
  t.Stop();
  if (logging_enabled)
    PrintStep("i", t.Seconds());
  pf_consume_trace.PrepareStorage(g.num_nodes());
  pf_consume_trace.BeginRun();
  parent[source] = source;
  SlidingQueue<NodeID> queue(g.num_nodes());
  queue.push_back(source);
  queue.slide_window();
  Bitmap curr(g.num_nodes());
  curr.reset();
  Bitmap front(g.num_nodes());
  front.reset();
  int64_t edges_to_check = g.num_edges_directed();
  int64_t scout_count = g.out_degree(source);
  while (!queue.empty()) {
    if (scout_count > edges_to_check / alpha) {
      int64_t awake_count, old_awake_count;
      TIME_OP(t, QueueToBitmap(queue, front));
      if (logging_enabled)
        PrintStep("e", t.Seconds());
      awake_count = queue.size();
      queue.slide_window();
      do {
        t.Start();
        old_awake_count = awake_count;
        // const auto join_start = chrono::high_resolution_clock::now();
        awake_count = BUStep(g, parent, front, curr);
        // const auto join_end = chrono::high_resolution_clock::now();
        // loop_time1 += chrono::duration_cast<chrono::microseconds>(join_end - join_start).count(); 
        front.swap(curr);
        t.Stop();
        if (logging_enabled)
          PrintStep("bu", t.Seconds(), awake_count);
      } while ((awake_count >= old_awake_count) ||
               (awake_count > g.num_nodes() / beta));
      TIME_OP(t, BitmapToQueue(g, front, queue));
      if (logging_enabled)
        PrintStep("c", t.Seconds());
      scout_count = 1;
    } else {
      t.Start();
      edges_to_check -= scout_count;
      // const auto join_start = chrono::high_resolution_clock::now();
      scout_count = TDStep(g, parent, queue);
      // const auto join_end = chrono::high_resolution_clock::now();
      // loop_time2 += chrono::duration_cast<chrono::microseconds>(join_end - join_start).count(); 
      queue.slide_window();
      t.Stop();
      if (logging_enabled)
        PrintStep("td", t.Seconds(), queue.size());
    }
  }
  #pragma omp parallel for
  for (NodeID n = 0; n < g.num_nodes(); n++)
    if (parent[n] < -1)
      parent[n] = -1;
  return parent;
}


void PrintBFSStats(const Graph &g, const pvector<NodeID> &bfs_tree) {
  int64_t tree_size = 0;
  int64_t n_edges = 0;
  for (NodeID n : g.vertices()) {
    if (bfs_tree[n] >= 0) {
      n_edges += g.out_degree(n);
      tree_size++;
    }
  }
  cout << "BFS Tree has " << tree_size << " nodes and ";
  cout << n_edges << " edges" << endl;
}


// BFS verifier does a serial BFS from same source and asserts:
// - parent[source] = source
// - parent[v] = u  =>  depth[v] = depth[u] + 1 (except for source)
// - parent[v] = u  => there is edge from u to v
// - all vertices reachable from source have a parent
bool BFSVerifier(const Graph &g, NodeID source,
                 const pvector<NodeID> &parent) {
  pvector<int> depth(g.num_nodes(), -1);
  depth[source] = 0;
  vector<NodeID> to_visit;
  to_visit.reserve(g.num_nodes());
  to_visit.push_back(source);
  for (auto it = to_visit.begin(); it != to_visit.end(); it++) {
    NodeID u = *it;
    for (NodeID v : g.out_neigh(u)) {
      if (depth[v] == -1) {
        depth[v] = depth[u] + 1;
        to_visit.push_back(v);
      }
    }
  }
  for (NodeID u : g.vertices()) {
    if ((depth[u] != -1) && (parent[u] != -1)) {
      if (u == source) {
        if (!((parent[u] == u) && (depth[u] == 0))) {
          cout << "Source wrong" << endl;
          return false;
        }
        continue;
      }
      bool parent_found = false;
      for (NodeID v : g.in_neigh(u)) {
        if (v == parent[u]) {
          if (depth[v] != depth[u] - 1) {
            cout << "Wrong depths for " << u << " & " << v << endl;
            return false;
          }
          parent_found = true;
          break;
        }
      }
      if (!parent_found) {
        cout << "Couldn't find edge from " << parent[u] << " to " << u << endl;
        return false;
      }
    } else if (depth[u] != parent[u]) {
      cout << "Reachability mismatch" << endl;
      return false;
    }
  }
  return true;
}


int main(int argc, char* argv[]) {
  // time_diff.init_atomic(); 
  // histogram.init_atomic_histogram(268435456*2);
  // loop_time1 = 0; 
  // loop_time2 = 0; 
  #ifdef OMP
  omp_set_num_threads(2); 
  #endif

  #ifdef TIME
  stamp_counter = 0; 
  array_counter = 0; 
  #endif 

  CLApp cli(argc, argv, "breadth-first search");
  if (!cli.ParseArgs())
    return -1;
  ResetPfSyncBranchTrace();
  HEAVY_TRACE_ONLY(pf_issue_trace.Reset());
  HEAVY_TRACE_ONLY(pf_consume_trace.ResetSummary());
  HEAVY_TRACE_ONLY(pf_site_counter_trace.Reset());
  /*-------set hyper parameters for inter-thread sync-------*/
  hyper_param.sync_frequency = cli.sync_frequency(); 
  hyper_param.skip_offset = cli.skip_offset(); 
  hyper_param.serialize_threshold = cli.serialize_threshold(); 
  hyper_param.unserialize_threshold = cli.serialize_threshold() > cli.unserialize_threshold() ? 
            cli.serialize_threshold() - cli.unserialize_threshold() : 0; 
  cout << "sync frequency = " << hyper_param.sync_frequency << ", serialize threshold = " << hyper_param.serialize_threshold 
       << ", unserialize threshold = " << hyper_param.unserialize_threshold << ", skip offset = " << hyper_param.skip_offset << endl; 
  // hyper_param_global = hyper_param; 
  // hyper_param1.sync_frequency = 15, hyper_param1.skip_offset = 5;
  // hyper_param1.serialize_threshold = 16, hyper_param1.unserialize_threshold = 15; 
  /*-------set hyper parameters for inter-thread sync-------*/
  Builder b(cli);
  Graph g = b.MakeGraph();
  SourcePicker<Graph> sp(g, cli.start_vertex());
  auto BFSBound = [&sp,&cli] (const Graph &g) {
    return DOBFS(g, sp.PickNext(), cli.logging_en());
  };
  SourcePicker<Graph> vsp(g, cli.start_vertex());
  auto VerifierBound = [&vsp] (const Graph &g, const pvector<NodeID> &parent) {
    return BFSVerifier(g, vsp.PickNext(), parent);
  };
  #ifdef TIME
  auto kernel_start = chrono::high_resolution_clock::now();
  #endif 
  BenchmarkKernel(cli, g, BFSBound, PrintBFSStats, VerifierBound);
  DumpPfSyncBranchTrace();
  HEAVY_TRACE_ONLY(pf_issue_trace.Dump());
  HEAVY_TRACE_ONLY(pf_consume_trace.Dump());
  HEAVY_TRACE_ONLY(pf_site_counter_trace.Dump());
  // histogram.print_atomic_histogram(); 
  // cout << "loop1 time = " << loop_time1/1e6 << "s, loop2 time = " << loop_time2/1e6 << "s\n"; 
  #ifdef TIME
  ofstream myout; 
  myout.open(OUTPUT); 
  for (uint32_t i = 0; i < array_counter; i++) {
    // time_diff[i] = chrono::duration_cast<chrono::nanoseconds>(time_array[i] - kernel_start).count(); 
    myout << chrono::duration_cast<chrono::microseconds>(time_array[i] - kernel_start).count() << endl; 
  }
  #endif 
  return 0;
}
