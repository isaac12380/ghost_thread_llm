// Copyright (c) 2015, The Regents of the University of California (Regents)
// See LICENSE.txt for license details

#include <functional>
#include <iostream>
#include <vector>
#include <map>

#include <omp.h>

#include "benchmark.h"
#include "bitmap.h"
#include "builder.h"
#include "command_line.h"
#include "graph.h"
#include "platform_atomics.h"
#include "pvector.h"
#include "sliding_queue.h"
#include "timer.h"
#include "util.h"


/*
GAP Benchmark Suite
Kernel: Betweenness Centrality (BC)
Author: Scott Beamer

Will return array of approx betweenness centrality scores for each vertex

This BC implementation makes use of the Brandes [1] algorithm with
implementation optimizations from Madduri et al. [2]. It is only approximate
because it does not compute the paths from every start vertex, but only a small
subset of them. Additionally, the scores are normalized to the range [0,1].

As an optimization to save memory, this implementation uses a Bitmap to hold
succ (list of successors) found during the BFS phase that are used in the back-
propagation phase.

[1] Ulrik Brandes. "A faster algorithm for betweenness centrality." Journal of
    Mathematical Sociology, 25(2):163–177, 2001.

[2] Kamesh Madduri, David Ediger, Karl Jiang, David A Bader, and Daniel
    Chavarria-Miranda. "A faster parallel algorithm and efficient multithreaded
    implementations for evaluating betweenness centrality on massive datasets."
    International Symposium on Parallel & Distributed Processing (IPDPS), 2009.
*/

// #define INNER_COUNT

using namespace std;
typedef float ScoreT;
typedef double CountT;

#if defined(GHOSTPF) && defined(GT_WEB_STANFORD)
#define GT_BC_PBFS_PRAGMA _Pragma("ghost_threading sync_frequency(200) skip_iter(1) serial_max_threshold(80) serial_min_threshold(50) accurate_sync(disable)")
#define GT_BC_BACK_PRAGMA _Pragma("ghost_threading sync_frequency(200) skip_iter(1) serial_max_threshold(80) serial_min_threshold(50) accurate_sync(disable)")
#elif defined(GHOSTPF) && defined(GT_WEB_GOOGLE)
#define GT_BC_PBFS_PRAGMA _Pragma("ghost_threading sync_frequency(400) skip_iter(1) serial_max_threshold(300) serial_min_threshold(295) accurate_sync(disable)")
#define GT_BC_BACK_PRAGMA _Pragma("ghost_threading sync_frequency(400) skip_iter(1) serial_max_threshold(300) serial_min_threshold(295) accurate_sync(disable)")
#elif defined(GHOSTPF) && defined(GT_AMAZON0312)
#define GT_BC_PBFS_PRAGMA _Pragma("ghost_threading sync_frequency(2) skip_iter(16) serial_max_threshold(60) serial_min_threshold(59) accurate_sync(disable)")
#define GT_BC_BACK_PRAGMA _Pragma("ghost_threading sync_frequency(2) skip_iter(16) serial_max_threshold(60) serial_min_threshold(59) accurate_sync(disable)")
#elif defined(GHOSTPF) && defined(GT_ROADNET_PA)
#define GT_BC_PBFS_PRAGMA _Pragma("ghost_threading sync_frequency(500) skip_iter(1) serial_max_threshold(80) serial_min_threshold(50) accurate_sync(disable)")
#define GT_BC_BACK_PRAGMA _Pragma("ghost_threading sync_frequency(500) skip_iter(1) serial_max_threshold(80) serial_min_threshold(50) accurate_sync(disable)")
#else
#define GT_BC_PBFS_PRAGMA
#define GT_BC_BACK_PRAGMA
#endif

#ifdef INNER_COUNT
#define BOUNDARY 64 

static map<int, uint64_t> inner_histogram; 
uint64_t max_count; 
#endif 

static double loop_time; 
Timer loop_timer; 

// #define SWPF 
// #define OMP

#ifndef NT
#define NT 32
#endif

void PBFS(const Graph &g, NodeID source, pvector<CountT> &path_counts,
    Bitmap &succ, vector<SlidingQueue<NodeID>::iterator> &depth_index,
    SlidingQueue<NodeID> &queue) {
  pvector<NodeID> depths(g.num_nodes(), -1);
  depths[source] = 0;
  path_counts[source] = 1;
  queue.push_back(source);
  depth_index.push_back(queue.begin());
  queue.slide_window();
  const NodeID* g_out_start = g.out_neigh(0).begin();
  #if !defined(GHOSTPF)
  #pragma omp parallel
  #endif
  {
    NodeID depth = 0;
    QueueBuffer<NodeID> lqueue(queue);
    while (!queue.empty()) {
      depth++;
      // #pragma omp barrier
      // #pragma omp single
      // {
      //   loop_timer.Start(); 
      // }
      #if defined(GHOSTPF)
      GT_BC_PBFS_PRAGMA
      #else
      #pragma omp for schedule(dynamic, 64) nowait
      #endif
      for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++) { // iter per invoc 16777216
        NodeID u = *q_iter;
        #ifdef INNER_COUNT
        uint64_t count = 0; 
        #endif 
        for (NodeID* v = g.out_neigh(u).begin(); v < g.out_neigh(u).end(); v++) { // iter per invoc 32 
        // for (NodeID &v : g.out_neigh(u)) { // iter per invoc 32 
          // v: 20 cpi, 20%; depths[v] 62 cpi, 58% 
          #if defined(GHOSTPF)
          if (v + 64 < g.out_neigh(u).end()) {
            __builtin_prefetch(v + 64);
            __builtin_prefetch(&depths[*(v + 32)]);
          }
          #elif defined(SWPF)
          if (v + 64 < g.out_neigh(u).end()) {
            __builtin_prefetch(v + 64); 
            __builtin_prefetch(&depths[*(v + 32)]); 
          }
          #endif 
          if ((depths[*v] == -1) &&
              (compare_and_swap(depths[*v], static_cast<NodeID>(-1), depth))) {
            lqueue.push_back(*v);
          }
          if (depths[*v] == depth) {
            succ.set_bit_atomic(v - g_out_start);
            #pragma omp atomic
            path_counts[*v] += path_counts[u]; // path_counts[v] 69 cpi, 11.7% 
          }
          #ifdef INNER_COUNT
          count++; 
          #endif 
        }
        #ifdef INNER_COUNT
        max_count = max_count >= count ? max_count : count; 
        if (count >= BOUNDARY)
          inner_histogram[BOUNDARY]++; 
        else
          inner_histogram[count]++;   
        #endif 
      }
      // #pragma omp barrier
      // #pragma omp single nowait 
      // {
      //   loop_timer.Stop(); 
      //   loop_time += loop_timer.Seconds(); 
      // } 
      lqueue.flush();
      #pragma omp barrier
      #pragma omp single
      {
        depth_index.push_back(queue.begin());
        queue.slide_window();
      }
    }
  }
  depth_index.push_back(queue.begin());
}


pvector<ScoreT> Brandes(const Graph &g, SourcePicker<Graph> &sp,
                        NodeID num_iters, bool logging_enabled = false) {
  Timer t;
  t.Start();
  pvector<ScoreT> scores(g.num_nodes(), 0);
  pvector<CountT> path_counts(g.num_nodes());
  Bitmap succ(g.num_edges_directed());
  vector<SlidingQueue<NodeID>::iterator> depth_index;
  SlidingQueue<NodeID> queue(g.num_nodes());
  t.Stop();
  if (logging_enabled)
    PrintStep("a", t.Seconds());
  const NodeID* g_out_start = g.out_neigh(0).begin();
  for (NodeID iter=0; iter < num_iters; iter++) {
    NodeID source = sp.PickNext();
    if (logging_enabled)
      PrintStep("Source", static_cast<int64_t>(source));
    t.Start();
    path_counts.fill(0);
    depth_index.resize(0);
    queue.reset();
    succ.reset();
    PBFS(g, source, path_counts, succ, depth_index, queue);
    t.Stop();
    if (logging_enabled)
      PrintStep("b", t.Seconds());
    pvector<ScoreT> deltas(g.num_nodes(), 0);
    t.Start();
    for (int d=depth_index.size()-2; d >= 0; d--) {
      // const auto start = chrono::high_resolution_clock::now();
      #if defined(GHOSTPF)
      GT_BC_BACK_PRAGMA
      #else
      #pragma omp parallel for schedule(dynamic, 64)
      #endif
      for (auto it = depth_index[d]; it < depth_index[d+1]; it++) { // 16777216 iter per invoc 
        NodeID u = *it;
        ScoreT delta_u = 0;
        #if defined(GHOSTPF)
        g.out_neigh(u).prefetch_end();
        #endif
        for (NodeID *v = g.out_neigh(u).begin(); v < g.out_neigh(u).end(); v++) {
        // for (NodeID &v : g.out_neigh(u)) {
          #if defined(GHOSTPF)
          if (v + 64 < g.out_neigh(u).end()) {
            __builtin_prefetch(v + 64);
          }
          if (succ.get_bit(v - g_out_start)) {
            __builtin_prefetch(&path_counts[*v]);
            __builtin_prefetch(&deltas[*v]);
          }
          #elif defined(SWPF)
          if (v + 64 < g.out_neigh(u).end()) {
            __builtin_prefetch(v + 64); 
            __builtin_prefetch(&path_counts[*(v+32)]); 
          }
          #endif 
          if (succ.get_bit(v - g_out_start)) {
            delta_u += (path_counts[u] / path_counts[*v]) * (1 + deltas[*v]);
          }
        }
        deltas[u] = delta_u;
        scores[u] += delta_u;
      }
      // const auto end = chrono::high_resolution_clock::now();
      // loop_time += chrono::duration_cast<chrono::microseconds>(end - start).count(); 
    }
    t.Stop();
    if (logging_enabled)
      PrintStep("p", t.Seconds());
  }
  // normalize scores
  ScoreT biggest_score = 0;
  #pragma omp parallel for reduction(max : biggest_score)
  for (NodeID n=0; n < g.num_nodes(); n++)
    biggest_score = max(biggest_score, scores[n]);
  #pragma omp parallel for
  for (NodeID n=0; n < g.num_nodes(); n++)
    scores[n] = scores[n] / biggest_score;
  return scores;
}


void PrintTopScores(const Graph &g, const pvector<ScoreT> &scores) {
  vector<pair<NodeID, ScoreT>> score_pairs(g.num_nodes());
  for (NodeID n : g.vertices())
    score_pairs[n] = make_pair(n, scores[n]);
  int k = 5;
  vector<pair<ScoreT, NodeID>> top_k = TopK(score_pairs, k);
  for (auto kvp : top_k)
    cout << kvp.second << ":" << kvp.first << endl;
}


// Still uses Brandes algorithm, but has the following differences:
// - serial (no need for atomics or dynamic scheduling)
// - uses vector for BFS queue
// - regenerates farthest to closest traversal order from depths
// - regenerates successors from depths
bool BCVerifier(const Graph &g, SourcePicker<Graph> &sp, NodeID num_iters,
                const pvector<ScoreT> &scores_to_test) {
  pvector<ScoreT> scores(g.num_nodes(), 0);
  for (int iter=0; iter < num_iters; iter++) {
    NodeID source = sp.PickNext();
    // BFS phase, only records depth & path_counts
    pvector<int> depths(g.num_nodes(), -1);
    depths[source] = 0;
    vector<CountT> path_counts(g.num_nodes(), 0);
    path_counts[source] = 1;
    vector<NodeID> to_visit;
    to_visit.reserve(g.num_nodes());
    to_visit.push_back(source);
    for (auto it = to_visit.begin(); it != to_visit.end(); it++) {
      NodeID u = *it;
      for (NodeID v : g.out_neigh(u)) {
        if (depths[v] == -1) {
          depths[v] = depths[u] + 1;
          to_visit.push_back(v);
        }
        if (depths[v] == depths[u] + 1)
          path_counts[v] += path_counts[u];
      }
    }
    // Get lists of vertices at each depth
    vector<vector<NodeID>> verts_at_depth;
    for (NodeID n : g.vertices()) {
      if (depths[n] != -1) {
        if (depths[n] >= static_cast<int>(verts_at_depth.size()))
          verts_at_depth.resize(depths[n] + 1);
        verts_at_depth[depths[n]].push_back(n);
      }
    }
    // Going from farthest to closest, compute "dependencies" (deltas)
    pvector<ScoreT> deltas(g.num_nodes(), 0);
    for (int depth=verts_at_depth.size()-1; depth >= 0; depth--) {
      for (NodeID u : verts_at_depth[depth]) {
        for (NodeID v : g.out_neigh(u)) {
          if (depths[v] == depths[u] + 1) {
            deltas[u] += (path_counts[u] / path_counts[v]) * (1 + deltas[v]);
          }
        }
        scores[u] += deltas[u];
      }
    }
  }
  // Normalize scores
  ScoreT biggest_score = *max_element(scores.begin(), scores.end());
  for (NodeID n : g.vertices())
    scores[n] = scores[n] / biggest_score;
  // Compare scores
  bool all_ok = true;
  for (NodeID n : g.vertices()) {
    ScoreT delta = abs(scores_to_test[n] - scores[n]);
    if (delta > std::numeric_limits<ScoreT>::epsilon()) {
      cout << n << ": " << scores[n] << " != " << scores_to_test[n];
      cout << "(" << delta << ")" << endl;
      all_ok = false;
    }
  }
  return all_ok;
}


int main(int argc, char* argv[]) {
  #ifdef OMP
  omp_set_num_threads(NT);
  #endif  
  #ifdef INNER_COUNT
  max_count = 0; 
  for (int i = 0; i <= BOUNDARY; i++) {
    inner_histogram[i] = 0; 
  }
  #endif 
  // loop_time = 0.0; 
  CLIterApp cli(argc, argv, "betweenness-centrality", 1);
  if (!cli.ParseArgs())
    return -1;
  if (cli.num_iters() > 1 && cli.start_vertex() != -1) // @profile: understand cli.start_vertex()
    cout << "Warning: iterating from same source (-r & -i)" << endl;
  Builder b(cli);
  Graph g = b.MakeGraph();
  SourcePicker<Graph> sp(g, cli.start_vertex());
  auto BCBound = [&sp, &cli] (const Graph &g) {
    return Brandes(g, sp, cli.num_iters(), cli.logging_en());
  };
  SourcePicker<Graph> vsp(g, cli.start_vertex());
  auto VerifierBound = [&vsp, &cli] (const Graph &g,
                                     const pvector<ScoreT> &scores) {
    return BCVerifier(g, vsp, cli.num_iters(), scores);
  };
  BenchmarkKernel(cli, g, BCBound, PrintTopScores, VerifierBound);
  // cout << "loop time = " << loop_time << "s" << endl; 
  #ifdef INNER_COUNT
  for (int i = 0; i <= BOUNDARY; i++) {
    cout << inner_histogram[i] << endl; 
  }
  cout << max_count << endl; 
  #endif 
  return 0;
}

#undef GT_BC_PBFS_PRAGMA
#undef GT_BC_BACK_PRAGMA
