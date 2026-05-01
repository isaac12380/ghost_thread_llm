# See LICENSE.txt for license details.

CXX_FLAGS += -std=c++11 -pthread -O3 -Wall -w
PAR_FLAG = -fopenmp
SERIAL = 0

ifneq (,$(findstring icpc,$(CXX)))
	PAR_FLAG = -openmp
endif

ifneq (,$(findstring sunCC,$(CXX)))
	CXX_FLAGS = -std=c++11 -xO3 -m64 -xtarget=native
	PAR_FLAG = -xopenmp
endif

ifneq ($(SERIAL), 1)
	CXX_FLAGS += $(PAR_FLAG)
endif

KERNELS = bc bfs cc cc_sv pr pr_spmv sssp tc
TPF_BINARIES = pr_tpf_plain pr_htpf
TPF_WEB_BINARIES = bc_tpf_plain_web bc_htpf_web pr_tpf_plain_web pr_htpf_web
SUITE = $(KERNELS) converter $(TPF_BINARIES) $(TPF_WEB_BINARIES)

.PHONY: all bc_web pr_web
all: $(SUITE) 

bc_web: bc_tpf_plain_web bc_htpf_web

pr_web: pr_tpf_plain_web pr_htpf_web

% : src/%.cc src/*.h
	$(CXX) $(CXX_FLAGS) $< -o $@

bc_tpf_plain_web: src/bc_tpf.cc src/*.h
	mkdir -p $(CURDIR)/../tmp_build
	TMPDIR=$(CURDIR)/../tmp_build $(CXX) $(CXX_FLAGS) -DWEB $< -o $@

bc_htpf_web: src/bc_tpf.cc src/*.h
	mkdir -p $(CURDIR)/../tmp_build
	TMPDIR=$(CURDIR)/../tmp_build $(CXX) $(CXX_FLAGS) -DHTPF -DWEB -DTUNING $< -o $@

pr_tpf_plain: src/pr_tpf.cc src/*.h
	mkdir -p $(CURDIR)/../tmp_build
	TMPDIR=$(CURDIR)/../tmp_build $(CXX) $(CXX_FLAGS) $< -o $@

pr_htpf: src/pr_tpf.cc src/*.h
	mkdir -p $(CURDIR)/../tmp_build
	TMPDIR=$(CURDIR)/../tmp_build $(CXX) $(CXX_FLAGS) -DHTPF -DTUNING $< -o $@

pr_tpf_plain_web: src/pr_tpf.cc src/*.h
	mkdir -p $(CURDIR)/../tmp_build
	TMPDIR=$(CURDIR)/../tmp_build $(CXX) $(CXX_FLAGS) -DWEB $< -o $@

pr_htpf_web: src/pr_tpf.cc src/*.h
	mkdir -p $(CURDIR)/../tmp_build
	TMPDIR=$(CURDIR)/../tmp_build $(CXX) $(CXX_FLAGS) -DHTPF -DWEB -DTUNING $< -o $@

# Testing
-include test/test.mk

# Benchmark Automation
-include benchmark/bench.mk


.PHONY: clean
clean:
	rm -f $(SUITE) test/out/*
