CXX      ?= c++
CXXSTD   ?= -std=c++17
# -DNDEBUG compiles out the debug-only precondition assert in pop().
# The asan/tsan targets override OPT and therefore keep asserts live.
OPT      ?= -O3 -DNDEBUG
WARN     := -Wall -Wextra -Wpedantic -Wshadow

# Some macOS Command Line Tools installs ship a truncated libc++ header set in
# the toolchain directory while the SDK copy is intact. Probe once and patch the
# include path only if the default is broken.
LIBCXX_OK := $(shell printf '\#include <atomic>\nint main(){}\n' | \
             $(CXX) $(CXXSTD) -x c++ - -o /dev/null 2>/dev/null && echo yes)
ifneq ($(LIBCXX_OK),yes)
  SDKROOT_PATH := $(shell xcrun --show-sdk-path 2>/dev/null)
  ifneq ($(SDKROOT_PATH),)
    SYSHDRS := -isystem $(SDKROOT_PATH)/usr/include/c++/v1
  endif
endif

CXXFLAGS := $(CXXSTD) $(OPT) $(WARN) $(SYSHDRS) -Iinclude -Isrc $(EXTRA_CXXFLAGS)
LDLIBS   := -lpthread

BIN := build
BINARIES := $(BIN)/test_queue $(BIN)/bench_throughput $(BIN)/bench_latency \
            $(BIN)/demo_producer $(BIN)/demo_consumer
HEADERS := include/shmspsc/ShmSpscQueue.h include/shmspsc/ShmSegment.h src/common.h

.PHONY: all test bench demo asan tsan clean

all: $(BINARIES)

$(BIN):
	@mkdir -p $(BIN)

$(BIN)/%: src/%.cpp $(HEADERS) | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDLIBS)

test: $(BIN)/test_queue
	./$(BIN)/test_queue

bench: $(BIN)/bench_throughput $(BIN)/bench_latency
	./$(BIN)/bench_throughput
	@echo
	./$(BIN)/bench_latency

demo: $(BIN)/demo_producer $(BIN)/demo_consumer
	@echo "run './$(BIN)/demo_consumer' and './$(BIN)/demo_producer' in two terminals"

# Sanitizers need -O1 so the queue still behaves like a real build.
asan:
	$(MAKE) clean
	$(MAKE) test OPT="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer"

tsan:
	$(MAKE) clean
	$(MAKE) test OPT="-O1 -g -fsanitize=thread"

clean:
	rm -rf $(BIN)
