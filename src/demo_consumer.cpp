// SPDX-License-Identifier: MIT
// Owns the shared segment and drains it. Start this first.
#include "shmspsc/ShmSpscQueue.h"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "common.h"

using demo::Message;
using shmspsc::Role;
using shmspsc::ShmSpscQueue;

namespace {
volatile std::sig_atomic_t g_stop = 0;
void onSignal(int) { g_stop = 1; }
} // namespace

int main(int argc, char **argv) {
  std::setvbuf(stdout, nullptr, _IOLBF, 0);
  const char *name = (argc > 1) ? argv[1] : "/shmspsc_demo";
  const std::size_t capacity =
      (argc > 2) ? std::strtoul(argv[2], nullptr, 10) : 8192;

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);

  try {
    auto q = ShmSpscQueue<Message>::create(name, capacity, Role::Consumer);
    std::printf("consumer  pid %d  segment %s  capacity %zu slots (%zu KiB)\n",
                ::getpid(), name, q.capacity(),
                ShmSpscQueue<Message>::bytesNeeded(capacity) / 1024);
    std::printf("waiting for a producer...\n");

    while (!g_stop && !q.waitForPeer(1000)) {
    }
    if (g_stop) {
      return 0;
    }
    std::printf("producer attached (pid %d)\n\n",
                q.control()->producerPid.load());

    std::vector<Message> buf(256);
    std::vector<std::uint64_t> latency;
    latency.reserve(1u << 20);

    std::uint64_t received = 0, expect = 0, errors = 0;
    const std::uint64_t start = demo::nowNs();
    std::uint64_t lastReport = start;

    while (!g_stop) {
      const std::size_t n = q.pop_n(buf.data(), buf.size());
      if (n == 0) {
        if (!q.peerAlive() && q.empty()) {
          break;
        }
        continue;
      }
      const std::uint64_t now = demo::nowNs();
      for (std::size_t i = 0; i < n; ++i) {
        if (buf[i].seq != expect++ ||
            buf[i].checksum != demo::checksumOf(buf[i])) {
          ++errors;
          expect = buf[i].seq + 1;
        }
        if (latency.size() < latency.capacity() && buf[i].sentNs != 0) {
          latency.push_back(now - buf[i].sentNs);
        }
      }
      received += n;

      if (now - lastReport >= 1000000000ULL) {
        std::printf("  %10llu msgs   %8.0f k/s   queue depth %6zu\n",
                    static_cast<unsigned long long>(received),
                    static_cast<double>(received) * 1e6 /
                        static_cast<double>(now - start),
                    q.size());
        lastReport = now;
      }
    }

    const std::uint64_t elapsed = demo::nowNs() - start;
    std::printf("\nreceived %llu messages in %.3f s  (%.0f k msg/s, %.0f MB/s)\n",
                static_cast<unsigned long long>(received),
                static_cast<double>(elapsed) / 1e9,
                static_cast<double>(received) * 1e6 /
                    static_cast<double>(elapsed),
                static_cast<double>(received) * sizeof(Message) * 1000.0 /
                    static_cast<double>(elapsed));
    std::printf("sequence/checksum errors: %llu\n",
                static_cast<unsigned long long>(errors));
    if (!latency.empty()) {
      // An unpaced producer keeps the ring full, so this is transport latency
      // plus queuing delay. bench_latency measures the unloaded path.
      demo::printLatency("one way+queue", latency);
    }
    return errors == 0 ? 0 : 1;
  } catch (const std::exception &e) {
    std::fprintf(stderr, "consumer: %s\n", e.what());
    return 1;
  }
}
