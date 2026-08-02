// SPDX-License-Identifier: MIT
// Attaches to a segment the consumer created and streams messages into it.
#include "shmspsc/ShmSpscQueue.h"

#include <csignal>
#include <cstdio>
#include <cstdlib>

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
  const std::uint64_t total =
      (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 5000000;
  const char *name = (argc > 2) ? argv[2] : "/shmspsc_demo";

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);

  try {
    auto q = ShmSpscQueue<Message>::attach(name, Role::Producer, 10000);
    std::printf("producer  pid %d  segment %s  capacity %zu slots\n", ::getpid(),
                name, q.capacity());
    std::printf("sending %llu messages...\n",
                static_cast<unsigned long long>(total));

    const std::uint64_t start = demo::nowNs();
    std::uint64_t sent = 0;
    for (; sent < total && !g_stop; ++sent) {
      if (!q.push(demo::makeMessage(sent, demo::nowNs()))) {
        std::printf("consumer went away after %llu messages\n",
                    static_cast<unsigned long long>(sent));
        break;
      }
    }
    const std::uint64_t elapsed = demo::nowNs() - start;

    std::printf("sent %llu messages in %.3f s  (%.0f k msg/s)\n",
                static_cast<unsigned long long>(sent),
                static_cast<double>(elapsed) / 1e9,
                static_cast<double>(sent) * 1e6 /
                    static_cast<double>(elapsed ? elapsed : 1));
    return 0;
  } catch (const std::exception &e) {
    std::fprintf(stderr, "producer: %s\n(start demo_consumer first)\n",
                 e.what());
    return 1;
  }
}
