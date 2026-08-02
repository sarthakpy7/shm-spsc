// SPDX-License-Identifier: MIT
#include "shmspsc/ShmSpscQueue.h"

#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <vector>

#include "common.h"

using demo::Message;
using shmspsc::Role;
using shmspsc::ShmSpscQueue;

namespace {

constexpr const char *kPing = "/shmspsc_ping";
constexpr const char *kPong = "/shmspsc_pong";

} // namespace

int main(int argc, char **argv) {
  std::setvbuf(stdout, nullptr, _IOLBF, 0);
  const std::uint64_t iters =
      (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 200000;
  const std::uint64_t warmup = iters / 10 + 1;

  std::printf("shmspsc latency  (%llu round trips over two %zu-byte queues)\n",
              static_cast<unsigned long long>(iters), sizeof(Message));

  const std::uint64_t clockNs = demo::clockOverheadNs();
  std::printf("steady_clock read overhead: ~%llu ns (not subtracted below)\n\n",
              static_cast<unsigned long long>(clockNs));

  auto ping = ShmSpscQueue<Message>::create(kPing, 64, Role::Producer);
  auto pong = ShmSpscQueue<Message>::create(kPong, 64, Role::Consumer);

  const pid_t pid = ::fork();
  if (pid == 0) {
    // Echo side: read ping, write it straight back on pong.
    auto in = ShmSpscQueue<Message>::attach(kPing, Role::Consumer, 5000);
    auto out = ShmSpscQueue<Message>::attach(kPong, Role::Producer, 5000);
    Message m{};
    for (std::uint64_t i = 0; i < iters + warmup; ++i) {
      while (!in.try_pop(m)) {
      }
      m.sentNs = demo::nowNs(); // stamp arrival so the parent can split the RTT
      while (!out.try_push(m)) {
      }
    }
    ::_exit(0);
  }

  if (!ping.waitForPeer(5000) || !pong.waitForPeer(5000)) {
    std::printf("echo process failed to attach\n");
    ::kill(pid, SIGKILL);
    ::waitpid(pid, nullptr, 0);
    return 1;
  }

  std::vector<std::uint64_t> rtt;
  rtt.reserve(iters);
  std::vector<std::uint64_t> oneWay;
  oneWay.reserve(iters);

  Message reply{};
  for (std::uint64_t i = 0; i < iters + warmup; ++i) {
    const std::uint64_t t0 = demo::nowNs();
    Message m = demo::makeMessage(i, t0);
    while (!ping.try_push(m)) {
    }
    while (!pong.try_pop(reply)) {
    }
    const std::uint64_t t1 = demo::nowNs();

    if (i >= warmup) {
      rtt.push_back(t1 - t0);
      // steady_clock is mach_absolute_time on macOS and CLOCK_MONOTONIC on
      // Linux: same epoch in both processes, so this cross-process delta is
      // meaningful.
      oneWay.push_back(reply.sentNs > t0 ? reply.sentNs - t0 : 0);
    }
  }

  ::waitpid(pid, nullptr, 0);

  demo::printLatency("round trip", rtt);
  demo::printLatency("one way", oneWay);
  std::printf("\nNote: macOS offers no thread affinity API, so these numbers\n"
              "include scheduler placement noise. Pin cores on Linux for a\n"
              "tighter distribution.\n");

  shmspsc::ShmSegment::remove(kPing);
  shmspsc::ShmSegment::remove(kPong);
  return 0;
}
