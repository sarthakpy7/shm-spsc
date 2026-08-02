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

constexpr const char *kName = "/shmspsc_bench";

// Wall clock spans fork, attach and full drain, so the number reported is
// end-to-end: nothing counted as delivered until the consumer has it.
void run(std::uint64_t total, std::size_t capacity, std::size_t batch) {
  auto q = ShmSpscQueue<Message>::create(kName, capacity, Role::Producer);

  const pid_t pid = ::fork();
  if (pid == 0) {
    auto c = ShmSpscQueue<Message>::attach(kName, Role::Consumer, 5000);
    std::vector<Message> buf(batch);
    std::uint64_t got = 0;
    if (batch == 1) {
      Message m{};
      while (got < total) {
        if (c.try_pop(m)) {
          ++got;
        }
      }
    } else {
      while (got < total) {
        got += c.pop_n(buf.data(), buf.size());
      }
    }
    ::_exit(0);
  }

  if (!q.waitForPeer(5000)) {
    std::printf("  consumer failed to attach\n");
    ::kill(pid, SIGKILL);
    ::waitpid(pid, nullptr, 0);
    return;
  }

  std::vector<Message> buf(batch);
  for (std::size_t i = 0; i < batch; ++i) {
    buf[i] = demo::makeMessage(i, 0);
  }

  const std::uint64_t start = demo::nowNs();
  if (batch == 1) {
    for (std::uint64_t i = 0; i < total; ++i) {
      while (!q.try_push(buf[0])) {
      }
    }
  } else {
    std::uint64_t sent = 0;
    while (sent < total) {
      const std::size_t want = static_cast<std::size_t>(
          std::min<std::uint64_t>(batch, total - sent));
      sent += q.push_n(buf.data(), want);
    }
  }
  ::waitpid(pid, nullptr, 0);
  const std::uint64_t elapsed = demo::nowNs() - start;

  const double ms = static_cast<double>(elapsed) / 1e6;
  const double opsPerMs = static_cast<double>(total) / ms;
  const double mbPerSec =
      static_cast<double>(total) * sizeof(Message) / (ms * 1000.0);

  std::printf("  batch %-4zu cap %-6zu %10.0f ops/ms  %8.0f MB/s  %6.1f ns/op\n",
              batch, capacity, opsPerMs, mbPerSec,
              static_cast<double>(elapsed) / static_cast<double>(total));
}

} // namespace

int main(int argc, char **argv) {
  std::setvbuf(stdout, nullptr, _IOLBF, 0);
  const std::uint64_t total =
      (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 10000000;

  std::printf("shmspsc throughput  (%llu x %zu-byte messages, two processes)\n",
              static_cast<unsigned long long>(total), sizeof(Message));
  std::printf("cache line %zu, capacity in slots\n\n", shmspsc::kCacheLine);

  for (std::size_t batch : {std::size_t(1), std::size_t(8), std::size_t(32),
                            std::size_t(128)}) {
    run(total, 8192, batch);
  }
  std::printf("\n");
  for (std::size_t cap : {std::size_t(64), std::size_t(1024),
                          std::size_t(65536)}) {
    run(total, cap, 32);
  }

  shmspsc::ShmSegment::remove(kName);
  return 0;
}
