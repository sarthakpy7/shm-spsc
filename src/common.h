// SPDX-License-Identifier: MIT
// Shared helpers for the demo and benchmark binaries.
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace demo {

struct Message {
  std::uint64_t seq;
  std::uint64_t sentNs;
  std::uint32_t payload[6];
  std::uint32_t checksum;
  std::uint32_t reserved;
};

static_assert(sizeof(Message) == 48, "Message is part of the wire format");

inline std::uint32_t checksumOf(const Message &m) {
  std::uint32_t h = 2166136261u;
  auto mix = [&h](std::uint32_t v) {
    h = (h ^ v) * 16777619u;
  };
  mix(static_cast<std::uint32_t>(m.seq));
  mix(static_cast<std::uint32_t>(m.seq >> 32));
  for (std::uint32_t v : m.payload) {
    mix(v);
  }
  return h;
}

inline Message makeMessage(std::uint64_t seq, std::uint64_t sentNs) {
  Message m{};
  m.seq = seq;
  m.sentNs = sentNs;
  for (std::uint32_t i = 0; i < 6; ++i) {
    m.payload[i] = static_cast<std::uint32_t>(seq * 31 + i);
  }
  m.checksum = checksumOf(m);
  return m;
}

inline std::uint64_t nowNs() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// `sorted` must already be sorted ascending.
inline std::uint64_t percentile(const std::vector<std::uint64_t> &sorted,
                                double p) {
  if (sorted.empty()) {
    return 0;
  }
  const std::size_t i = static_cast<std::size_t>(p * (sorted.size() - 1));
  return sorted[std::min(i, sorted.size() - 1)];
}

inline void printLatency(const char *label,
                         std::vector<std::uint64_t> samples) {
  std::sort(samples.begin(), samples.end());
  std::printf("  %-14s p50 %6llu   p99 %6llu   p99.9 %7llu   max %8llu  (ns)\n",
              label,
              static_cast<unsigned long long>(percentile(samples, 0.50)),
              static_cast<unsigned long long>(percentile(samples, 0.99)),
              static_cast<unsigned long long>(percentile(samples, 0.999)),
              static_cast<unsigned long long>(samples.back()));
}

// Clock overhead is measured, not assumed -- on macOS a steady_clock read costs
// tens of ns, which is a large fraction of the numbers we report.
inline std::uint64_t clockOverheadNs(int iters = 200000) {
  const std::uint64_t start = nowNs();
  std::uint64_t sink = 0;
  for (int i = 0; i < iters; ++i) {
    sink += nowNs();
  }
  const std::uint64_t end = nowNs();
  asm volatile("" : : "r"(sink) : "memory");
  return (end - start) / static_cast<std::uint64_t>(iters);
}

} // namespace demo
