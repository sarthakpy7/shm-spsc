// SPDX-License-Identifier: MIT
#pragma once

#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility> // std::swap

#include "shmspsc/ShmSegment.h"

namespace shmspsc {

#ifndef SHMSPSC_CACHE_LINE_SIZE
#if defined(__APPLE__) && defined(__aarch64__)
#define SHMSPSC_CACHE_LINE_SIZE 128 // Apple Silicon
#else
#define SHMSPSC_CACHE_LINE_SIZE 64
#endif
#endif

inline constexpr std::size_t kCacheLine = SHMSPSC_CACHE_LINE_SIZE;

enum class Role { Producer, Consumer };

// kill(pid, 0) fails both when the process is gone (ESRCH) and when we merely
// may not signal it (EPERM -- a peer running as another user). Only ESRCH means
// dead; treating EPERM as dead would let a second producer reclaim a slot that
// is still in use and silently break the SPSC invariant.
inline bool processAlive(std::int32_t pid) noexcept {
  if (pid <= 0) {
    return false;
  }
  if (::kill(static_cast<pid_t>(pid), 0) == 0) {
    return true;
  }
  return errno != ESRCH;
}

// Lives at offset 0 of the shared segment. Layout is part of the wire format:
// both processes compile it independently and must agree byte for byte.
struct alignas(kCacheLine) ControlBlock {
  // Line 0: geometry, written once before `ready`, read-only afterwards.
  // Field order is chosen so the compiler inserts no implicit padding; the
  // block must be exactly 56 bytes to fit a 64-byte line on x86.
  std::uint64_t magic;
  std::uint64_t capacity; // includes the one unused slack slot
  std::uint64_t totalBytes;
  std::uint32_t version;
  std::uint32_t cacheLineSize;
  std::uint32_t elemSize;
  std::uint32_t elemAlign;
  std::uint32_t slotsOffset;
  std::atomic<std::int32_t> producerPid;
  std::atomic<std::int32_t> consumerPid;
  std::atomic<std::uint32_t> ready;
  char pad0_[kCacheLine - 56];

  // Producer's line.
  alignas(kCacheLine) std::atomic<std::uint64_t> writeIdx;
  char pad1_[kCacheLine - sizeof(std::atomic<std::uint64_t>)];

  // Consumer's line.
  alignas(kCacheLine) std::atomic<std::uint64_t> readIdx;
  char pad2_[kCacheLine - sizeof(std::atomic<std::uint64_t>)];

  // Guard line: keeps slot 0 off the consumer's line.
  alignas(kCacheLine) char pad3_[kCacheLine];
};

static_assert(sizeof(ControlBlock) == 4 * kCacheLine, "ControlBlock layout");
static_assert(alignof(ControlBlock) == kCacheLine, "ControlBlock alignment");
static_assert(offsetof(ControlBlock, writeIdx) == kCacheLine, "writeIdx line");
static_assert(offsetof(ControlBlock, readIdx) == 2 * kCacheLine, "readIdx line");
static_assert(std::is_standard_layout<ControlBlock>::value, "must be POD-ish");

// A std::atomic that is not lock-free would be implemented with a process-local
// lock table, which the peer process cannot see. Shared memory requires the
// real thing.
static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
              "uint64 atomics must be lock-free for cross-process use");

template <typename T> class ShmSpscQueue {
  static_assert(std::is_trivially_copyable<T>::value,
                "T must be trivially copyable: shared memory cannot hold "
                "pointers, vtables or heap-owning members");
  static_assert(alignof(T) <= kCacheLine, "T over-aligned for this layout");

public:
  static constexpr std::uint64_t kMagic = 0x53484d5350534331ULL; // "SHMSPSC1"
  static constexpr std::uint32_t kVersion = 1;

  ShmSpscQueue() = default;
  ShmSpscQueue(const ShmSpscQueue &) = delete;
  ShmSpscQueue &operator=(const ShmSpscQueue &) = delete;

  // Not `= default`. A defaulted move copies ctrl_, slots_ and bound_ member
  // wise, so the moved-from handle still believes it owns the segment. Both
  // destructors then run detach(): the second clears a pid slot it no longer
  // owns and dereferences memory the first already munmapped. Swap instead, so
  // the source is left null and unbound.
  ShmSpscQueue(ShmSpscQueue &&o) noexcept { swapWith(o); }

  ShmSpscQueue &operator=(ShmSpscQueue &&o) noexcept {
    if (this != &o) {
      detach(); // release whatever this handle held first
      swapWith(o);
    }
    return *this;
  }

  ~ShmSpscQueue() { detach(); }

  void swapWith(ShmSpscQueue &o) noexcept {
    std::swap(seg_, o.seg_);
    std::swap(ctrl_, o.ctrl_);
    std::swap(slots_, o.slots_);
    std::swap(capacity_, o.capacity_);
    std::swap(role_, o.role_);
    std::swap(bound_, o.bound_);
    std::swap(readIdxCache_, o.readIdxCache_);
    std::swap(writeIdxCache_, o.writeIdxCache_);
  }

  // Throws rather than wrapping around and quietly allocating a tiny segment
  // that every index calculation would then run off the end of.
  static std::size_t bytesNeeded(std::size_t capacity) {
    constexpr std::size_t kMax = std::numeric_limits<std::size_t>::max();
    if (capacity == kMax ||
        (capacity + 1) > (kMax - sizeof(ControlBlock)) / sizeof(T)) {
      throw std::invalid_argument(
          "capacity " + std::to_string(capacity) + " x " +
          std::to_string(sizeof(T)) + " bytes overflows size_t");
    }
    return sizeof(ControlBlock) + (capacity + 1) * sizeof(T);
  }

  static ShmSpscQueue create(const std::string &name, std::size_t capacity,
                             Role role) {
    if (capacity < 1) {
      capacity = 1;
    }
    const std::uint64_t slots = static_cast<std::uint64_t>(capacity) + 1;

    ShmSpscQueue q;
    q.seg_ = ShmSegment::create(name, bytesNeeded(capacity));
    q.ctrl_ = static_cast<ControlBlock *>(q.seg_.data());

    std::memset(q.ctrl_, 0, sizeof(ControlBlock));
    q.ctrl_->magic = kMagic;
    q.ctrl_->version = kVersion;
    q.ctrl_->cacheLineSize = static_cast<std::uint32_t>(kCacheLine);
    q.ctrl_->elemSize = static_cast<std::uint32_t>(sizeof(T));
    q.ctrl_->elemAlign = static_cast<std::uint32_t>(alignof(T));
    q.ctrl_->capacity = slots;
    q.ctrl_->slotsOffset = static_cast<std::uint32_t>(sizeof(ControlBlock));
    q.ctrl_->totalBytes = q.seg_.size();
    q.ctrl_->writeIdx.store(0, std::memory_order_relaxed);
    q.ctrl_->readIdx.store(0, std::memory_order_relaxed);

    q.bind(role);
    q.ctrl_->ready.store(1, std::memory_order_release);
    return q;
  }

  // timeoutMs > 0 polls until the creator publishes `ready`.
  static ShmSpscQueue attach(const std::string &name, Role role,
                             unsigned timeoutMs = 0) {
    ShmSpscQueue q;
    for (unsigned waited = 0;; waited += 1) {
      try {
        q.seg_ = ShmSegment::open(name);
        q.ctrl_ = static_cast<ControlBlock *>(q.seg_.data());
        if (q.ctrl_->ready.load(std::memory_order_acquire) == 1) {
          break;
        }
      } catch (const std::exception &) {
        if (waited >= timeoutMs) {
          throw;
        }
      }
      if (waited >= timeoutMs) {
        throw std::runtime_error("timed out waiting for " + name);
      }
      ::usleep(1000);
    }

    q.validate();
    q.bind(role);
    return q;
  }

  bool try_push(const T &v) noexcept {
    const std::uint64_t w = ctrl_->writeIdx.load(std::memory_order_relaxed);
    std::uint64_t next = w + 1;
    if (next == capacity_) {
      next = 0;
    }
    if (next == readIdxCache_) {
      readIdxCache_ = ctrl_->readIdx.load(std::memory_order_acquire);
      if (next == readIdxCache_) {
        return false;
      }
    }
    std::memcpy(&slots_[w], &v, sizeof(T));
    ctrl_->writeIdx.store(next, std::memory_order_release);
    return true;
  }

  // Copies as many of `n` as fit. Returns how many were written.
  std::size_t push_n(const T *src, std::size_t n) noexcept {
    // Bail before the release store: republishing an unchanged index still
    // dirties the line and costs the peer a coherency miss.
    if (n == 0) {
      return 0;
    }
    const std::uint64_t w = ctrl_->writeIdx.load(std::memory_order_relaxed);
    std::uint64_t free = freeSlots(w, readIdxCache_);
    if (free < n) {
      readIdxCache_ = ctrl_->readIdx.load(std::memory_order_acquire);
      free = freeSlots(w, readIdxCache_);
      if (free == 0) {
        return 0;
      }
    }
    const std::size_t count = std::min<std::size_t>(n, free);
    const std::size_t first =
        std::min<std::size_t>(count, static_cast<std::size_t>(capacity_ - w));
    std::memcpy(&slots_[w], src, first * sizeof(T));
    if (count > first) {
      std::memcpy(&slots_[0], src + first, (count - first) * sizeof(T));
    }
    std::uint64_t next = w + count;
    if (next >= capacity_) {
      next -= capacity_;
    }
    ctrl_->writeIdx.store(next, std::memory_order_release);
    return count;
  }

  // Zero-copy peek into shared memory; nullptr when empty.
  const T *front() noexcept {
    const std::uint64_t r = ctrl_->readIdx.load(std::memory_order_relaxed);
    if (r == writeIdxCache_) {
      writeIdxCache_ = ctrl_->writeIdx.load(std::memory_order_acquire);
      if (r == writeIdxCache_) {
        return nullptr;
      }
    }
    return &slots_[r];
  }

  // Precondition: front() returned non-null. Popping an empty queue advances
  // readIdx past writeIdx and corrupts the ring, so it is checked in debug
  // builds; -DNDEBUG compiles the check away entirely.
  void pop() noexcept {
    const std::uint64_t r = ctrl_->readIdx.load(std::memory_order_relaxed);
    assert(ctrl_->writeIdx.load(std::memory_order_acquire) != r &&
           "pop() requires a preceding front() that returned non-null");
    std::uint64_t next = r + 1;
    if (next == capacity_) {
      next = 0;
    }
    ctrl_->readIdx.store(next, std::memory_order_release);
  }

  bool try_pop(T &out) noexcept {
    const T *p = front();
    if (p == nullptr) {
      return false;
    }
    std::memcpy(&out, p, sizeof(T));
    pop();
    return true;
  }

  std::size_t pop_n(T *dst, std::size_t n) noexcept {
    if (n == 0) { // see push_n
      return 0;
    }
    const std::uint64_t r = ctrl_->readIdx.load(std::memory_order_relaxed);
    std::uint64_t avail = usedSlots(writeIdxCache_, r);
    if (avail < n) {
      writeIdxCache_ = ctrl_->writeIdx.load(std::memory_order_acquire);
      avail = usedSlots(writeIdxCache_, r);
      if (avail == 0) {
        return 0;
      }
    }
    const std::size_t count = std::min<std::size_t>(n, avail);
    const std::size_t first =
        std::min<std::size_t>(count, static_cast<std::size_t>(capacity_ - r));
    std::memcpy(dst, &slots_[r], first * sizeof(T));
    if (count > first) {
      std::memcpy(dst + first, &slots_[0], (count - first) * sizeof(T));
    }
    std::uint64_t next = r + count;
    if (next >= capacity_) {
      next -= capacity_;
    }
    ctrl_->readIdx.store(next, std::memory_order_release);
    return count;
  }

  // Spin until space is available. Returns false if the peer process exited.
  bool push(const T &v, unsigned peerCheckSpins = 4096) noexcept {
    for (unsigned spins = 0;; ++spins) {
      if (try_push(v)) {
        return true;
      }
      if (spins >= peerCheckSpins) {
        if (!peerAlive()) {
          return false;
        }
        spins = 0;
      }
    }
  }

  bool pop(T &out, unsigned peerCheckSpins = 4096) noexcept {
    for (unsigned spins = 0;; ++spins) {
      if (try_pop(out)) {
        return true;
      }
      if (spins >= peerCheckSpins) {
        if (!peerAlive() && empty()) {
          return false;
        }
        spins = 0;
      }
    }
  }

  // Instantaneous estimate only. The two index loads are not atomic with
  // respect to each other: if the peer advances between them, usedSlots() can
  // take the wrap branch and report a value near capacity for a nearly empty
  // queue. Intended for diagnostics, tests and demos -- not for control flow.
  // Callers needing an exact answer must use the return value of try_push,
  // try_pop or front, which is authoritative by construction.
  std::size_t size() const noexcept {
    const std::uint64_t w = ctrl_->writeIdx.load(std::memory_order_acquire);
    const std::uint64_t r = ctrl_->readIdx.load(std::memory_order_acquire);
    return usedSlots(w, r);
  }

  // Same caveat as size(): a transiently inconsistent snapshot under concurrent
  // access. Diagnostics only; branch on front() == nullptr or try_pop instead.
  bool empty() const noexcept {
    return ctrl_->writeIdx.load(std::memory_order_acquire) ==
           ctrl_->readIdx.load(std::memory_order_acquire);
  }

  std::size_t capacity() const noexcept {
    return static_cast<std::size_t>(capacity_ - 1);
  }

  // PID reuse makes this advisory, not a guarantee.
  bool peerAlive() const noexcept {
    const std::int32_t pid =
        (role_ == Role::Producer)
            ? ctrl_->consumerPid.load(std::memory_order_relaxed)
            : ctrl_->producerPid.load(std::memory_order_relaxed);
    if (pid == 0) {
      return true; // peer has not attached yet
    }
    return processAlive(pid); // -1 (clean detach) reports dead
  }

  const ControlBlock *control() const noexcept { return ctrl_; }
  const std::string &name() const noexcept { return seg_.name(); }
  bool valid() const noexcept { return ctrl_ != nullptr; }

  // Blocks until the peer has attached. Returns false on timeout.
  bool waitForPeer(unsigned timeoutMs) const noexcept {
    for (unsigned waited = 0;; ++waited) {
      const std::int32_t pid =
          (role_ == Role::Producer)
              ? ctrl_->consumerPid.load(std::memory_order_acquire)
              : ctrl_->producerPid.load(std::memory_order_acquire);
      if (pid > 0) {
        return true;
      }
      if (waited >= timeoutMs) {
        return false;
      }
      ::usleep(1000);
    }
  }

  void detach() noexcept {
    // Only a handle that finished bind() owns a pid slot. Without this guard a
    // handle destroyed after a failed attach would clear the creator's slot.
    if (ctrl_ != nullptr && bound_) {
      auto &slot =
          (role_ == Role::Producer) ? ctrl_->producerPid : ctrl_->consumerPid;
      std::int32_t self = static_cast<std::int32_t>(::getpid());
      slot.compare_exchange_strong(self, -1, std::memory_order_release);
    }
    ctrl_ = nullptr;
    slots_ = nullptr;
    bound_ = false;
    seg_.reset();
  }

private:
  void bind(Role role) {
    role_ = role;
    capacity_ = ctrl_->capacity;
    slots_ = reinterpret_cast<T *>(reinterpret_cast<char *>(ctrl_) +
                                   ctrl_->slotsOffset);
    readIdxCache_ = ctrl_->readIdx.load(std::memory_order_acquire);
    writeIdxCache_ = ctrl_->writeIdx.load(std::memory_order_acquire);

    auto &slot =
        (role == Role::Producer) ? ctrl_->producerPid : ctrl_->consumerPid;
    const std::int32_t self = static_cast<std::int32_t>(::getpid());

    // Claiming the slot must be a single atomic step. A liveness check followed
    // by a plain store lets two processes both observe the same stale pid,
    // both judge it dead, and both store -- last writer wins and *both* believe
    // they are the sole owner. The CAS makes exactly one win; the loser re-reads
    // the winner's pid, finds it alive, and throws.
    std::int32_t expected = slot.load(std::memory_order_acquire);
    for (;;) {
      if (expected == self) {
        break; // re-attach from this same process
      }
      // 0 means never claimed, -1 a clean detach; anything else is a live
      // conflict unless the process behind it is gone.
      if (expected > 0 && processAlive(expected)) {
        throw std::runtime_error(
            std::string("a live ") +
            (role == Role::Producer ? "producer" : "consumer") +
            " is already attached (pid " + std::to_string(expected) + ")");
      }
      if (slot.compare_exchange_weak(expected, self, std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
        break;
      }
      // CAS failed: `expected` now holds the current value. Re-evaluate it --
      // a competing process may have just claimed the slot.
    }
    bound_ = true;
  }

  void validate() const {
    if (ctrl_->magic != kMagic) {
      throw std::runtime_error("bad magic: not a shmspsc segment");
    }
    if (ctrl_->version != kVersion) {
      throw std::runtime_error("version mismatch: segment v" +
                               std::to_string(ctrl_->version) + ", binary v" +
                               std::to_string(kVersion));
    }
    // A peer compiled for a different cache line size lays the control block
    // out differently -- the indices would not even be at the same offsets.
    if (ctrl_->cacheLineSize != kCacheLine) {
      throw std::runtime_error("cache line mismatch: segment " +
                               std::to_string(ctrl_->cacheLineSize) +
                               ", binary " + std::to_string(kCacheLine));
    }
    if (ctrl_->elemSize != sizeof(T) || ctrl_->elemAlign != alignof(T)) {
      throw std::runtime_error("element layout mismatch: segment " +
                               std::to_string(ctrl_->elemSize) + "/" +
                               std::to_string(ctrl_->elemAlign) + ", binary " +
                               std::to_string(sizeof(T)) + "/" +
                               std::to_string(alignof(T)));
    }
    // The kernel rounds a shm object up to a page, so the mapping is normally
    // larger than the geometry asks for. Only a mapping that is too *small* is
    // a problem.
    const std::uint64_t needed =
        ctrl_->slotsOffset + ctrl_->capacity * sizeof(T);
    if (needed > ctrl_->totalBytes || ctrl_->totalBytes > seg_.size()) {
      throw std::runtime_error(
          "segment too small: needs " + std::to_string(needed) + ", declared " +
          std::to_string(ctrl_->totalBytes) + ", mapped " +
          std::to_string(seg_.size()));
    }
  }

  std::uint64_t freeSlots(std::uint64_t w, std::uint64_t r) const noexcept {
    return (r > w ? r - w : r + capacity_ - w) - 1;
  }

  std::uint64_t usedSlots(std::uint64_t w, std::uint64_t r) const noexcept {
    return w >= r ? w - r : w + capacity_ - r;
  }

  ShmSegment seg_;
  ControlBlock *ctrl_ = nullptr;
  T *slots_ = nullptr;
  std::uint64_t capacity_ = 0;
  Role role_ = Role::Producer;
  bool bound_ = false;

  // Process-local, deliberately not in shared memory: each side caches the
  // peer's index so the common case never touches the peer's cache line.
  std::uint64_t readIdxCache_ = 0;
  std::uint64_t writeIdxCache_ = 0;
};

} // namespace shmspsc
