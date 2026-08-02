// SPDX-License-Identifier: MIT
#include "shmspsc/ShmSpscQueue.h"

#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "common.h"

using demo::Message;
using shmspsc::Role;
using shmspsc::ShmSegment;
using shmspsc::ShmSpscQueue;

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool ok, const char *expr, const char *file, int line) {
  ++g_checks;
  if (!ok) {
    ++g_failures;
    std::printf("  FAIL %s:%d  %s\n", file, line, expr);
  }
}

#define CHECK(expr) check((expr), #expr, __FILE__, __LINE__)

void section(const char *name) { std::printf("[ %s ]\n", name); }

// ---------------------------------------------------------------- basics

void testGeometry() {
  section("geometry and capacity");
  auto q = ShmSpscQueue<std::uint64_t>::create("/shmspsc_t1", 7, Role::Producer);

  CHECK(q.capacity() == 7);
  CHECK(q.size() == 0);
  CHECK(q.empty());
  CHECK(q.control()->capacity == 8); // 7 usable + 1 slack
  CHECK(q.control()->cacheLineSize == shmspsc::kCacheLine);
  CHECK(q.control()->elemSize == sizeof(std::uint64_t));
  CHECK(q.control()->slotsOffset == sizeof(shmspsc::ControlBlock));
  CHECK(q.control()->totalBytes ==
        sizeof(shmspsc::ControlBlock) + 8 * sizeof(std::uint64_t));
}

void testFillAndDrain() {
  section("fill, reject when full, drain");
  auto q = ShmSpscQueue<std::uint64_t>::create("/shmspsc_t2", 4, Role::Producer);

  for (std::uint64_t i = 0; i < 4; ++i) {
    CHECK(q.try_push(i));
  }
  CHECK(q.size() == 4);
  CHECK(!q.try_push(99)); // full: the slack slot is never handed out

  std::uint64_t v = 0;
  for (std::uint64_t i = 0; i < 4; ++i) {
    CHECK(q.try_pop(v));
    CHECK(v == i);
  }
  CHECK(q.empty());
  CHECK(!q.try_pop(v));
}

void testWrapAround() {
  section("index wrap-around");
  auto q = ShmSpscQueue<std::uint64_t>::create("/shmspsc_t3", 3, Role::Producer);

  // Far more iterations than the capacity, so indices wrap many times.
  std::uint64_t v = 0;
  for (std::uint64_t i = 0; i < 10000; ++i) {
    CHECK(q.try_push(i));
    CHECK(q.try_pop(v));
    if (v != i) {
      CHECK(v == i);
      break;
    }
  }
  CHECK(q.empty());

  // Partially-full wrap: keep two in flight the whole way round.
  CHECK(q.try_push(1000));
  CHECK(q.try_push(1001));
  for (std::uint64_t i = 1002; i < 3000; ++i) {
    CHECK(q.try_push(i));
    CHECK(q.try_pop(v));
    if (v != i - 2) {
      CHECK(v == i - 2);
      break;
    }
  }
}

void testFrontPop() {
  section("front / pop");
  auto q = ShmSpscQueue<std::uint64_t>::create("/shmspsc_t4", 4, Role::Producer);

  CHECK(q.front() == nullptr);
  CHECK(q.try_push(42));

  const std::uint64_t *p = q.front();
  CHECK(p != nullptr);
  CHECK(p != nullptr && *p == 42);
  CHECK(q.front() == p); // peeking does not consume
  q.pop();
  CHECK(q.front() == nullptr);
}

// ---------------------------------------------------------------- batching

void testBatch() {
  section("push_n / pop_n");
  auto q = ShmSpscQueue<std::uint64_t>::create("/shmspsc_t5", 8, Role::Producer);

  std::uint64_t src[8];
  for (std::uint64_t i = 0; i < 8; ++i) {
    src[i] = i;
  }

  CHECK(q.push_n(src, 8) == 8);
  CHECK(q.size() == 8);
  CHECK(q.push_n(src, 1) == 0); // full

  std::uint64_t dst[8] = {};
  CHECK(q.pop_n(dst, 8) == 8);
  for (std::uint64_t i = 0; i < 8; ++i) {
    CHECK(dst[i] == i);
  }
  CHECK(q.empty());

  // Partial: ask for more than fits, get what fits.
  CHECK(q.push_n(src, 8) == 8);
  CHECK(q.pop_n(dst, 3) == 3);
  CHECK(q.push_n(src, 8) == 3);
  CHECK(q.size() == 8);
}

void testBatchWrap() {
  section("batch across the wrap boundary");
  auto q = ShmSpscQueue<std::uint64_t>::create("/shmspsc_t6", 8, Role::Producer);

  std::uint64_t src[8], dst[8];
  std::uint64_t next = 0, expect = 0;
  bool ok = true;

  // Offset the indices so every subsequent batch straddles the wrap.
  for (int i = 0; i < 5; ++i) {
    src[0] = next++;
    q.push_n(src, 1);
  }
  q.pop_n(dst, 5);
  expect = 5;

  for (int round = 0; round < 500 && ok; ++round) {
    for (std::uint64_t i = 0; i < 6; ++i) {
      src[i] = next++;
    }
    ok = ok && q.push_n(src, 6) == 6;
    ok = ok && q.pop_n(dst, 6) == 6;
    for (std::uint64_t i = 0; i < 6 && ok; ++i) {
      ok = ok && dst[i] == expect++;
    }
  }
  CHECK(ok);
  CHECK(q.empty());
}

void testBatchMatchesSingle() {
  section("batch and single-item paths agree");
  auto a = ShmSpscQueue<std::uint64_t>::create("/shmspsc_t7", 16, Role::Producer);
  auto b = ShmSpscQueue<std::uint64_t>::create("/shmspsc_t8", 16, Role::Producer);

  std::uint64_t buf[16];
  bool ok = true;
  std::uint64_t next = 0;

  for (int round = 0; round < 300 && ok; ++round) {
    const std::size_t n = 1 + static_cast<std::size_t>(round % 12);
    for (std::size_t i = 0; i < n; ++i) {
      buf[i] = next + i;
    }

    const std::size_t viaBatch = a.push_n(buf, n);
    std::size_t viaSingle = 0;
    for (std::size_t i = 0; i < n; ++i) {
      if (!b.try_push(buf[i])) {
        break;
      }
      ++viaSingle;
    }
    ok = ok && viaBatch == viaSingle && a.size() == b.size();
    next += viaBatch;

    std::uint64_t x = 0, y = 0;
    const std::size_t drain = a.size() / 2;
    for (std::size_t i = 0; i < drain && ok; ++i) {
      ok = ok && a.pop_n(&x, 1) == 1 && b.try_pop(y) && x == y;
    }
  }
  CHECK(ok);
}

// ------------------------------------------------------- attach validation

void testAttachValidation() {
  section("attach-time layout validation");

  CHECK(!ShmSegment::exists("/shmspsc_t9"));

  bool threw = false;
  try {
    ShmSpscQueue<std::uint64_t>::attach("/shmspsc_t9", Role::Consumer);
  } catch (const std::exception &) {
    threw = true;
  }
  CHECK(threw); // no such segment

  auto q = ShmSpscQueue<Message>::create("/shmspsc_t9", 4, Role::Producer);

  threw = false;
  try {
    // Same segment, wrong element type: 48 bytes vs 8.
    ShmSpscQueue<std::uint64_t>::attach("/shmspsc_t9", Role::Consumer);
  } catch (const std::exception &) {
    threw = true;
  }
  CHECK(threw);

  threw = false;
  try {
    auto c = ShmSpscQueue<Message>::attach("/shmspsc_t9", Role::Consumer);
    CHECK(c.capacity() == 4);
  } catch (const std::exception &) {
    threw = true;
  }
  CHECK(!threw); // correct type attaches cleanly

  // A failed attach must not disturb the creator's registration.
  CHECK(q.control()->producerPid.load() == static_cast<int>(::getpid()));
}

void testDoubleAttachRefused() {
  section("second live consumer is refused");
  const char *name = "/shmspsc_tb";
  auto q = ShmSpscQueue<Message>::create(name, 4, Role::Producer);

  int fds[2];
  CHECK(::pipe(fds) == 0);

  const pid_t pid = ::fork();
  if (pid == 0) {
    ::close(fds[0]);
    auto c = ShmSpscQueue<Message>::attach(name, Role::Consumer, 5000);
    char ready = 1;
    ssize_t ignored = ::write(fds[1], &ready, 1);
    (void)ignored;
    ::usleep(300000); // hold the slot while the parent tries to take it
    ::_exit(0);
  }

  ::close(fds[1]);
  char ready = 0;
  CHECK(::read(fds[0], &ready, 1) == 1);
  ::close(fds[0]);

  bool threw = false;
  try {
    ShmSpscQueue<Message>::attach(name, Role::Consumer);
  } catch (const std::exception &) {
    threw = true;
  }
  CHECK(threw);

  int status = 0;
  ::waitpid(pid, &status, 0);

  // Once that consumer exits, the stale pid is reclaimable.
  threw = false;
  try {
    auto c = ShmSpscQueue<Message>::attach(name, Role::Consumer);
    CHECK(c.capacity() == 4);
  } catch (const std::exception &) {
    threw = true;
  }
  CHECK(!threw);
}

void testNameValidation() {
  section("shm name validation");
  auto rejects = [](const std::string &name) {
    try {
      ShmSpscQueue<std::uint64_t>::create(name, 4, Role::Producer);
    } catch (const std::exception &) {
      return true;
    }
    ShmSegment::remove(name);
    return false;
  };
  CHECK(rejects("no_leading_slash"));
  CHECK(rejects("/a/b"));
  CHECK(rejects("/this_name_is_far_too_long_for_macos_limits"));
}

void testSegmentUnlinkedOnDestruct() {
  section("segment lifetime");
  {
    auto q = ShmSpscQueue<std::uint64_t>::create("/shmspsc_ta", 4,
                                                 Role::Producer);
    CHECK(ShmSegment::exists("/shmspsc_ta"));
  }
  CHECK(!ShmSegment::exists("/shmspsc_ta"));
}

// ------------------------------------------------------- move semantics

// A defaulted move copied ctrl_/slots_/bound_ member wise, leaving two handles
// each believing they owned the segment. These pin that down.
void testMoveSemantics() {
  section("move semantics");
  using Q = ShmSpscQueue<std::uint64_t>;
  const std::int32_t self = static_cast<std::int32_t>(::getpid());

  // 1. Move construction transfers ownership and nulls the source.
  {
    const char *name = "/shmspsc_move_1";
    ShmSegment::remove(name);
    auto src = Q::create(name, 4, Role::Producer);
    auto dst = std::move(src);

    CHECK(src.control() == nullptr);
    CHECK(!src.valid());
    CHECK(dst.valid());
    CHECK(dst.capacity() == 4);
    CHECK(dst.control()->producerPid.load() == self);
    ShmSegment::remove(name);
  }

  // 2. The moved-from handle destructs first; the destination stays usable.
  {
    const char *name = "/shmspsc_move_2";
    ShmSegment::remove(name);
    Q dst;
    {
      auto src = Q::create(name, 4, Role::Producer);
      dst = std::move(src);
    } // src (moved-from, now empty) destructs here

    CHECK(dst.valid());
    CHECK(dst.control()->producerPid.load() == self);
    std::uint64_t v = 0;
    CHECK(dst.try_push(7));
    CHECK(dst.try_pop(v));
    CHECK(v == 7);
    ShmSegment::remove(name);
  }

  // 3. Reverse order: destination destructs first, then the moved-from source.
  //    Locals destruct in reverse declaration order, so declaring src first
  //    makes dst die first. This is the ordering that used to segfault.
  {
    const char *name = "/shmspsc_move_3";
    ShmSegment::remove(name);
    {
      auto src = Q::create(name, 4, Role::Producer);
      auto dst = std::move(src);
      CHECK(dst.valid());
    }
    CHECK(true); // reaching here at all is the assertion
    ShmSegment::remove(name);
  }

  // 4. Move assignment releases whatever the target already held.
  {
    const char *a = "/shmspsc_move_4a";
    const char *b = "/shmspsc_move_4b";
    ShmSegment::remove(a);
    ShmSegment::remove(b);

    auto qa = Q::create(a, 4, Role::Producer);
    auto qb = Q::create(b, 8, Role::Producer);
    CHECK(ShmSegment::exists(a));
    CHECK(ShmSegment::exists(b));

    qa = std::move(qb);

    CHECK(!ShmSegment::exists(a)); // target's old segment unlinked
    CHECK(ShmSegment::exists(b));
    CHECK(qa.valid());
    CHECK(qa.capacity() == 8);
    CHECK(qa.control()->producerPid.load() == self);
    CHECK(!qb.valid());
    CHECK(qb.control() == nullptr);

    ShmSegment::remove(a);
    ShmSegment::remove(b);
  }

  // 5. Self-move-assignment is a no-op, not a self-destruct.
  {
    const char *name = "/shmspsc_move_5";
    ShmSegment::remove(name);
    auto q = Q::create(name, 4, Role::Producer);
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
#endif
    q = std::move(q); // deliberate: the guard in operator= must catch this
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
    CHECK(q.valid());
    CHECK(q.capacity() == 4);
    CHECK(q.control()->producerPid.load() == self);
    std::uint64_t v = 0;
    CHECK(q.try_push(11));
    CHECK(q.try_pop(v));
    CHECK(v == 11);
    ShmSegment::remove(name);
  }

  // 6. Queued data and the index caches survive the move.
  {
    const char *name = "/shmspsc_move_6";
    ShmSegment::remove(name);
    auto src = Q::create(name, 8, Role::Producer);
    CHECK(src.try_push(10));
    CHECK(src.try_push(20));
    CHECK(src.try_push(30));
    CHECK(src.size() == 3);

    auto dst = std::move(src);
    CHECK(dst.size() == 3);

    std::uint64_t v = 0;
    CHECK(dst.try_pop(v));
    CHECK(v == 10);
    CHECK(dst.try_pop(v));
    CHECK(v == 20);
    CHECK(dst.try_pop(v));
    CHECK(v == 30);
    CHECK(dst.empty());
    ShmSegment::remove(name);
  }
}

// -------------------------------------------------------- cross-process

// The real test: a separate process, its own address space, its own mapping.
void testCrossProcess(std::uint64_t total, std::size_t capacity) {
  section("cross-process fuzz");
  std::printf("  %llu messages through a %zu-slot queue\n",
              static_cast<unsigned long long>(total), capacity);

  const char *name = "/shmspsc_x1";
  auto q = ShmSpscQueue<Message>::create(name, capacity, Role::Producer);

  const pid_t pid = ::fork();
  if (pid == 0) {
    // Child: consumer. _exit() so no inherited destructor unlinks the segment.
    int rc = 0;
    try {
      auto c = ShmSpscQueue<Message>::attach(name, Role::Consumer, 5000);
      Message m{};
      for (std::uint64_t expect = 0; expect < total; ++expect) {
        if (!c.pop(m)) {
          rc = 2; // producer vanished
          break;
        }
        if (m.seq != expect || m.checksum != demo::checksumOf(m)) {
          rc = 3; // gap, duplicate, reorder or torn write
          break;
        }
      }
    } catch (const std::exception &) {
      rc = 4;
    }
    ::_exit(rc);
  }

  CHECK(pid > 0);
  // Without this the parent would spin forever if the child never attached.
  const bool peer = q.waitForPeer(5000);
  CHECK(peer);

  bool pushed = peer;
  for (std::uint64_t i = 0; i < total && pushed; ++i) {
    pushed = q.push(demo::makeMessage(i, 0));
  }
  CHECK(pushed);

  int status = 0;
  ::waitpid(pid, &status, 0);
  CHECK(WIFEXITED(status));
  const int rc = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
  if (rc != 0) {
    std::printf("  consumer exit code %d (2=producer gone, 3=data mismatch, "
                "4=attach failed)\n",
                rc);
  }
  CHECK(rc == 0);
}

void testCrossProcessBatched(std::uint64_t total, std::size_t capacity) {
  section("cross-process fuzz, batched");
  const char *name = "/shmspsc_x2";
  auto q = ShmSpscQueue<Message>::create(name, capacity, Role::Producer);

  const pid_t pid = ::fork();
  if (pid == 0) {
    int rc = 0;
    try {
      auto c = ShmSpscQueue<Message>::attach(name, Role::Consumer, 5000);
      std::vector<Message> buf(64);
      std::uint64_t expect = 0;
      while (expect < total) {
        const std::size_t n = c.pop_n(buf.data(), buf.size());
        if (n == 0) {
          if (!c.peerAlive() && c.empty()) {
            rc = 2;
            break;
          }
          continue;
        }
        for (std::size_t i = 0; i < n; ++i) {
          if (buf[i].seq != expect++ ||
              buf[i].checksum != demo::checksumOf(buf[i])) {
            rc = 3;
            break;
          }
        }
        if (rc != 0) {
          break;
        }
      }
    } catch (const std::exception &) {
      rc = 4;
    }
    ::_exit(rc);
  }

  CHECK(pid > 0);
  const bool peer = q.waitForPeer(5000);
  CHECK(peer);

  std::vector<Message> buf(64);
  std::uint64_t sent = 0;
  while (peer && sent < total) {
    const std::size_t want =
        std::min<std::size_t>(buf.size(), static_cast<std::size_t>(total - sent));
    for (std::size_t i = 0; i < want; ++i) {
      buf[i] = demo::makeMessage(sent + i, 0);
    }
    std::size_t done = 0;
    while (done < want) {
      const std::size_t n = q.push_n(buf.data() + done, want - done);
      if (n == 0 && !q.peerAlive()) {
        break;
      }
      done += n;
    }
    if (done < want) {
      break;
    }
    sent += want;
  }
  CHECK(sent == total);

  int status = 0;
  ::waitpid(pid, &status, 0);
  const int rc = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
  if (rc != 0) {
    std::printf("  consumer exit code %d\n", rc);
  }
  CHECK(rc == 0);
}

void testPeerDeath() {
  section("peer death detection");
  const char *name = "/shmspsc_x3";
  auto q = ShmSpscQueue<Message>::create(name, 4, Role::Producer);

  const pid_t pid = ::fork();
  if (pid == 0) {
    auto c = ShmSpscQueue<Message>::attach(name, Role::Consumer, 5000);
    ::_exit(0); // attach, then die immediately
  }

  int status = 0;
  ::waitpid(pid, &status, 0);

  // The queue holds 4; the 5th push blocks forever unless death is detected.
  for (int i = 0; i < 4; ++i) {
    CHECK(q.try_push(demo::makeMessage(static_cast<std::uint64_t>(i), 0)));
  }
  CHECK(!q.push(demo::makeMessage(99, 0))); // returns false instead of hanging
}

} // namespace

int main(int argc, char **argv) {
  // Line buffering keeps progress visible when stdout is a pipe, and stops a
  // forked child from inheriting and re-flushing the parent's buffer.
  std::setvbuf(stdout, nullptr, _IOLBF, 0);

  const std::uint64_t total =
      (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 2000000;

  std::printf("shmspsc tests  (cache line %zu, ControlBlock %zu bytes, "
              "Message %zu bytes)\n\n",
              shmspsc::kCacheLine, sizeof(shmspsc::ControlBlock),
              sizeof(Message));

  testGeometry();
  testFillAndDrain();
  testWrapAround();
  testFrontPop();
  testBatch();
  testBatchWrap();
  testBatchMatchesSingle();
  testAttachValidation();
  testDoubleAttachRefused();
  testNameValidation();
  testSegmentUnlinkedOnDestruct();
  testMoveSemantics();
  testCrossProcess(total, 1024);
  testCrossProcess(total / 4, 2); // pathologically small: constant contention
  testCrossProcessBatched(total, 1024);
  testPeerDeath();

  std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
  return g_failures == 0 ? 0 : 1;
}
