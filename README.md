# shmspsc

A wait-free single-producer/single-consumer ring buffer that lives in POSIX
shared memory, so the two ends are **separate processes** rather than two
threads. Header-only, C++17, no dependencies.

Sending a 48-byte message from one process to another costs **~125 ns one way**
and sustains **23 GB/s** batched — no syscall, no kernel copy, no lock.

```
   process A (producer)          process B (consumer)
        │                              │
        └──────► /dev/shm/spsc ◄───────┘
              same physical pages,
              different virtual addresses
```

## Why

The usual ways to move data between processes all go through the kernel: a pipe
or socket costs a syscall and two copies per message (~5-10 µs round trip). A
shared-memory ring buffer costs a few cache-line transfers and nothing else.
This is how market data reaches strategy processes in trading systems, and how
audio and video pipelines hand off buffers.

The queue itself is a classic SPSC ring buffer — one writer, one reader, no CAS,
no locks. What this project adds is everything required to make that work when
the two ends do not share an address space.

## The hard part: no pointers

Each process `mmap`s the segment at whatever virtual address the kernel picks.
Those addresses differ. **Any pointer stored inside the segment is meaningless
to the other process.** So the shared region holds only byte offsets and
fixed-width integers, and every pointer is derived locally:

```cpp
slots_ = reinterpret_cast<T *>(
    reinterpret_cast<char *>(ctrl_) + ctrl_->slotsOffset);
```

The same rule constrains the payload, enforced at compile time:

```cpp
static_assert(std::is_trivially_copyable<T>::value,
              "shared memory cannot hold pointers, vtables or heap-owning members");
```

A `std::string` in a message would put a heap pointer into the segment — valid
in the sender, a wild pointer in the receiver.

## Layout

Both processes compile `ControlBlock` independently, so its layout is a wire
format. Field order is chosen to leave no implicit padding, and the offsets are
locked down with `static_assert`:

```
offset 0          geometry: magic, capacity, sizes, pids, ready flag
offset 1×line     writeIdx    ← only the producer stores here
offset 2×line     readIdx     ← only the consumer stores here
offset 3×line     guard padding
offset 4×line     slots[capacity]
```

Each index gets its own cache line so a push and a pop never contend for the
same line (false sharing). The guard line keeps slot 0 off the consumer's line.
Cache line size is **128 bytes on Apple Silicon**, 64 elsewhere — and since it
changes the layout, it is written into the control block and checked on attach.

## Correctness across an address-space boundary

`attach()` refuses anything it cannot prove compatible: magic number, format
version, cache line size, `sizeof(T)`, `alignof(T)`, and segment size. A
mismatch throws instead of silently reading garbage:

```
element layout mismatch: segment 48/8, binary 8/8
```

Two further details that are easy to get wrong:

- **Atomics must be lock-free.** A non-lock-free `std::atomic` is implemented
  with a process-local lock table the peer cannot see, so it would compile and
  silently fail. `static_assert(std::atomic<uint64_t>::is_always_lock_free)`.
- **The mapping is larger than requested.** The kernel rounds a shm object up to
  a page, so the creator's requested size and the opener's `fstat` size differ.
  Validation checks `mapped >= declared`, never equality.

Ordering is the standard release/acquire pair: the producer constructs the
message, then `release`-stores the index; the consumer `acquire`-loads it. The
index store *is* the publication. This works unchanged across processes —
shared memory is coherent through the same cache hierarchy.

## Performance work

**Index caching.** Reading the peer's index on every operation drags its cache
line across cores. Each side instead keeps a stale private copy — deliberately
*not* in shared memory — and only refreshes when that copy says it must block:

```cpp
if (next == readIdxCache_) {                                   // maybe full
  readIdxCache_ = ctrl_->readIdx.load(std::memory_order_acquire);  // now check
  if (next == readIdxCache_) return false;                     // really full
}
```

The stale value errs toward "no space", never toward "space available", so being
wrong costs a refresh and never correctness. After one refresh the producer can
run through every freed slot with zero cross-core traffic.

**Batching.** `push_n` / `pop_n` move a run of items with at most two `memcpy`s
(one per side of the wrap) and a single index store, amortizing the release
store and any coherency miss across the whole batch. This is the largest single
win — **11× over item-at-a-time**.

## Results

Apple M-series, 8 cores, two processes, 48-byte messages, 5M messages per run:

| batch | ops/ms  | MB/s  | ns/op |
| ----: | ------: | ----: | ----: |
| 1     |  44,048 |  2,114 | 22.7 |
| 8     | 128,933 |  6,189 |  7.8 |
| 32    | 262,052 | 12,578 |  3.8 |
| 128   | 483,734 | 23,219 |  2.1 |

Latency, 200k round trips over two queues (unloaded):

| | p50 | p99 | p99.9 |
| --- | --: | --: | --: |
| round trip | 250 ns | 417 ns | 500 ns |
| one way    | 125 ns | 209 ns | 291 ns |

For scale: a pipe round trip on the same machine is roughly 5-10 µs, so this is
~20-40× faster. `steady_clock` costs ~18 ns to read and is *not* subtracted.

macOS exposes no thread-affinity API, so these include scheduler placement
noise; pinning cores on Linux tightens the tail considerably.

## Build and run

```sh
make            # builds everything into build/
make test       # 24k assertions incl. cross-process fuzz
make bench      # throughput + latency
make asan       # tests under AddressSanitizer + UBSan
```

Two-terminal demo — the consumer owns the segment, so start it first:

```sh
./build/demo_consumer            # terminal 1
./build/demo_producer 8000000    # terminal 2
```

```
received 8000000 messages in 0.223 s  (35941 k msg/s, 1725 MB/s)
sequence/checksum errors: 0
```

## API

```cpp
using shmspsc::ShmSpscQueue; using shmspsc::Role;

auto q = ShmSpscQueue<Msg>::create("/my_queue", 8192, Role::Consumer);
auto p = ShmSpscQueue<Msg>::attach("/my_queue", Role::Producer, 5000 /*ms*/);
```

| | |
| --- | --- |
| `try_push(v)` / `try_pop(out)` | non-blocking, `false` when full/empty |
| `push(v)` / `pop(out)` | spin until ready; `false` if the peer died |
| `push_n(src, n)` / `pop_n(dst, n)` | batched, returns how many moved |
| `front()` / `pop()` | zero-copy peek into shared memory, then release |
| `waitForPeer(ms)` | block until the other side attaches |
| `peerAlive()` | `kill(pid, 0)` liveness probe |
| `size()` `empty()` `capacity()` | |

Segment names must start with `/` and stay under 32 characters (macOS
`PSHMNAMLEN`).

## Testing

Shared-memory bugs do not reproduce in a single process, so most of the suite
`fork()`s and drives the queue from a genuinely separate address space:

- ring mechanics: wrap-around, full/empty, batch-vs-single equivalence
- attach validation: wrong element type, missing segment, name rules
- cross-process fuzz: 2M messages, every one checked for sequence gaps,
  duplicates, reordering and checksum — including through a **2-slot** queue,
  where producer and consumer collide on every single message
- lifecycle: double-attach refused, stale pid reclaimed, peer death unblocks a
  spinning `push()` instead of hanging

## Limitations

- One producer, one consumer. A second of either is refused at attach.
- `T` must be trivially copyable and self-contained.
- Both binaries must agree on `sizeof(T)`, alignment and cache line size —
  checked at attach, so a mismatch is a clean error, not corruption.
- `peerAlive()` uses `kill(pid, 0)`; PID reuse makes it advisory.
- A producer killed mid-`push_n` can leave a partial batch unpublished. The
  index only advances after the copy completes, so the reader never sees a torn
  message — the batch is simply lost.
- No huge-page support: `MAP_HUGETLB` is Linux-only.

## References

Background on the techniques used here:

- [Ring buffer](https://en.wikipedia.org/wiki/Circular_buffer)
- [False sharing](https://en.wikipedia.org/wiki/False_sharing)
- [`std::memory_order`](https://en.cppreference.com/w/cpp/atomic/memory_order)
- [POSIX `shm_open`](https://pubs.opengroup.org/onlinepubs/9699919799/functions/shm_open.html)

## License

MIT — see [LICENSE](LICENSE).
