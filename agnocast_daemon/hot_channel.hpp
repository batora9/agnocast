// SPDX-License-Identifier: Apache-2.0
#pragma once

// Process-shared request/response slot for the daemon hot path.
//
// After the Unix-socket handshake the daemon hands the client a memfd mapping
// of this struct. Subsequent commands are posted here instead of sendmsg/recvmsg
// so the caller can spin for the reply without leaving the CPU (the p99.9 source
// of the UDS path is that recvmsg sleep).

#include "protocol.h"

#include <linux/futex.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <ctime>

// Covers ReceiveMsgResponse (~25 KB) and GetTopicSubscriberInfoResponse (~811 KB).
#define AGNOCAST_HOT_PAYLOAD_SIZE (1024 * 1024)

// Brief spin so a daemon already on-core is caught without a futex sleep.
// Must stay far below a timeslice: a 100 ms client spin at SCHED_FIFO 80
// starved the other subscribers and blew e2e p50 from ~0.3 ms to ~2 ms at S=32.
#define AGNOCAST_HOT_CLIENT_SPIN_NS (20000LL)  // 20 us

// Do not spin for the next request after a reply. The daemon is FIFO 90; a
// 50 us post-reply spin held the core so FIFO 80 clients could not observe
// response_seq (~50 us tax on every IPC, and e2e convoy).
#define AGNOCAST_HOT_DAEMON_SPIN_NS (0LL)

struct HotChannel
{
  alignas(64) std::atomic<uint32_t> request_seq;
  alignas(64) std::atomic<uint32_t> response_seq;
  uint32_t command;
  uint32_t request_size;
  int32_t error_code;
  uint32_t response_size;
  int64_t recv_ns;
  int64_t lock_begin_ns;
  int64_t work_ns;
  int64_t send_ns;
  uint8_t payload[AGNOCAST_HOT_PAYLOAD_SIZE];
};

inline void hot_cpu_relax()
{
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

inline int64_t hot_now_ns()
{
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
}

inline int hot_futex_wait(std::atomic<uint32_t> * addr, uint32_t expected, const timespec * timeout)
{
  return static_cast<int>(syscall(
    SYS_futex, reinterpret_cast<uint32_t *>(addr), FUTEX_WAIT, expected, timeout, nullptr, 0));
}

inline int hot_futex_wake(std::atomic<uint32_t> * addr, int nwaiters)
{
  return static_cast<int>(syscall(
    SYS_futex, reinterpret_cast<uint32_t *>(addr), FUTEX_WAKE, nwaiters, nullptr, nullptr, 0));
}

// Returns true if *seq became `want` within spin_ns.
inline bool hot_spin_until(std::atomic<uint32_t> * seq, uint32_t want, int64_t spin_ns)
{
  const int64_t start = hot_now_ns();
  while (seq->load(std::memory_order_acquire) != want) {
    if (hot_now_ns() - start >= spin_ns) {
      return seq->load(std::memory_order_acquire) == want;
    }
    hot_cpu_relax();
  }
  return true;
}

// Spin briefly, then FUTEX_WAIT until *seq == want or timeout_ns elapses.
inline bool hot_wait_until(
  std::atomic<uint32_t> * seq, uint32_t want, int64_t spin_ns, int64_t timeout_ns)
{
  if (hot_spin_until(seq, want, spin_ns)) {
    return true;
  }
  const int64_t deadline = hot_now_ns() + timeout_ns;
  while (true) {
    const uint32_t cur = seq->load(std::memory_order_acquire);
    if (cur == want) {
      return true;
    }
    const int64_t remain = deadline - hot_now_ns();
    if (remain <= 0) {
      return seq->load(std::memory_order_acquire) == want;
    }
    timespec ts{};
    ts.tv_sec = remain / 1000000000LL;
    ts.tv_nsec = remain % 1000000000LL;
    hot_futex_wait(seq, cur, &ts);
  }
}
