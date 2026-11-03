// SPDX-License-Identifier: Apache-2.0
#ifdef AGNOCAST_USE_DAEMON

#include "agnocast/agnocast_bench_timing.hpp"
#include "agnocast/agnocast_ipc.hpp"
#include "agnocast/agnocast_utils.hpp"
#include "protocol.h"
#include "topic_shm.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace agnocast
{

static std::mutex socket_mtx;

#ifdef AGNOCAST_BENCH_TIMING
namespace
{

// Stamps of the round trip daemon_call() performed most recently on this thread.
// Read straight afterwards by finalize_breakdown(), before another call can
// overwrite them.
struct CallStamps
{
  int64_t send_begin;   // client, immediately before sendmsg
  int64_t send_end;     // client, immediately after sendmsg
  int64_t recv_end;     // client, immediately after recvmsg
  int64_t daemon_recv;  // daemon stamps, 0 when the response carried no trailer
  int64_t daemon_work;
  int64_t daemon_notify_begin;
  int64_t daemon_notify_end;
  int64_t daemon_send;
};

thread_local CallStamps g_stamps = {};

// Splits [enter_ns, exit_ns] using the stamps of the call that just completed.
void finalize_breakdown(IpcBreakdown & out, int64_t enter_ns, int64_t exit_ns)
{
  const CallStamps s = g_stamps;

  out = IpcBreakdown{};
  out.total_ns = exit_ns - enter_ns;
  out.req_ns = s.send_begin - enter_ns;
  out.send_syscall_ns = s.send_end - s.send_begin;
  out.post_ns = exit_ns - s.recv_end;

  if (s.daemon_recv == 0 || s.daemon_send == 0) {
    // No trailer: keep the segments summing to the total by attributing the
    // whole socket round trip to up_ns.
    out.up_ns = s.recv_end - s.send_begin;
    return;
  }

  out.daemon_stamped = 1;
  out.up_ns = s.daemon_recv - s.send_begin;
  out.down_ns = s.recv_end - s.daemon_send;
  const int64_t work_begin = (s.daemon_work != 0) ? s.daemon_work : s.daemon_recv;
  const int64_t work_end = (s.daemon_notify_begin != 0) ? s.daemon_notify_begin : s.daemon_send;
  out.lock_ns = work_begin - s.daemon_recv;
  out.work_ns = work_end - work_begin;
  out.notify_ns = s.daemon_send - work_end;
}

// For a call served from the topic segment without a round trip. There are no
// daemon stamps; lock_ns/work_ns/notify_ns are the segment mutex wait, the
// critical section, and the eventfd writes.
void finalize_local_breakdown(
  IpcBreakdown & out, int64_t enter_ns, int64_t lock_begin_ns, int64_t lock_end_ns,
  int64_t work_end_ns, int64_t notify_end_ns, int64_t exit_ns)
{
  out = IpcBreakdown{};
  out.total_ns = exit_ns - enter_ns;
  out.req_ns = lock_begin_ns - enter_ns;
  out.lock_ns = lock_end_ns - lock_begin_ns;
  out.work_ns = work_end_ns - lock_end_ns;
  out.notify_ns = notify_end_ns - work_end_ns;
  out.post_ns = exit_ns - notify_end_ns;
}

}  // namespace

// The macros expand inside namespace agnocast, so unqualified lookup reaches
// the finalizers in the unnamed namespace above.
#define AGNOCAST_BENCH_CALL_ENTER() const int64_t agnocast_bench_enter_ns = bench_timing_now_ns()
#define AGNOCAST_BENCH_CALL_LEAVE(slot) \
  finalize_breakdown(last_##slot##_ipc_breakdown, agnocast_bench_enter_ns, bench_timing_now_ns())
#define AGNOCAST_BENCH_LOCAL_STAMP(var) var = bench_timing_now_ns()
#define AGNOCAST_BENCH_CALL_LEAVE_LOCAL(slot, stamps)                                             \
  finalize_local_breakdown(                                                                       \
    last_##slot##_ipc_breakdown, agnocast_bench_enter_ns, (stamps).lock_begin, (stamps).lock_end, \
    (stamps).work_end, (stamps).notify_end, bench_timing_now_ns())
#else
#define AGNOCAST_BENCH_CALL_ENTER() static_cast<void>(0)
#define AGNOCAST_BENCH_CALL_LEAVE(slot) static_cast<void>(0)
#define AGNOCAST_BENCH_LOCAL_STAMP(var) static_cast<void>(0)
#define AGNOCAST_BENCH_CALL_LEAVE_LOCAL(slot, stamps) static_cast<void>(0)
#endif  // AGNOCAST_BENCH_TIMING

namespace
{

// Stamps of a call served from the topic segment (bench builds only).
struct LocalStamps
{
  int64_t lock_begin = 0;
  int64_t lock_end = 0;
  int64_t work_end = 0;
  int64_t notify_end = 0;
};

}  // namespace

// Client-side wait for the daemon reply. Always tries MSG_DONTWAIT once so a
// datagram already in the socket buffer does not require a blocking recvmsg
// wakeup. Optional busy-wait (pause + DONTWAIT) is AIMD-capped, disabled, or
// a fixed window via AGNOCAST_UDS_RECV_SPIN_NS.
namespace
{

constexpr int64_t kAdaptiveSpinCapNs = 50000;  // 50 us; stay far below a timeslice
constexpr int64_t kAdaptiveSpinAddNs = 2000;   // 2 us
constexpr int64_t kAdaptiveSpinFloorNs = 1000;

enum class RecvSpinMode {
  Adaptive,
  Fixed,
  Disabled,
};

struct RecvSpinConfig
{
  RecvSpinMode mode = RecvSpinMode::Adaptive;
  int64_t fixed_ns = 0;
};

void cpu_relax()
{
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

int64_t monotonic_now_ns()
{
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
}

RecvSpinConfig load_recv_spin_config()
{
  const char * v = std::getenv("AGNOCAST_UDS_RECV_SPIN_NS");
  if (v == nullptr || v[0] == '\0') {
    return {};
  }
  char * end = nullptr;
  const long long n = std::strtoll(v, &end, 10);
  if (end == v || *end != '\0' || n < 0) {
    RCLCPP_WARN(
      logger, "AGNOCAST_UDS_RECV_SPIN_NS='%s' is invalid; using adaptive spin (cap %lld ns)", v,
      static_cast<long long>(kAdaptiveSpinCapNs));
    return {};
  }
  RecvSpinConfig cfg{};
  if (n == 0) {
    cfg.mode = RecvSpinMode::Disabled;
  } else {
    cfg.mode = RecvSpinMode::Fixed;
    cfg.fixed_ns = static_cast<int64_t>(n);
  }
  return cfg;
}

const RecvSpinConfig & recv_spin_config()
{
  static const RecvSpinConfig cfg = load_recv_spin_config();
  return cfg;
}

void adaptive_on_hit(int64_t & budget)
{
  budget = std::min(kAdaptiveSpinCapNs, budget + kAdaptiveSpinAddNs);
}

void adaptive_on_spin_miss(int64_t & budget)
{
  budget /= 2;
  if (budget < kAdaptiveSpinFloorNs) {
    budget = 0;
  }
}

thread_local int64_t t_spin_budget_ns = 0;

ssize_t recvmsg_dontwait(int fd, msghdr * msg)
{
  while (true) {
    const ssize_t n = recvmsg(fd, msg, MSG_DONTWAIT | MSG_CMSG_CLOEXEC);
    if (n < 0 && errno == EINTR) {
      continue;
    }
    return n;
  }
}

// 0 on a complete datagram, -1 on error (errno set).
int recv_response(msghdr * recv_msg)
{
  const RecvSpinConfig & cfg = recv_spin_config();

  // A zero-length read is the daemon closing the connection: every reply
  // carries at least its header.
  ssize_t n = recvmsg_dontwait(agnocast_fd, recv_msg);
  if (n == 0) {
    errno = ECONNRESET;
    return -1;
  }
  if (n > 0) {
    if (cfg.mode == RecvSpinMode::Adaptive) {
      adaptive_on_hit(t_spin_budget_ns);
    }
    return 0;
  }
  if (errno != EAGAIN && errno != EWOULDBLOCK) {
    return -1;
  }

  int64_t budget = 0;
  if (cfg.mode == RecvSpinMode::Fixed) {
    budget = cfg.fixed_ns;
  } else if (cfg.mode == RecvSpinMode::Adaptive) {
    budget = t_spin_budget_ns;
  }

  const bool spun = budget > 0;
  if (spun) {
    const int64_t start = monotonic_now_ns();
    while (monotonic_now_ns() - start < budget) {
      cpu_relax();
      n = recvmsg_dontwait(agnocast_fd, recv_msg);
      if (n == 0) {
        errno = ECONNRESET;
        return -1;
      }
      if (n > 0) {
        if (cfg.mode == RecvSpinMode::Adaptive) {
          adaptive_on_hit(t_spin_budget_ns);
        }
        return 0;
      }
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        return -1;
      }
    }
  }

  const int64_t block_begin = monotonic_now_ns();
  n = recvmsg(agnocast_fd, recv_msg, MSG_CMSG_CLOEXEC);
  if (n < 0) {
    return -1;
  }
  if (n == 0) {
    errno = ECONNRESET;
    return -1;
  }
  if (cfg.mode == RecvSpinMode::Adaptive) {
    if (spun) {
      adaptive_on_spin_miss(t_spin_budget_ns);
    } else if (monotonic_now_ns() - block_begin < kAdaptiveSpinCapNs) {
      // Budget was 0 so we never spun; a short blocking wait means a small
      // window would have hit. Grow so the next call can busy-wait.
      adaptive_on_hit(t_spin_budget_ns);
    }
  }
  return 0;
}

}  // namespace

// Returns 0 on success, sets errno on failure.
//
// send_fd, when >= 0, is attached to the request with SCM_RIGHTS. The kernel
// module reads a subscriber's notification eventfd out of the caller's own fd
// table, which a userspace daemon cannot do, so the descriptor itself has to
// travel with the registration. Ownership moves to the daemon on success.
//
// Descriptors the daemon attaches to the reply are stored in recv_fds (up to
// recv_fd_cap, the rest closed) and counted in *recv_fd_num; the caller owns
// them, even when the call itself reports an error.
//
// Caller holds socket_mtx. *transport_failed tells a broken connection (the
// daemon is gone) apart from an error the daemon replied with.
namespace
{

bool is_transport_error(int err)
{
  return err == EPIPE || err == ECONNRESET || err == ENOTCONN || err == ECONNREFUSED;
}

}  // namespace

static int daemon_call_locked(
  uint32_t cmd, const void * req_payload, uint32_t req_size, void * resp_payload,
  uint32_t resp_size, int send_fd, int * recv_fds, uint32_t recv_fd_cap, uint32_t * recv_fd_num,
  bool * transport_failed)
{
  *transport_failed = false;
  if (recv_fd_num != nullptr) *recv_fd_num = 0;

  // --- send request ---
  RequestHeader req_hdr{cmd, req_size};
  iovec iov[2];
  iov[0] = {&req_hdr, sizeof(req_hdr)};
  iov[1] = {const_cast<void *>(req_payload), req_size};
  msghdr send_msg{};
  send_msg.msg_iov = iov;
  send_msg.msg_iovlen = (req_size > 0) ? 2 : 1;

  union {
    cmsghdr align;
    char bytes[CMSG_SPACE(sizeof(int))];
  } cmsg_buf{};
  if (send_fd >= 0) {
    send_msg.msg_control = cmsg_buf.bytes;
    send_msg.msg_controllen = sizeof(cmsg_buf.bytes);
    cmsghdr * cmsg = CMSG_FIRSTHDR(&send_msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &send_fd, sizeof(send_fd));
  }
#ifdef AGNOCAST_BENCH_TIMING
  g_stamps = CallStamps{};
  g_stamps.send_begin = bench_timing_now_ns();
#endif
  if (sendmsg(agnocast_fd, &send_msg, MSG_NOSIGNAL) < 0) {
    *transport_failed = is_transport_error(errno);
    return -1;
  }
#ifdef AGNOCAST_BENCH_TIMING
  g_stamps.send_end = bench_timing_now_ns();
#endif

  // --- receive response ---
  ResponseHeader resp_hdr{};
  iovec riov[3];
  int n_riov = 0;
  riov[n_riov++] = {&resp_hdr, sizeof(resp_hdr)};
  if (resp_size > 0 && resp_payload != nullptr) {
    riov[n_riov++] = {resp_payload, resp_size};
  }
#ifdef AGNOCAST_BENCH_TIMING
  // Filled only if the daemon appends a trailer; stays zeroed otherwise, which
  // finalize_breakdown() detects through the magic value.
  BenchTimingTrailer trailer{};
  riov[n_riov++] = {&trailer, sizeof(trailer)};
#endif
  msghdr recv_msg{};
  recv_msg.msg_iov = riov;
  recv_msg.msg_iovlen = static_cast<size_t>(n_riov);
  union {
    cmsghdr align;
    char bytes[CMSG_SPACE(sizeof(int) * AGNOCAST_SUBSCRIBER_EVENTFD_BATCH)];
  } recv_cmsg_buf{};
  if (recv_fds != nullptr) {
    recv_msg.msg_control = recv_cmsg_buf.bytes;
    recv_msg.msg_controllen = sizeof(recv_cmsg_buf.bytes);
  }
  if (recv_response(&recv_msg) < 0) {
    *transport_failed = is_transport_error(errno);
    return -1;
  }
  if (recv_fds != nullptr) {
    for (cmsghdr * c = CMSG_FIRSTHDR(&recv_msg); c != nullptr; c = CMSG_NXTHDR(&recv_msg, c)) {
      if (c->cmsg_level != SOL_SOCKET || c->cmsg_type != SCM_RIGHTS) continue;
      const size_t fd_num = (c->cmsg_len - CMSG_LEN(0)) / sizeof(int);
      for (size_t i = 0; i < fd_num; i++) {
        int got = -1;
        memcpy(&got, CMSG_DATA(c) + i * sizeof(int), sizeof(got));
        if (*recv_fd_num < recv_fd_cap) {
          recv_fds[(*recv_fd_num)++] = got;
        } else {
          close(got);
        }
      }
    }
  }
#ifdef AGNOCAST_BENCH_TIMING
  g_stamps.recv_end = bench_timing_now_ns();
  if (trailer.magic == AGNOCAST_BENCH_TRAILER_MAGIC) {
    g_stamps.daemon_recv = trailer.recv_ns;
    g_stamps.daemon_work = trailer.work_ns;
    g_stamps.daemon_notify_begin = trailer.notify_begin_ns;
    g_stamps.daemon_notify_end = trailer.notify_end_ns;
    g_stamps.daemon_send = trailer.send_ns;
  }
#endif

  if (resp_hdr.error_code != 0) {
    errno = resp_hdr.error_code;
    return -1;
  }
  return 0;
}

// ============================================================
// Surviving a daemon crash
// ============================================================
//
// The message data plane runs on the topic segments, which this process keeps
// mapped, so publish, receive and release go on while the daemon is down. What
// needs the daemon (registration, queries, looking up a new publisher's
// memory) waits for it to come back: when the connection breaks, one thread
// reconnects, replays what this process registered (restore_locked), and
// repeats the call that failed. A call the daemon committed but could not
// answer is not applied twice: the restarted daemon removes what no client
// reclaimed, so the repeated call starts from a state without it.
//
//   AGNOCAST_DAEMON_RECONNECT=0               fail as before instead
//   AGNOCAST_DAEMON_RECONNECT_TIMEOUT_MS=<n>  how long a call waits (default 60000)

namespace
{

struct CallOptions
{
  // Wait for the daemon to come back and repeat the call when the connection
  // breaks. Off for callers that can do without an answer for now.
  bool wait_for_daemon = true;
  // Runs under socket_mtx right after a successful reply, so that a restore
  // cannot slip in between the daemon committing a registration and this
  // process recording it.
  void (*on_success)(void *) = nullptr;
  void * ctx = nullptr;
};

std::atomic<uint64_t> g_conn_generation{0};
std::atomic<uint64_t> g_reconnect_count{0};
std::mutex g_recover_mtx;
std::condition_variable g_recover_cv;
bool g_recovering = false;

bool reconnect_enabled()
{
  static const bool enabled = [] {
    const char * v = std::getenv("AGNOCAST_DAEMON_RECONNECT");
    return v == nullptr || std::strcmp(v, "0") != 0;
  }();
  return enabled;
}

int64_t reconnect_timeout_ms()
{
  static const int64_t ms = [] {
    const char * v = std::getenv("AGNOCAST_DAEMON_RECONNECT_TIMEOUT_MS");
    if (v == nullptr || v[0] == '\0') return int64_t{60000};
    const long long n = std::strtoll(v, nullptr, 10);
    return n > 0 ? static_cast<int64_t>(n) : int64_t{60000};
  }();
  return ms;
}

int open_daemon_socket()
{
  const int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (fd < 0) return -1;
  const int buf = AGNOCAST_DAEMON_SOCKET_BUF_SIZE;
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
  setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, AGNOCAST_DAEMON_SOCKET_PATH, sizeof(addr.sun_path) - 1);
  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }
  return fd;
}

bool restore_locked();

// Connects to the daemon until deadline_ns (< 0: forever) and replays this
// process's registrations over the new connection.
bool reconnect_and_restore(int64_t deadline_ns)
{
  const int64_t begin = monotonic_now_ns();
  RCLCPP_WARN(logger, "lost the connection to agnocast_daemon; reconnecting");
  while (true) {
    const int fd = open_daemon_socket();
    if (fd >= 0) {
      std::lock_guard<std::mutex> lock(socket_mtx);
      if (agnocast_fd < 0) {
        close(fd);
        return false;
      }
      // Keep the descriptor number: others hold agnocast_fd by value.
      if (fd != agnocast_fd && dup3(fd, agnocast_fd, O_CLOEXEC) < 0) {
        close(fd);
        return false;
      }
      if (fd != agnocast_fd) close(fd);
      if (restore_locked()) {
        g_reconnect_count.fetch_add(1);
        RCLCPP_INFO(
          logger, "reconnected to agnocast_daemon and restored registrations in %.3f ms",
          static_cast<double>(monotonic_now_ns() - begin) / 1e6);
        return true;
      }
    }
    if (deadline_ns >= 0 && monotonic_now_ns() >= deadline_ns) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
}

// Called by a thread whose call broke on connection generation failed_gen.
// True once a newer connection is in place. With wait false it only reports
// whether one already is.
bool recover_connection(uint64_t failed_gen, bool wait, int64_t timeout_ms)
{
  std::unique_lock<std::mutex> lk(g_recover_mtx);
  if (g_conn_generation.load() != failed_gen) return true;
  if (!wait) return false;
  const int64_t deadline = timeout_ms < 0 ? -1 : monotonic_now_ns() + timeout_ms * 1000000LL;
  if (g_recovering) {
    auto done = [&] { return g_conn_generation.load() != failed_gen || !g_recovering; };
    if (timeout_ms < 0) {
      g_recover_cv.wait(lk, done);
    } else {
      g_recover_cv.wait_for(lk, std::chrono::milliseconds(timeout_ms), done);
    }
    return g_conn_generation.load() != failed_gen;
  }
  g_recovering = true;
  lk.unlock();
  const bool ok = reconnect_and_restore(deadline);
  lk.lock();
  g_recovering = false;
  if (ok) g_conn_generation.fetch_add(1);
  g_recover_cv.notify_all();
  return ok;
}

// Notices the daemon going away even while nothing here needs it, so the
// restarted daemon gets this process's registrations back promptly.
void watcher_main()
{
  while (true) {
    const uint64_t gen = g_conn_generation.load();
    const int fd = agnocast_fd;
    if (fd < 0) return;
    pollfd p{fd, POLLRDHUP, 0};
    const int r = poll(&p, 1, -1);
    if (r < 0) {
      if (errno == EINTR) continue;
      return;
    }
    if (p.revents & POLLNVAL) return;  // closed: the process is shutting down
    if ((p.revents & (POLLHUP | POLLRDHUP | POLLERR)) && g_conn_generation.load() == gen) {
      recover_connection(gen, true, -1);
    }
  }
}

// Started with the first publisher or subscriber, when the heap is usable.
void ensure_watcher()
{
  static std::atomic<pid_t> started_in{0};
  if (!reconnect_enabled()) return;
  const pid_t self = getpid();
  pid_t expected = started_in.load();
  if (expected == self) return;
  if (!started_in.compare_exchange_strong(expected, self)) return;
  std::thread(watcher_main).detach();
}

}  // namespace

// Returns 0 on success, sets errno on failure. See daemon_call_locked.
static int daemon_call(
  uint32_t cmd, const void * req_payload, uint32_t req_size, void * resp_payload,
  uint32_t resp_size, int send_fd = -1, int * recv_fds = nullptr, uint32_t recv_fd_cap = 0,
  uint32_t * recv_fd_num = nullptr, const CallOptions * opts = nullptr)
{
  static const CallOptions kDefault{};
  const CallOptions & o = (opts != nullptr) ? *opts : kDefault;
  while (true) {
    const uint64_t gen = g_conn_generation.load();
    bool transport_failed = false;
    int r = 0;
    int saved_errno = 0;
    {
      std::lock_guard<std::mutex> lock(socket_mtx);
      r = daemon_call_locked(
        cmd, req_payload, req_size, resp_payload, resp_size, send_fd, recv_fds, recv_fd_cap,
        recv_fd_num, &transport_failed);
      saved_errno = errno;
      if (r == 0 && o.on_success != nullptr) o.on_success(o.ctx);
    }
    if (r == 0 || !transport_failed || !reconnect_enabled()) {
      errno = saved_errno;
      return r;
    }
    if (!recover_connection(gen, o.wait_for_daemon, reconnect_timeout_ms())) {
      errno = saved_errno;
      return -1;
    }
  }
}

uint64_t daemon_reconnect_count()
{
  return g_reconnect_count.load();
}

// Helper to copy a name_info string into a fixed-size char array (null-terminated).
static void copy_name(char * dst, size_t dst_size, const struct name_info & src)
{
  size_t len = (src.len < dst_size - 1) ? src.len : (dst_size - 1);
  if (src.ptr != nullptr) {
    std::memcpy(dst, src.ptr, len);
  }
  dst[len] = '\0';
}

static void copy_name_out(char * dst, size_t dst_size, const char * src)
{
  std::strncpy(dst, src, dst_size - 1);
  dst[dst_size - 1] = '\0';
}

// The topic's shared segment (topic_shm.hpp), handed over with each publisher
// and subscriber registration.
namespace
{

struct TopicSegment
{
  AgnocastTopicShm * shm = nullptr;
  // Kept open so the segment can be handed to a restarted daemon.
  int fd = -1;
  std::string topic_name;

  // A publisher's copies of the subscribers' eventfds, valid for
  // notify_generation of shm->sub_generation.
  std::mutex notify_mtx;
  uint64_t notify_generation = UINT64_MAX;
  std::unordered_map<int32_t, int> notify_fds;

  TopicSegment() = default;
  ~TopicSegment()
  {
    for (const auto & [id, efd] : notify_fds) {
      (void)id;
      close(efd);
    }
    if (shm != nullptr) munmap(shm, sizeof(*shm));
    if (fd >= 0) close(fd);
  }
  TopicSegment(const TopicSegment &) = delete;
  TopicSegment & operator=(const TopicSegment &) = delete;
};

// Keyed by (topic, publisher or subscriber id). Held by shared_ptr so a thread
// still using a segment keeps it mapped after the handle is removed; the
// by-inode map lets every handle on one topic share one mapping.
//
// Lock ordering: socket_mtx → segments_mtx → registry_mtx.
std::mutex segments_mtx;
std::unordered_map<std::string, std::shared_ptr<TopicSegment>> segments;
std::unordered_map<ino_t, std::weak_ptr<TopicSegment>> segments_by_inode;

std::string segment_key(const struct name_info & topic_name, int32_t pubsub_id)
{
  std::string key(topic_name.ptr != nullptr ? topic_name.ptr : "", topic_name.len);
  key.push_back('\0');
  key.append(std::to_string(pubsub_id));
  return key;
}

std::shared_ptr<TopicSegment> find_segment(const struct name_info & topic_name, int32_t pubsub_id)
{
  const std::string key = segment_key(topic_name, pubsub_id);
  std::lock_guard<std::mutex> lock(segments_mtx);
  const auto it = segments.find(key);
  return (it != segments.end()) ? it->second : nullptr;
}

// AGNOCAST_TOPIC_SHM=0 keeps every call on the daemon request path, so one
// build can be measured both ways. Such a client cannot be restored after a
// daemon crash: restoring hands the segments back.
bool topic_shm_enabled()
{
  static const bool enabled = [] {
    const char * v = std::getenv("AGNOCAST_TOPIC_SHM");
    return v == nullptr || std::strcmp(v, "0") != 0;
  }();
  return enabled;
}

// Takes ownership of shm_fd. A handle without a segment still works over plain
// requests, so failing here is reported and otherwise ignored.
void attach_topic_segment(const struct name_info & topic_name, int32_t pubsub_id, int shm_fd)
{
  if (shm_fd < 0) return;
  struct FdCloser
  {
    int fd;
    ~FdCloser()
    {
      if (fd >= 0) close(fd);
    }
  } closer{shm_fd};
  if (!topic_shm_enabled()) return;

  struct stat st
  {
  };
  if (fstat(shm_fd, &st) < 0 || st.st_size != static_cast<off_t>(sizeof(AgnocastTopicShm))) {
    RCLCPP_WARN(logger, "unexpected topic segment from the daemon; using plain requests");
    return;
  }

  std::lock_guard<std::mutex> lock(segments_mtx);
  std::shared_ptr<TopicSegment> segment;
  if (const auto it = segments_by_inode.find(st.st_ino); it != segments_by_inode.end()) {
    segment = it->second.lock();
  }
  if (!segment) {
    void * mapped =
      mmap(nullptr, sizeof(AgnocastTopicShm), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (mapped == MAP_FAILED) {
      RCLCPP_WARN(logger, "mapping the topic segment failed: %s", strerror(errno));
      return;
    }
    segment = std::make_shared<TopicSegment>();
    segment->shm = static_cast<AgnocastTopicShm *>(mapped);
    segment->topic_name.assign(topic_name.ptr != nullptr ? topic_name.ptr : "", topic_name.len);
    if (__atomic_load_n(&segment->shm->magic, __ATOMIC_ACQUIRE) != AGNOCAST_TOPIC_SHM_MAGIC) {
      RCLCPP_WARN(logger, "topic segment is not initialized; using plain requests");
      return;
    }
    segment->fd = shm_fd;
    closer.fd = -1;
    for (auto it = segments_by_inode.begin(); it != segments_by_inode.end();) {
      it = it->second.expired() ? segments_by_inode.erase(it) : std::next(it);
    }
    segments_by_inode[st.st_ino] = segment;
  }
  segments[segment_key(topic_name, pubsub_id)] = std::move(segment);
}

void detach_topic_segment(const struct name_info & topic_name, int32_t pubsub_id)
{
  std::lock_guard<std::mutex> lock(segments_mtx);
  segments.erase(segment_key(topic_name, pubsub_id));
}

// ------------------------------------------------------------
// Memory pools this process has mapped
// ------------------------------------------------------------

// Append-only: a pool is never unmapped. Readers scan [0, g_region_num) without
// a lock; the writer publishes a slot by bumping the count.
struct MappedRegion
{
  pid_t pid;
  uint64_t addr;
  uint64_t size;
};
constexpr uint32_t kMaxMappedRegions = 4096 + 1;
MappedRegion g_regions[kMaxMappedRegions];
std::atomic<uint32_t> g_region_num{0};
std::mutex g_region_write_mtx;

bool region_mapped(pid_t pid)
{
  const uint32_t n = g_region_num.load(std::memory_order_acquire);
  for (uint32_t i = 0; i < n; i++) {
    if (g_regions[i].pid == pid) return true;
  }
  return false;
}

// Which publishers a receive may hand out: those whose memory is mapped, plus
// those whose memory the caller maps right after this call returns.
struct ReadablePublishers
{
  const publisher_shm_info * fresh = nullptr;
  uint32_t fresh_num = 0;

  bool operator()(int32_t pid) const
  {
    for (uint32_t i = 0; i < fresh_num; i++) {
      if (fresh[i].pid == pid) return true;
    }
    return region_mapped(pid);
  }
};

// ------------------------------------------------------------
// What this process registered, for a restarted daemon
// ------------------------------------------------------------

struct PublisherRecord
{
  std::string topic_name;
  int32_t id;
  AddPublisherRequest req;
};

struct SubscriberRecord
{
  std::string topic_name;
  int32_t id;
  AddSubscriberRequest req;
  int eventfd;
};

struct BridgeRecord
{
  bool has_r2a = false;
  bool has_a2r = false;
};

struct ClientRegistry
{
  pid_t owner = 0;
  std::vector<PublisherRecord> publishers;
  std::vector<SubscriberRecord> subscribers;
  std::unordered_map<std::string, BridgeRecord> bridges;
  std::unordered_map<std::string, uint32_t> ros2_subscriber_num;
  std::unordered_map<std::string, uint32_t> ros2_publisher_num;
};

// The process registration is recorded during initialization, before the heap
// may be used, so it lives in plain storage.
struct ProcessRecord
{
  pid_t owner = 0;
  uint32_t domain_id = 0;
  bool is_performance_bridge_manager = false;
  uint64_t shm_addr = 0;
};
ProcessRecord g_process_record;

std::mutex registry_mtx;
// Never freed: a forked child starts a registry of its own and must not free
// the parent's through its own allocator.
ClientRegistry * g_registry = nullptr;

ClientRegistry & registry_locked()
{
  if (g_registry == nullptr || g_registry->owner != getpid()) {
    g_registry = new ClientRegistry();
    g_registry->owner = getpid();
  }
  return *g_registry;
}

std::string to_string(const struct name_info & name)
{
  return std::string(name.ptr != nullptr ? name.ptr : "", name.len);
}

void forget_publisher(const std::string & topic_name, int32_t id)
{
  std::lock_guard<std::mutex> lock(registry_mtx);
  auto & v = registry_locked().publishers;
  v.erase(
    std::remove_if(
      v.begin(), v.end(),
      [&](const PublisherRecord & r) { return r.id == id && r.topic_name == topic_name; }),
    v.end());
}

void forget_subscriber(const std::string & topic_name, int32_t id)
{
  std::lock_guard<std::mutex> lock(registry_mtx);
  auto & v = registry_locked().subscribers;
  v.erase(
    std::remove_if(
      v.begin(), v.end(),
      [&](const SubscriberRecord & r) { return r.id == id && r.topic_name == topic_name; }),
    v.end());
}

// Replays this process's registrations to a freshly connected daemon. Caller
// holds socket_mtx. False only when the connection broke again.
bool restore_locked()
{
  auto call =
    [](uint32_t cmd, const void * req, uint32_t req_size, int send_fd, const char * what) -> int {
    bool transport_failed = false;
    const int r = daemon_call_locked(
      cmd, req, req_size, nullptr, 0, send_fd, nullptr, 0, nullptr, &transport_failed);
    if (r < 0 && transport_failed) return -2;
    if (r < 0 && errno != EEXIST) {
      RCLCPP_WARN(logger, "restoring %s after a daemon restart failed: %s", what, strerror(errno));
    }
    return r;
  };

  if (g_process_record.owner == getpid()) {
    RestoreProcessRequest req{};
    req.shm_addr = g_process_record.shm_addr;
    req.domain_id = g_process_record.domain_id;
    req.is_performance_bridge_manager = g_process_record.is_performance_bridge_manager;
    if (call(AGNOCAST_CMD_RESTORE_PROCESS, &req, sizeof(req), -1, "the process") == -2) {
      return false;
    }

    RestoreMappingsRequest map_req{};
    const uint32_t n = g_region_num.load(std::memory_order_acquire);
    for (uint32_t i = 0; i < n; i++) {
      if (g_regions[i].addr == g_process_record.shm_addr) continue;
      map_req.shm_addrs[map_req.num++] = g_regions[i].addr;
      if (map_req.num == AGNOCAST_RESTORE_MAPPING_BATCH || i + 1 == n) {
        if (call(AGNOCAST_CMD_RESTORE_MAPPINGS, &map_req, sizeof(map_req), -1, "mappings") == -2) {
          return false;
        }
        map_req.num = 0;
      }
    }
  }

  std::vector<std::pair<std::string, std::shared_ptr<TopicSegment>>> topics;
  {
    std::lock_guard<std::mutex> lock(segments_mtx);
    for (const auto & [key, segment] : segments) {
      (void)key;
      if (!segment || segment->fd < 0) continue;
      const bool listed = std::any_of(
        topics.begin(), topics.end(), [&](const auto & t) { return t.second == segment; });
      if (!listed) topics.emplace_back(segment->topic_name, segment);
    }
  }
  for (const auto & [name, segment] : topics) {
    RestoreTopicRequest req{};
    std::strncpy(req.topic_name, name.c_str(), sizeof(req.topic_name) - 1);
    if (call(AGNOCAST_CMD_RESTORE_TOPIC, &req, sizeof(req), segment->fd, "a topic") == -2) {
      return false;
    }
  }

  ClientRegistry snapshot;
  {
    std::lock_guard<std::mutex> lock(registry_mtx);
    snapshot = registry_locked();
  }
  for (const auto & rec : snapshot.publishers) {
    RestorePublisherRequest req{};
    req.reg = rec.req;
    req.publisher_id = rec.id;
    if (call(AGNOCAST_CMD_RESTORE_PUBLISHER, &req, sizeof(req), -1, "a publisher") == -2) {
      return false;
    }
  }
  for (const auto & rec : snapshot.subscribers) {
    RestoreSubscriberRequest req{};
    req.reg = rec.req;
    req.subscriber_id = rec.id;
    const int r =
      call(AGNOCAST_CMD_RESTORE_SUBSCRIBER, &req, sizeof(req), rec.eventfd, "a subscriber");
    if (r == -2) return false;
  }
  for (const auto & [name, br] : snapshot.bridges) {
    for (const bool is_r2a : {true, false}) {
      if (is_r2a ? !br.has_r2a : !br.has_a2r) continue;
      AddBridgeRequest req{};
      std::strncpy(req.topic_name, name.c_str(), sizeof(req.topic_name) - 1);
      req.is_r2a = is_r2a;
      bool transport_failed = false;
      AddBridgeResponse resp{};
      if (
        daemon_call_locked(
          AGNOCAST_CMD_ADD_BRIDGE, &req, sizeof(req), &resp, sizeof(resp), -1, nullptr, 0, nullptr,
          &transport_failed) < 0 &&
        transport_failed) {
        return false;
      }
    }
  }
  for (const auto & [name, num] : snapshot.ros2_subscriber_num) {
    SetRos2SubscriberNumRequest req{};
    std::strncpy(req.topic_name, name.c_str(), sizeof(req.topic_name) - 1);
    req.ros2_subscriber_num = num;
    if (call(AGNOCAST_CMD_SET_ROS2_SUBSCRIBER_NUM, &req, sizeof(req), -1, "a ROS 2 count") == -2) {
      return false;
    }
  }
  for (const auto & [name, num] : snapshot.ros2_publisher_num) {
    SetRos2PublisherNumRequest req{};
    std::strncpy(req.topic_name, name.c_str(), sizeof(req.topic_name) - 1);
    req.ros2_publisher_num = num;
    if (call(AGNOCAST_CMD_SET_ROS2_PUBLISHER_NUM, &req, sizeof(req), -1, "a ROS 2 count") == -2) {
      return false;
    }
  }
  return call(AGNOCAST_CMD_RESTORE_DONE, nullptr, 0, -1, "the registrations") != -2;
}

// ------------------------------------------------------------
// Data plane
// ------------------------------------------------------------

// Caller holds segment.notify_mtx. Does not wait for a daemon that is down:
// the cached eventfds keep working meanwhile.
int refresh_notify_fds(TopicSegment & segment)
{
  std::unordered_map<int32_t, int> fds;
  uint64_t generation = UINT64_MAX;
  uint32_t offset = 0;
  CallOptions opts;
  opts.wait_for_daemon = false;
  while (true) {
    GetSubscriberEventfdsRequest req{};
    std::strncpy(req.topic_name, segment.topic_name.c_str(), sizeof(req.topic_name) - 1);
    req.offset = offset;
    GetSubscriberEventfdsResponse resp{};
    int got[AGNOCAST_SUBSCRIBER_EVENTFD_BATCH];
    uint32_t got_num = 0;
    const int r = daemon_call(
      AGNOCAST_CMD_GET_SUBSCRIBER_EVENTFDS, &req, sizeof(req), &resp, sizeof(resp), -1, got,
      AGNOCAST_SUBSCRIBER_EVENTFD_BATCH, &got_num, &opts);
    if (r < 0 || got_num != resp.num) {
      for (uint32_t i = 0; i < got_num; i++) close(got[i]);
      for (const auto & [id, efd] : fds) {
        (void)id;
        close(efd);
      }
      if (r == 0) errno = EPROTO;
      return -1;
    }
    // The first batch's generation is the conservative one: a change between
    // batches is caught by the next publish.
    if (offset == 0) generation = resp.sub_generation;
    for (uint32_t i = 0; i < resp.num; i++) {
      const auto [it, inserted] = fds.emplace(resp.subscriber_ids[i], got[i]);
      if (!inserted) close(got[i]);
    }
    offset += resp.num;
    if (resp.num == 0 || offset >= resp.total) break;
  }

  // While a restarted daemon is still collecting the subscribers, it lists only
  // those already restored. Keep waking the others with the eventfds from
  // before: ids are never reused, so an id still subscribed in the segment is
  // still the same subscriber.
  {
    TopicShmLock lock(segment.shm);
    for (auto & [id, efd] : segment.notify_fds) {
      if (efd < 0 || fds.count(id) > 0 || !topic_shm_valid_id(id)) continue;
      if (segment.shm->ids[id].kind != TOPIC_SHM_ID_SUBSCRIBER) continue;
      fds.emplace(id, efd);
      efd = -1;
    }
  }
  for (const auto & [id, efd] : segment.notify_fds) {
    (void)id;
    if (efd >= 0) close(efd);
  }
  segment.notify_fds = std::move(fds);
  segment.notify_generation = generation;
  return 0;
}

void notify_subscribers_local(
  TopicSegment & segment, const int32_t * subscriber_ids, uint32_t subscriber_num,
  uint64_t generation)
{
  std::lock_guard<std::mutex> lock(segment.notify_mtx);
  if (segment.notify_generation != generation && refresh_notify_fds(segment) < 0) {
    // The daemon is down or restarting: wake whom we can with the cached
    // eventfds and try again on the next publish.
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
      RCLCPP_WARN(
        logger, "fetching subscriber eventfds for '%s' failed (%s); using cached ones",
        segment.topic_name.c_str(), strerror(errno));
    }
  }
  const uint64_t one = 1;
  for (uint32_t i = 0; i < subscriber_num; i++) {
    const auto it = segment.notify_fds.find(subscriber_ids[i]);
    if (it == segment.notify_fds.end()) continue;
    // EAGAIN means the counter is saturated: the subscriber is already awake.
    if (write(it->second, &one, sizeof(one)) == -1 && errno != EAGAIN) {
      RCLCPP_ERROR(logger, "eventfd write failed: %s", strerror(errno));
    }
  }
}

thread_local int32_t t_subscriber_ids[kTopicShmIdNum];

int publish_local(
  TopicSegment & segment, union ioctl_publish_msg_args * args,
  [[maybe_unused]] LocalStamps & stamps)
{
  int64_t entry_id = -1;
  uint64_t released_addrs[AGNOCAST_PROTO_MAX_RELEASE_NUM];
  uint32_t released_num = 0;
  uint32_t subscriber_num = 0;
  uint64_t generation = 0;
  int ret = 0;
  AGNOCAST_BENCH_LOCAL_STAMP(stamps.lock_begin);
  topic_shm_lock(segment.shm);
  AGNOCAST_BENCH_LOCAL_STAMP(stamps.lock_end);
  ret = topic_shm_publish(
    segment.shm, args->publisher_id, args->msg_virtual_address, &entry_id, released_addrs,
    &released_num, t_subscriber_ids, &subscriber_num);
  generation = segment.shm->sub_generation;
  topic_shm_unlock(segment.shm);
  AGNOCAST_BENCH_LOCAL_STAMP(stamps.work_end);
  if (ret < 0) {
    errno = -ret;
    return -1;
  }

  notify_subscribers_local(segment, t_subscriber_ids, subscriber_num, generation);
  AGNOCAST_BENCH_LOCAL_STAMP(stamps.notify_end);

  args->ret_entry_id = entry_id;
  args->ret_released_num = released_num;
  for (uint32_t i = 0; i < released_num; i++) {
    args->ret_released_addrs[i] = released_addrs[i];
  }
  return 0;
}

// When a publisher appeared since the subscriber last looked, asks the daemon
// where the publishers' memory is and writes the pools this process has not
// mapped yet to out (the caller maps them after the call). Returns how many.
// Without the daemon it returns 0 and leaves the flag set: the publishers'
// entries then wait in the segment until their memory can be mapped.
uint32_t fetch_new_publisher_memory(
  TopicSegment & segment, int32_t subscriber_id, publisher_shm_info * out, uint32_t out_size)
{
  bool needed = false;
  {
    TopicShmLock lock(segment.shm);
    needed = topic_shm_consume_mmap_update(segment.shm, subscriber_id);
  }
  if (!needed) return 0;

  GetPublisherShmInfosRequest req{};
  std::strncpy(req.topic_name, segment.topic_name.c_str(), sizeof(req.topic_name) - 1);
  req.subscriber_id = subscriber_id;
  auto resp = std::make_unique<GetPublisherShmInfosResponse>();
  CallOptions opts;
  opts.wait_for_daemon = false;
  if (
    daemon_call(
      AGNOCAST_CMD_GET_PUBLISHER_SHM_INFOS, &req, sizeof(req), resp.get(), sizeof(*resp), -1,
      nullptr, 0, nullptr, &opts) < 0) {
    TopicShmLock lock(segment.shm);
    topic_shm_request_mmap_update(segment.shm, subscriber_id);
    return 0;
  }

  uint32_t num = 0;
  const uint32_t total = std::min<uint32_t>(resp->pub_shm_num, AGNOCAST_PROTO_MAX_PUBLISHER_NUM);
  for (uint32_t i = 0; i < total && num < out_size; i++) {
    const auto & info = resp->pub_shm_infos[i];
    if (region_mapped(info.pid)) continue;
    out[num].pid = info.pid;
    out[num].shm_addr = info.shm_addr;
    out[num].shm_size = info.shm_size;
    num++;
  }
  return num;
}

int receive_local(
  TopicSegment & segment, union ioctl_receive_msg_args * args, publisher_shm_info * shm_buf,
  uint32_t shm_buf_size, [[maybe_unused]] LocalStamps & stamps)
{
  const uint32_t pub_shm_num =
    fetch_new_publisher_memory(segment, args->subscriber_id, shm_buf, shm_buf_size);

  int64_t entry_ids[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  uint64_t entry_addrs[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  uint16_t entry_num = 0;
  bool call_again = false;
  const ReadablePublishers readable{shm_buf, pub_shm_num};
  AGNOCAST_BENCH_LOCAL_STAMP(stamps.lock_begin);
  topic_shm_lock(segment.shm);
  AGNOCAST_BENCH_LOCAL_STAMP(stamps.lock_end);
  const int ret = topic_shm_receive(
    segment.shm, args->subscriber_id, readable, entry_ids, entry_addrs, &entry_num, &call_again);
  topic_shm_unlock(segment.shm);
  AGNOCAST_BENCH_LOCAL_STAMP(stamps.work_end);
  AGNOCAST_BENCH_LOCAL_STAMP(stamps.notify_end);
  if (ret < 0) {
    errno = -ret;
    return -1;
  }

  args->ret_entry_num = entry_num;
  args->ret_call_again = call_again;
  args->ret_pub_shm_num = pub_shm_num;
  for (uint16_t i = 0; i < entry_num; i++) {
    args->ret_entry_ids[i] = entry_ids[i];
    args->ret_entry_addrs[i] = entry_addrs[i];
  }
  return 0;
}

int take_local(
  TopicSegment & segment, union ioctl_take_msg_args * args, publisher_shm_info * shm_buf,
  uint32_t shm_buf_size)
{
  if (!topic_shm_valid_id(args->subscriber_id)) {
    errno = EINVAL;
    return -1;
  }
  const uint32_t pub_shm_num =
    fetch_new_publisher_memory(segment, args->subscriber_id, shm_buf, shm_buf_size);

  uint64_t addr = 0;
  int64_t entry_id = -1;
  const ReadablePublishers readable{shm_buf, pub_shm_num};
  int ret = 0;
  {
    TopicShmLock lock(segment.shm);
    ret = topic_shm_take(
      segment.shm, args->subscriber_id, args->allow_same_message, readable, &addr, &entry_id);
  }
  if (ret < 0) {
    errno = -ret;
    return -1;
  }
  args->ret_addr = addr;
  args->ret_entry_id = entry_id;
  args->ret_pub_shm_num = pub_shm_num;
  return 0;
}

}  // namespace

void daemon_note_mapped_region(pid_t pid, uint64_t addr, uint64_t size)
{
  std::lock_guard<std::mutex> lock(g_region_write_mtx);
  const uint32_t n = g_region_num.load(std::memory_order_relaxed);
  for (uint32_t i = 0; i < n; i++) {
    if (g_regions[i].pid == pid && g_regions[i].addr == addr) return;
  }
  if (n == kMaxMappedRegions) return;
  g_regions[n] = MappedRegion{pid, addr, size};
  g_region_num.store(n + 1, std::memory_order_release);
}

int agnocast_ipc_get_version(struct ioctl_get_version_args * args)
{
  GetVersionResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_GET_VERSION, nullptr, 0, &resp, sizeof(resp));
  if (r == 0) {
    std::memcpy(args->ret_version, resp.version, VERSION_BUFFER_LEN);
  }
  return r;
}

int agnocast_ipc_add_process(union ioctl_add_process_args * args)
{
  AddProcessRequest req{};
  // The daemon protocol still carries a bool; only the bridge manager role has
  // ever set it.
  req.is_performance_bridge_manager = (args->role == PROCESS_ROLE_BRIDGE_MANAGER);
  req.domain_id = args->domain_id;
  AddProcessResponse resp{};
  struct Ctx
  {
    const AddProcessRequest * req;
    const AddProcessResponse * resp;
  } ctx{&req, &resp};
  CallOptions opts;
  opts.ctx = &ctx;
  // No heap here: this runs before the allocator is set up.
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    // A bridge manager that lost the race is told to exit, not given a pool.
    if (c->resp->shm_addr == 0) return;
    g_process_record.owner = getpid();
    g_process_record.domain_id = c->req->domain_id;
    g_process_record.is_performance_bridge_manager = c->req->is_performance_bridge_manager;
    g_process_record.shm_addr = c->resp->shm_addr;
  };
  int r = daemon_call(
    AGNOCAST_CMD_ADD_PROCESS, &req, sizeof(req), &resp, sizeof(resp), -1, nullptr, 0, nullptr,
    &opts);
  if (r == 0) {
    args->ret_addr = resp.shm_addr;
    args->ret_shm_size = resp.shm_size;
    args->ret_unlink_daemon_exist = resp.unlink_daemon_exist;
    args->ret_bridge_daemon_exist = resp.performance_bridge_daemon_exist;
    args->ret_discovery_agent_exist = resp.discovery_agent_exist;
  }
  return r;
}

int agnocast_ipc_add_publisher(union ioctl_add_publisher_args * args)
{
  AddPublisherRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  copy_name(req.node_name, sizeof(req.node_name), args->node_name);
  req.qos_depth = args->qos_depth;
  req.qos_is_transient_local = args->qos_is_transient_local;
  req.is_bridge = args->is_bridge;
  AddPublisherResponse resp{};
  int shm_fd = -1;
  uint32_t shm_fd_num = 0;
  ensure_watcher();
  // ret_id overlaps topic_name in the union, so take the name first.
  const struct name_info topic_name = args->topic_name;
  struct Ctx
  {
    const struct name_info * topic_name;
    const AddPublisherRequest * req;
    const AddPublisherResponse * resp;
    int * shm_fd;
    uint32_t * shm_fd_num;
  } ctx{&topic_name, &req, &resp, &shm_fd, &shm_fd_num};
  CallOptions opts;
  opts.ctx = &ctx;
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    attach_topic_segment(
      *c->topic_name, c->resp->publisher_id, *c->shm_fd_num > 0 ? *c->shm_fd : -1);
    *c->shm_fd_num = 0;
    std::lock_guard<std::mutex> lock(registry_mtx);
    registry_locked().publishers.push_back(
      PublisherRecord{to_string(*c->topic_name), c->resp->publisher_id, *c->req});
  };
  int r = daemon_call(
    AGNOCAST_CMD_ADD_PUBLISHER, &req, sizeof(req), &resp, sizeof(resp), -1, &shm_fd, 1, &shm_fd_num,
    &opts);
  if (r == 0) {
    args->ret_id = resp.publisher_id;
  } else if (shm_fd_num > 0) {
    close(shm_fd);
  }
  return r;
}

int agnocast_ipc_add_subscriber(union ioctl_add_subscriber_args * args)
{
  AddSubscriberRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  copy_name(req.node_name, sizeof(req.node_name), args->node_name);
  req.qos_depth = args->qos_depth;
  req.qos_is_transient_local = args->qos_is_transient_local;
  req.qos_is_reliable = args->qos_is_reliable;
  req.is_take_sub = args->is_take_sub;
  req.ignore_local_publications = args->ignore_local_publications;
  req.is_bridge = args->is_bridge;
  AddSubscriberResponse resp{};
  // args->eventfd is the subscriber's notification eventfd; publishers write to
  // it, the way the kernel module signals the eventfd_ctx.
  int shm_fd = -1;
  uint32_t shm_fd_num = 0;
  ensure_watcher();
  // ret_id overlaps topic_name in the union, so take the name first.
  const struct name_info topic_name = args->topic_name;
  const int eventfd = args->eventfd;
  struct Ctx
  {
    const struct name_info * topic_name;
    const AddSubscriberRequest * req;
    const AddSubscriberResponse * resp;
    int eventfd;
    int * shm_fd;
    uint32_t * shm_fd_num;
  } ctx{&topic_name, &req, &resp, eventfd, &shm_fd, &shm_fd_num};
  CallOptions opts;
  opts.ctx = &ctx;
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    attach_topic_segment(
      *c->topic_name, c->resp->subscriber_id, *c->shm_fd_num > 0 ? *c->shm_fd : -1);
    *c->shm_fd_num = 0;
    std::lock_guard<std::mutex> lock(registry_mtx);
    registry_locked().subscribers.push_back(
      SubscriberRecord{to_string(*c->topic_name), c->resp->subscriber_id, *c->req, c->eventfd});
  };
  int r = daemon_call(
    AGNOCAST_CMD_ADD_SUBSCRIBER, &req, sizeof(req), &resp, sizeof(resp), eventfd, &shm_fd, 1,
    &shm_fd_num, &opts);
  if (r == 0) {
    args->ret_id = resp.subscriber_id;
  } else if (shm_fd_num > 0) {
    close(shm_fd);
  }
  return r;
}

int agnocast_ipc_publish_msg(union ioctl_publish_msg_args * args)
{
  AGNOCAST_BENCH_CALL_ENTER();

  if (const auto segment = find_segment(args->topic_name, args->publisher_id)) {
    LocalStamps stamps;
    const int r = publish_local(*segment, args, stamps);
#ifdef AGNOCAST_BENCH_TIMING
    last_publish_notify_end_ns = stamps.notify_end;
#endif
    AGNOCAST_BENCH_CALL_LEAVE_LOCAL(publish, stamps);
    return r;
  }

  PublishMsgRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.publisher_id = args->publisher_id;
  req.msg_virtual_address = args->msg_virtual_address;
  PublishMsgResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_PUBLISH_MSG, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    args->ret_entry_id = resp.entry_id;
    args->ret_released_num = resp.released_num;
    for (uint32_t i = 0; i < resp.released_num && i < AGNOCAST_PROTO_MAX_RELEASE_NUM; i++) {
      args->ret_released_addrs[i] = resp.released_addrs[i];
    }
  }

#ifdef AGNOCAST_BENCH_TIMING
  last_publish_notify_end_ns = g_stamps.daemon_notify_end;
#endif
  AGNOCAST_BENCH_CALL_LEAVE(publish);
  return r;
}

int agnocast_ipc_receive_msg(union ioctl_receive_msg_args * args)
{
  AGNOCAST_BENCH_CALL_ENTER();

  // ioctl_receive_msg_args is a union: the output ret_* fields overlap the input
  // pub_shm_info_addr/size. The kernel reads inputs before writing outputs, but here
  // we must snapshot the caller-provided buffer pointer/size before writing any ret_*
  // field, otherwise writing ret_entry_ids[]/ret_entry_addrs[] clobbers them.
  const uint64_t pub_shm_info_addr = args->pub_shm_info_addr;
  const uint32_t pub_shm_info_size = args->pub_shm_info_size;

  if (const auto segment = find_segment(args->topic_name, args->subscriber_id)) {
    LocalStamps stamps;
    const int r = receive_local(
      *segment, args, reinterpret_cast<publisher_shm_info *>(pub_shm_info_addr), pub_shm_info_size,
      stamps);
    AGNOCAST_BENCH_CALL_LEAVE_LOCAL(receive, stamps);
    return r;
  }

  ReceiveMsgRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.subscriber_id = args->subscriber_id;
  ReceiveMsgResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_RECEIVE_MSG, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    args->ret_entry_num = resp.entry_num;
    args->ret_call_again = resp.call_again;
    args->ret_pub_shm_num = resp.pub_shm_num;
    for (uint16_t i = 0; i < resp.entry_num && i < AGNOCAST_PROTO_MAX_RECEIVE_NUM; i++) {
      args->ret_entry_ids[i] = resp.entry_ids[i];
      args->ret_entry_addrs[i] = resp.entry_addrs[i];
    }
    // Copy pub_shm_infos into the caller-provided buffer (snapshotted above),
    // minus pools already mapped: a restarted daemon may list them again.
    auto * shm_buf = reinterpret_cast<publisher_shm_info *>(pub_shm_info_addr);
    uint32_t copy_count = 0;
    for (uint32_t i = 0; i < resp.pub_shm_num && copy_count < pub_shm_info_size; i++) {
      if (region_mapped(resp.pub_shm_infos[i].pid)) continue;
      shm_buf[copy_count].pid = resp.pub_shm_infos[i].pid;
      shm_buf[copy_count].shm_addr = resp.pub_shm_infos[i].shm_addr;
      shm_buf[copy_count].shm_size = resp.pub_shm_infos[i].shm_size;
      copy_count++;
    }
    args->ret_pub_shm_num = copy_count;
  }

  AGNOCAST_BENCH_CALL_LEAVE(receive);
  return r;
}

int agnocast_ipc_take_msg(union ioctl_take_msg_args * args)
{
  // ioctl_take_msg_args is a union: snapshot the caller-provided buffer pointer/size before
  // writing any ret_* field so the output does not clobber the input pub_shm_info_addr/size.
  const uint64_t pub_shm_info_addr = args->pub_shm_info_addr;
  const uint32_t pub_shm_info_size = args->pub_shm_info_size;

  if (const auto segment = find_segment(args->topic_name, args->subscriber_id)) {
    return take_local(
      *segment, args, reinterpret_cast<publisher_shm_info *>(pub_shm_info_addr), pub_shm_info_size);
  }

  TakeMsgRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.subscriber_id = args->subscriber_id;
  req.allow_same_message = args->allow_same_message;
  TakeMsgResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_TAKE_MSG, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    args->ret_addr = resp.addr;
    args->ret_entry_id = resp.entry_id;
    auto * shm_buf = reinterpret_cast<publisher_shm_info *>(pub_shm_info_addr);
    uint32_t copy_count = 0;
    for (uint32_t i = 0; i < resp.pub_shm_num && copy_count < pub_shm_info_size; i++) {
      if (region_mapped(resp.pub_shm_infos[i].pid)) continue;
      shm_buf[copy_count].pid = resp.pub_shm_infos[i].pid;
      shm_buf[copy_count].shm_addr = resp.pub_shm_infos[i].shm_addr;
      shm_buf[copy_count].shm_size = resp.pub_shm_infos[i].shm_size;
      copy_count++;
    }
    args->ret_pub_shm_num = copy_count;
  }
  return r;
}

int agnocast_ipc_release_sub_ref(struct ioctl_update_entry_args * args)
{
  if (const auto segment = find_segment(args->topic_name, args->pubsub_id)) {
    int ret = 0;
    {
      TopicShmLock lock(segment->shm);
      ret = topic_shm_release_ref(segment->shm, args->pubsub_id, args->entry_id);
    }
    if (ret < 0) {
      errno = -ret;
      return -1;
    }
    return 0;
  }

  ReleaseSubRefRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.pubsub_id = args->pubsub_id;
  req.entry_id = args->entry_id;
  return daemon_call(AGNOCAST_CMD_RELEASE_SUB_REF, &req, sizeof(req), nullptr, 0);
}

int agnocast_ipc_remove_publisher(struct ioctl_remove_publisher_args * args)
{
  RemovePublisherRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.publisher_id = args->publisher_id;
  // Forgotten first, so a restore running meanwhile does not bring it back.
  forget_publisher(to_string(args->topic_name), args->publisher_id);
  const uint64_t gen = g_conn_generation.load();
  int r = daemon_call(AGNOCAST_CMD_REMOVE_PUBLISHER, &req, sizeof(req), nullptr, 0);
  if (r < 0 && (errno == ENODATA || errno == EINVAL) && g_conn_generation.load() != gen) {
    r = 0;  // the restarted daemon already dropped it as unclaimed
  }
  detach_topic_segment(args->topic_name, args->publisher_id);
  return r;
}

int agnocast_ipc_remove_subscriber(struct ioctl_remove_subscriber_args * args)
{
  RemoveSubscriberRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.subscriber_id = args->subscriber_id;
  forget_subscriber(to_string(args->topic_name), args->subscriber_id);
  const uint64_t gen = g_conn_generation.load();
  int r = daemon_call(AGNOCAST_CMD_REMOVE_SUBSCRIBER, &req, sizeof(req), nullptr, 0);
  if (r < 0 && (errno == ENODATA || errno == EINVAL) && g_conn_generation.load() != gen) {
    r = 0;  // the restarted daemon already dropped it as unclaimed
  }
  detach_topic_segment(args->topic_name, args->subscriber_id);
  return r;
}

int agnocast_ipc_get_subscriber_num(union ioctl_get_subscriber_num_args * args)
{
  GetSubscriberNumRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  GetSubscriberNumResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_GET_SUBSCRIBER_NUM, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    args->ret_other_process_subscriber_num = resp.other_process_subscriber_num;
    args->ret_same_process_subscriber_num = resp.same_process_subscriber_num;
    args->ret_ros2_subscriber_num = resp.ros2_subscriber_num;
    args->ret_a2r_bridge_exist = resp.a2r_bridge_exist;
    args->ret_r2a_bridge_exist = resp.r2a_bridge_exist;
  }
  return r;
}

int agnocast_ipc_get_publisher_num(union ioctl_get_publisher_num_args * args)
{
  GetPublisherNumRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  GetPublisherNumResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_GET_PUBLISHER_NUM, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    args->ret_publisher_num = resp.publisher_num;
    args->ret_ros2_publisher_num = resp.ros2_publisher_num;
    args->ret_r2a_bridge_exist = resp.r2a_bridge_exist;
    args->ret_a2r_bridge_exist = resp.a2r_bridge_exist;
  }
  return r;
}

int agnocast_ipc_get_exit_process(struct ioctl_get_exit_process_args * args)
{
  GetExitProcessResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_GET_EXIT_PROCESS, nullptr, 0, &resp, sizeof(resp));
  if (r == 0) {
    args->ret_daemon_should_exit = resp.daemon_should_exit;
    args->ret_pid = resp.pid;
  }
  return r;
}

int agnocast_ipc_get_subscriber_qos(struct ioctl_get_subscriber_qos_args * args)
{
  GetSubscriberQosRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.subscriber_id = args->subscriber_id;
  GetSubscriberQosResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_GET_SUBSCRIBER_QOS, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    args->ret_depth = resp.depth;
    args->ret_is_transient_local = resp.is_transient_local;
    args->ret_is_reliable = resp.is_reliable;
  }
  return r;
}

int agnocast_ipc_get_publisher_qos(struct ioctl_get_publisher_qos_args * args)
{
  GetPublisherQosRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.publisher_id = args->publisher_id;
  GetPublisherQosResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_GET_PUBLISHER_QOS, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    args->ret_depth = resp.depth;
    args->ret_is_transient_local = resp.is_transient_local;
  }
  return r;
}

int agnocast_ipc_add_bridge(struct ioctl_add_bridge_args * args)
{
  AddBridgeRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.is_r2a = args->is_r2a;
  AddBridgeResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_ADD_BRIDGE, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    args->ret_pid = resp.pid;
    args->ret_has_r2a = resp.has_r2a;
    args->ret_has_a2r = resp.has_a2r;
    std::lock_guard<std::mutex> lock(registry_mtx);
    BridgeRecord & br = registry_locked().bridges[req.topic_name];
    br.has_r2a = resp.has_r2a;
    br.has_a2r = resp.has_a2r;
  }
  return r;
}

int agnocast_ipc_remove_bridge(struct ioctl_remove_bridge_args * args)
{
  RemoveBridgeRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.is_r2a = args->is_r2a;
  {
    std::lock_guard<std::mutex> lock(registry_mtx);
    auto & bridges = registry_locked().bridges;
    if (auto it = bridges.find(req.topic_name); it != bridges.end()) {
      (args->is_r2a ? it->second.has_r2a : it->second.has_a2r) = false;
      if (!it->second.has_r2a && !it->second.has_a2r) bridges.erase(it);
    }
  }
  return daemon_call(AGNOCAST_CMD_REMOVE_BRIDGE, &req, sizeof(req), nullptr, 0);
}

int agnocast_ipc_check_and_request_bridge_shutdown(
  struct ioctl_check_and_request_bridge_shutdown_args * args)
{
  CheckAndRequestBridgeShutdownResponse resp{};
  int r =
    daemon_call(AGNOCAST_CMD_CHECK_AND_REQUEST_BRIDGE_SHUTDOWN, nullptr, 0, &resp, sizeof(resp));
  if (r == 0) {
    args->ret_should_shutdown = resp.should_shutdown;
  }
  return r;
}

int agnocast_ipc_notify_bridge_shutdown()
{
  return daemon_call(AGNOCAST_CMD_NOTIFY_BRIDGE_SHUTDOWN, nullptr, 0, nullptr, 0);
}

int agnocast_ipc_set_ros2_subscriber_num(struct ioctl_set_ros2_subscriber_num_args * args)
{
  SetRos2SubscriberNumRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.ros2_subscriber_num = args->ros2_subscriber_num;
  const int r = daemon_call(AGNOCAST_CMD_SET_ROS2_SUBSCRIBER_NUM, &req, sizeof(req), nullptr, 0);
  if (r == 0) {
    std::lock_guard<std::mutex> lock(registry_mtx);
    registry_locked().ros2_subscriber_num[req.topic_name] = req.ros2_subscriber_num;
  }
  return r;
}

int agnocast_ipc_set_ros2_publisher_num(struct ioctl_set_ros2_publisher_num_args * args)
{
  SetRos2PublisherNumRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.ros2_publisher_num = args->ros2_publisher_num;
  const int r = daemon_call(AGNOCAST_CMD_SET_ROS2_PUBLISHER_NUM, &req, sizeof(req), nullptr, 0);
  if (r == 0) {
    std::lock_guard<std::mutex> lock(registry_mtx);
    registry_locked().ros2_publisher_num[req.topic_name] = req.ros2_publisher_num;
  }
  return r;
}

int agnocast_ipc_get_topic_list(union ioctl_topic_list_args * args)
{
  const uint64_t name_buf_addr = args->topic_name_buffer_addr;
  const uint64_t domain_buf_addr = args->domain_id_buffer_addr;
  const uint32_t name_buf_size = args->topic_name_buffer_size;

  GetTopicListResponse resp{};
  int r = daemon_call(AGNOCAST_CMD_GET_TOPIC_LIST, nullptr, 0, &resp, sizeof(resp));
  if (r == 0) {
    uint32_t copy_count = resp.topic_num;
    if (copy_count > name_buf_size) {
      copy_count = name_buf_size;
    }
    auto * name_buf = reinterpret_cast<char *>(name_buf_addr);
    if (name_buf != nullptr) {
      for (uint32_t i = 0; i < copy_count; i++) {
        char * dst = name_buf + static_cast<size_t>(i) * TOPIC_NAME_BUFFER_SIZE;
        std::strncpy(dst, resp.topic_names[i], TOPIC_NAME_BUFFER_SIZE - 1);
        dst[TOPIC_NAME_BUFFER_SIZE - 1] = '\0';
      }
    }
    auto * domain_buf = reinterpret_cast<uint32_t *>(domain_buf_addr);
    if (domain_buf != nullptr) {
      for (uint32_t i = 0; i < copy_count; i++) {
        domain_buf[i] = 0;
      }
    }
    args->ret_topic_num = copy_count;
  }
  return r;
}

static void copy_topic_info_entries(
  union ioctl_topic_info_args * args, uint32_t entry_num, const AgnocastTopicInfoEntry * entries)
{
  const uint64_t buf_addr = args->topic_info_ret_buffer_addr;
  const uint32_t buf_size = args->topic_info_ret_buffer_size;
  uint32_t copy_count = (entry_num < buf_size) ? entry_num : buf_size;
  auto * buf = reinterpret_cast<topic_info_ret *>(buf_addr);
  if (buf != nullptr) {
    for (uint32_t i = 0; i < copy_count; i++) {
      std::strncpy(buf[i].node_name, entries[i].node_name, NODE_NAME_BUFFER_SIZE - 1);
      buf[i].node_name[NODE_NAME_BUFFER_SIZE - 1] = '\0';
      buf[i].qos_depth = entries[i].qos_depth;
      buf[i].qos_is_transient_local = entries[i].qos_is_transient_local;
      buf[i].qos_is_reliable = entries[i].qos_is_reliable;
      buf[i].is_bridge = entries[i].is_bridge;
    }
  }
  args->ret_topic_info_ret_num = copy_count;
}

int agnocast_ipc_get_topic_subscriber_info(union ioctl_topic_info_args * args)
{
  GetTopicSubscriberInfoRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  // Heap-allocate: the inline entry array is ~811 KiB and must not live on the stack.
  auto resp = std::make_unique<GetTopicSubscriberInfoResponse>();
  std::memset(resp.get(), 0, sizeof(*resp));
  int r = daemon_call(
    AGNOCAST_CMD_GET_TOPIC_SUBSCRIBER_INFO, &req, sizeof(req), resp.get(), sizeof(*resp));
  if (r == 0) {
    copy_topic_info_entries(args, resp->entry_num, resp->entries);
  }
  return r;
}

int agnocast_ipc_get_topic_publisher_info(union ioctl_topic_info_args * args)
{
  GetTopicPublisherInfoRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  auto resp = std::make_unique<GetTopicPublisherInfoResponse>();
  std::memset(resp.get(), 0, sizeof(*resp));
  int r = daemon_call(
    AGNOCAST_CMD_GET_TOPIC_PUBLISHER_INFO, &req, sizeof(req), resp.get(), sizeof(*resp));
  if (r == 0) {
    copy_topic_info_entries(args, resp->entry_num, resp->entries);
  }
  return r;
}

int agnocast_ipc_debug_check_topic(const std::string & topic_name, DaemonTopicCheck * out)
{
  DebugCheckTopicRequest req{};
  std::strncpy(req.topic_name, topic_name.c_str(), sizeof(req.topic_name) - 1);
  DebugCheckTopicResponse resp{};
  const int r = daemon_call(AGNOCAST_CMD_DEBUG_CHECK_TOPIC, &req, sizeof(req), &resp, sizeof(resp));
  if (r == 0) {
    out->shm_violations = resp.shm_violations;
    out->registry_violations = resp.registry_violations;
    out->rollback_count = resp.rollback_count;
    out->rollback_record_num = resp.rollback_record_num;
    out->entry_num = resp.entry_num;
    out->sub_num = resp.sub_num;
    std::memcpy(out->message, resp.message, sizeof(out->message));
    out->message[sizeof(out->message) - 1] = '\0';
  }
  return r;
}

int agnocast_ipc_get_recovery_status(DaemonRecoveryStatus * out)
{
  GetRecoveryStatusResponse resp{};
  const int r = daemon_call(AGNOCAST_CMD_GET_RECOVERY_STATUS, nullptr, 0, &resp, sizeof(resp));
  if (r == 0) {
    out->recovered = resp.recovered;
    out->in_recovery = resp.in_recovery;
    out->expected_num = resp.expected_num;
    out->restored_num = resp.restored_num;
    out->dead_num = resp.dead_num;
    out->timed_out_num = resp.timed_out_num;
    out->adopted_topic_num = resp.adopted_topic_num;
    out->orphan_publisher_num = resp.orphan_publisher_num;
    out->orphan_subscriber_num = resp.orphan_subscriber_num;
    out->start_ns = resp.start_ns;
    out->end_ns = resp.end_ns;
  }
  return r;
}

}  // namespace agnocast

#endif  // AGNOCAST_USE_DAEMON
