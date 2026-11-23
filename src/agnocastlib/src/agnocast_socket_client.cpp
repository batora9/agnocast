// SPDX-License-Identifier: Apache-2.0
#ifdef AGNOCAST_USE_DAEMON

#include "agnocast/agnocast_bench_timing.hpp"
#include "agnocast/agnocast_ipc.hpp"
#include "agnocast/agnocast_utils.hpp"
#include "protocol.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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

// For a call answered from the channel without a round trip: the whole span is
// local work, reported as req_ns, with no daemon stamps.
void finalize_local_breakdown(IpcBreakdown & out, int64_t enter_ns, int64_t exit_ns)
{
  out = IpcBreakdown{};
  out.total_ns = exit_ns - enter_ns;
  out.req_ns = out.total_ns;
}

}  // namespace

// The macros expand inside namespace agnocast, so unqualified lookup reaches
// the finalizers in the unnamed namespace above.
#define AGNOCAST_BENCH_CALL_ENTER() const int64_t agnocast_bench_enter_ns = bench_timing_now_ns()
#define AGNOCAST_BENCH_CALL_LEAVE(slot) \
  finalize_breakdown(last_##slot##_ipc_breakdown, agnocast_bench_enter_ns, bench_timing_now_ns())
#define AGNOCAST_BENCH_CALL_LEAVE_LOCAL(slot) \
  finalize_local_breakdown(                   \
    last_##slot##_ipc_breakdown, agnocast_bench_enter_ns, bench_timing_now_ns())
#else
#define AGNOCAST_BENCH_CALL_ENTER() static_cast<void>(0)
#define AGNOCAST_BENCH_CALL_LEAVE(slot) static_cast<void>(0)
#define AGNOCAST_BENCH_CALL_LEAVE_LOCAL(slot) static_cast<void>(0)
#endif  // AGNOCAST_BENCH_TIMING

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
    const ssize_t n = recvmsg(fd, msg, MSG_DONTWAIT);
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
  n = recvmsg(agnocast_fd, recv_msg, 0);
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

namespace
{

bool is_transport_error(int err)
{
  return err == EPIPE || err == ECONNRESET || err == ENOTCONN || err == ECONNREFUSED;
}

}  // namespace

// Returns 0 on success, sets errno on failure.
//
// send_fd, when >= 0, is attached to the request with SCM_RIGHTS. The kernel
// module reads a subscriber's notification eventfd out of the caller's own fd
// table, which a userspace daemon cannot do, so the descriptor itself has to
// travel with the registration. The daemon gets its own copy.
//
// Caller holds socket_mtx. *transport_failed tells a broken connection (the
// daemon is gone) apart from an error the daemon replied with.
static int daemon_call_locked(
  uint32_t cmd, const void * req_payload, uint32_t req_size, void * resp_payload,
  uint32_t resp_size, int send_fd, bool * transport_failed)
{
  *transport_failed = false;

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
  if (recv_response(&recv_msg) < 0) {
    *transport_failed = is_transport_error(errno);
    return -1;
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

// ============================================================
// What this process knows, for a restarted daemon
// ============================================================
//
// The daemon keeps its metadata on its heap. After it restarts, it rebuilds
// that metadata from what its clients report (protocol.h, "Daemon restart
// recovery"), so this process keeps its own share of it:
//
//   - its registration and memory pool, and the pools it mapped
//   - per publisher: the entries it was not told to free, and the addresses
//     it asked to publish without getting the reply yet
//   - per subscriber: the entries it holds a reference to, and the last entry
//     handed to the user (what it has not handed out it does not hold: the
//     daemon delivers it again)
//
// Two rules keep this exact. A record is updated before the action it allows
// (a publish is recorded as pending before the request; an entry is recorded
// as held before the user gets it; a release is forgotten before it is sent),
// and a restore reads a subscriber's record and retires its channel under one
// lock, so no receive or release can fall between the two.

namespace
{

std::string to_string(const struct name_info & name)
{
  return std::string(name.ptr != nullptr ? name.ptr : "", name.len);
}

std::string pubsub_key(const std::string & topic_name, int32_t id)
{
  std::string key = topic_name;
  key.push_back('\0');
  key.append(std::to_string(id));
  return key;
}

// ---- The process registration and the pools it mapped ----
// Both are written during initialize_agnocast(), before the heap may be used,
// so they live in plain storage.

struct ProcessRecord
{
  pid_t owner = 0;
  uint32_t domain_id = 0;
  bool is_performance_bridge_manager = false;
  uint64_t shm_addr = 0;
};
ProcessRecord g_process_record;

struct MappedRegion
{
  pid_t pid;
  uint64_t addr;
  uint64_t size;
};
constexpr uint32_t kMaxMappedRegions = 4096 + 1;
MappedRegion g_regions[kMaxMappedRegions];
std::atomic<uint32_t> g_region_num{0};
pid_t g_regions_owner = 0;
std::mutex g_region_mtx;

bool region_mapped(pid_t pid)
{
  std::lock_guard<std::mutex> lock(g_region_mtx);
  if (g_regions_owner != getpid()) return false;
  const uint32_t n = g_region_num.load(std::memory_order_relaxed);
  for (uint32_t i = 0; i < n; i++) {
    if (g_regions[i].pid == pid) return true;
  }
  return false;
}

// ---- Channels, publishers and subscribers ----

struct ClientChannel
{
  AgnocastSubscriberChannel * shm = nullptr;
  // Several threads can drop the last reference to different messages of one
  // subscription at once; the daemon is the ring's only consumer.
  std::mutex release_mtx;

  ClientChannel() = default;
  ~ClientChannel()
  {
    if (shm != nullptr) munmap(shm, sizeof(*shm));
  }
  ClientChannel(const ClientChannel &) = delete;
  ClientChannel & operator=(const ClientChannel &) = delete;
};

struct PublisherState
{
  std::string topic_name;
  int32_t id = 0;
  AddPublisherRequest req{};
  std::mutex mtx;
  // Held across a publish: at most one publish of this publisher is without a
  // reply at any time, which a restarted daemon relies on (protocol.h). The
  // daemon serializes the publishes of a topic anyway.
  std::mutex publish_mtx;
  std::map<int64_t, uint64_t> entries;  // entry id → message address
  int64_t last_entry_id = -1;
  std::unordered_set<uint64_t> pending;  // published, reply not seen yet
};

struct SubscriberState
{
  std::string topic_name;
  int32_t id = 0;
  AddSubscriberRequest req{};
  int eventfd = -1;  // the user's; this process keeps it open
  std::mutex mtx;
  // Held by shared_ptr so a thread still using a channel keeps it mapped
  // after a restore or a removal replaced it. Null: plain requests only.
  std::shared_ptr<ClientChannel> channel;
  std::unordered_map<int64_t, uint64_t> held;  // entry id → message address
  int64_t latest_received_entry_id = -1;
  // The newest entry handed out from each publisher, released or not: it
  // tells a restarted daemon whether a publish whose reply was lost was
  // committed (see AGNOCAST_RESTORE_ENTRY_DELIVERED in protocol.h).
  std::unordered_map<int32_t, std::pair<int64_t, uint64_t>> newest_by_publisher;

  // Caller holds mtx.
  void note_delivered(int64_t entry_id, uint64_t addr, int32_t publisher_id)
  {
    held[entry_id] = addr;
    latest_received_entry_id = std::max(latest_received_entry_id, entry_id);
    auto & newest = newest_by_publisher[publisher_id];
    if (newest.first < entry_id || newest.second == 0) newest = {entry_id, addr};
  }
};

struct BridgeRecord
{
  bool has_r2a = false;
  bool has_a2r = false;
};

// Never freed: a child forked without exec starts its own and must not free
// the parent's through its own allocator.
struct ClientState
{
  pid_t owner = 0;
  std::unordered_map<std::string, std::shared_ptr<PublisherState>> publishers;
  std::unordered_map<std::string, std::shared_ptr<SubscriberState>> subscribers;
  std::unordered_map<std::string, BridgeRecord> bridges;
  std::unordered_map<std::string, uint32_t> ros2_subscriber_num;
  std::unordered_map<std::string, uint32_t> ros2_publisher_num;
};

// Lock ordering: socket_mtx → state_mtx → PublisherState / SubscriberState::mtx.
std::mutex state_mtx;
ClientState * g_state = nullptr;

ClientState & state_locked()
{
  if (g_state == nullptr || g_state->owner != getpid()) {
    g_state = new ClientState();
    g_state->owner = getpid();
  }
  return *g_state;
}

std::shared_ptr<PublisherState> find_publisher(const struct name_info & topic_name, int32_t id)
{
  std::lock_guard<std::mutex> lock(state_mtx);
  auto & m = state_locked().publishers;
  const auto it = m.find(pubsub_key(to_string(topic_name), id));
  return it != m.end() ? it->second : nullptr;
}

std::shared_ptr<SubscriberState> find_subscriber(const struct name_info & topic_name, int32_t id)
{
  std::lock_guard<std::mutex> lock(state_mtx);
  auto & m = state_locked().subscribers;
  const auto it = m.find(pubsub_key(to_string(topic_name), id));
  return it != m.end() ? it->second : nullptr;
}

// AGNOCAST_SUB_CHANNEL=0 keeps every subscriber on the plain request path, so
// one build can be measured both ways.
bool sub_channel_enabled()
{
  const char * v = std::getenv("AGNOCAST_SUB_CHANNEL");
  return v == nullptr || std::strcmp(v, "0") != 0;
}

// Hands over what the daemon pushed at publish time (see AgnocastPushRing).
// false when the subscriber has to send the request instead, in which case
// args is untouched. Receives of one subscription are serialized by mmap_mtx,
// so this is the ring's only consumer. Caller holds sub.mtx.
bool take_pushed_locked(SubscriberState & sub, union ioctl_receive_msg_args * args)
{
  if (!sub.channel) return false;
  AgnocastPushRing & ring = sub.channel->shm->push;
  // ipc_pending first: once it reads set, every push made before it is
  // visible, so an empty ring then means the rest is behind the request.
  const bool ipc_pending = __atomic_load_n(&ring.ipc_pending, __ATOMIC_ACQUIRE) != 0;
  const uint64_t head = __atomic_load_n(&ring.head, __ATOMIC_ACQUIRE);
  uint64_t tail = __atomic_load_n(&ring.tail, __ATOMIC_RELAXED);
  if (head == tail && ipc_pending) return false;

  int64_t entry_ids[MAX_RECEIVE_NUM];
  uint64_t entry_addrs[MAX_RECEIVE_NUM];
  int32_t publisher_ids[MAX_RECEIVE_NUM];
  uint16_t entry_num = 0;
  for (; tail != head && entry_num < MAX_RECEIVE_NUM; tail++, entry_num++) {
    const AgnocastPushSlot & slot = ring.slots[tail % AGNOCAST_PUSH_RING_SLOTS];
    entry_ids[entry_num] = __atomic_load_n(&slot.entry_id, __ATOMIC_RELAXED);
    entry_addrs[entry_num] = __atomic_load_n(&slot.entry_addr, __ATOMIC_RELAXED);
    publisher_ids[entry_num] = __atomic_load_n(&slot.publisher_id, __ATOMIC_RELAXED);
  }
  __atomic_store_n(&ring.tail, tail, __ATOMIC_RELEASE);

  args->ret_entry_num = entry_num;
  args->ret_call_again = (tail != head) || ipc_pending;
  args->ret_pub_shm_num = 0;
  for (uint16_t i = 0; i < entry_num; i++) {
    args->ret_entry_ids[i] = entry_ids[i];
    args->ret_entry_addrs[i] = entry_addrs[i];
    sub.note_delivered(entry_ids[i], entry_addrs[i], publisher_ids[i]);
  }
  return true;
}

// false when the ring is full; the caller then reports the release directly.
bool queue_release(ClientChannel & channel, int64_t entry_id)
{
  std::lock_guard<std::mutex> lock(channel.release_mtx);
  AgnocastReleaseRing & ring = channel.shm->release;
  const uint64_t head = __atomic_load_n(&ring.head, __ATOMIC_RELAXED);
  const uint64_t tail = __atomic_load_n(&ring.tail, __ATOMIC_ACQUIRE);
  if (head - tail >= AGNOCAST_RELEASE_RING_SLOTS) return false;
  __atomic_store_n(&ring.entry_ids[head % AGNOCAST_RELEASE_RING_SLOTS], entry_id, __ATOMIC_RELAXED);
  __atomic_store_n(&ring.head, head + 1, __ATOMIC_RELEASE);
  return true;
}

// A channel ready to be handed to the daemon, or null (the subscriber then
// works over plain requests). *memfd is the descriptor to send.
std::shared_ptr<ClientChannel> make_channel(int * memfd)
{
  *memfd = -1;
  if (!sub_channel_enabled()) return nullptr;
  const int fd = memfd_create("agnocast_sub_channel", MFD_CLOEXEC | MFD_ALLOW_SEALING);
  if (fd < 0) {
    RCLCPP_WARN(logger, "memfd_create for the subscriber channel failed: %s", strerror(errno));
    return nullptr;
  }
  constexpr size_t kChannelSize = sizeof(AgnocastSubscriberChannel);
  auto channel = std::make_shared<ClientChannel>();
  void * mapped = MAP_FAILED;
  if (ftruncate(fd, kChannelSize) == 0) {
    mapped = mmap(nullptr, kChannelSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  }
  if (
    mapped == MAP_FAILED || fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL) < 0) {
    RCLCPP_WARN(logger, "preparing the subscriber channel failed: %s", strerror(errno));
    if (mapped != MAP_FAILED) munmap(mapped, kChannelSize);
    close(fd);
    return nullptr;
  }
  channel->shm = static_cast<AgnocastSubscriberChannel *>(mapped);
  __atomic_store_n(&channel->shm->magic, AGNOCAST_SUB_CHANNEL_MAGIC, __ATOMIC_RELEASE);
  *memfd = fd;
  return channel;
}

// ============================================================
// Surviving a daemon restart
// ============================================================
//
// When the connection breaks, one thread reconnects, reports what this
// process knows (restore_locked), and repeats the call that failed. A watcher
// thread does the same as soon as the daemon goes away, so the restarted
// daemon's grace period does not wait for this process's next call.
//
//   AGNOCAST_DAEMON_RECONNECT=0               fail as before instead
//   AGNOCAST_DAEMON_RECONNECT_TIMEOUT_MS=<n>  how long a call waits (default 60000)

struct CallOptions
{
  // Runs under socket_mtx right after a successful reply, so a restore cannot
  // slip in between the daemon committing something and this process
  // recording it.
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

// Sends one restore request. -2 when the connection broke again.
int restore_call(uint32_t cmd, const void * req, uint32_t req_size, int send_fd, const char * what)
{
  bool transport_failed = false;
  const int r = daemon_call_locked(cmd, req, req_size, nullptr, 0, send_fd, &transport_failed);
  if (r < 0 && transport_failed) return -2;
  if (r < 0) {
    // A daemon that cannot take this process back answers every retry the
    // same way: say it a few times, not every 5 ms.
    static std::atomic<int> warned{0};
    if (warned.fetch_add(1) < 16) {
      RCLCPP_WARN(logger, "restoring %s after a daemon restart failed: %s", what, strerror(errno));
    }
  }
  return r;
}

// Sends entries in batches. -2 when the connection broke.
int restore_entries(
  const std::string & topic_name, int32_t id, uint32_t kind,
  const std::vector<std::pair<int64_t, uint64_t>> & entries)
{
  RestoreEntriesRequest req{};
  std::strncpy(req.topic_name, topic_name.c_str(), sizeof(req.topic_name) - 1);
  req.pubsub_id = id;
  req.kind = kind;
  for (size_t i = 0; i < entries.size(); i++) {
    req.entries[req.entry_num].entry_id = entries[i].first;
    req.entries[req.entry_num].addr = entries[i].second;
    req.entry_num++;
    if (req.entry_num == AGNOCAST_PROTO_RESTORE_BATCH || i + 1 == entries.size()) {
      if (restore_call(AGNOCAST_CMD_RESTORE_ENTRIES, &req, sizeof(req), -1, "entries") == -2) {
        return -2;
      }
      req.entry_num = 0;
    }
  }
  return 0;
}

// Reports what this process knows to a freshly connected daemon. Caller holds
// socket_mtx. False when the connection broke again or the daemon is not
// recovering (it then knows nothing this process could rely on).
bool restore_locked()
{
  const pid_t self = getpid();
  if (g_process_record.owner != self) {
    // Nothing registered yet: just let the daemon stop waiting for us.
    const int r = restore_call(AGNOCAST_CMD_RESTORE_DONE, nullptr, 0, -1, "nothing");
    return r != -2;
  }

  {
    RestoreProcessRequest req{};
    req.shm_addr = g_process_record.shm_addr;
    req.domain_id = g_process_record.domain_id;
    req.is_performance_bridge_manager = g_process_record.is_performance_bridge_manager;
    const int r = restore_call(AGNOCAST_CMD_RESTORE_PROCESS, &req, sizeof(req), -1, "the process");
    if (r == -2) return false;
    if (r < 0) {
      static std::atomic<bool> warned{false};
      if (!warned.exchange(true)) {
        RCLCPP_ERROR(
          logger,
          "agnocast_daemon restarted without recovering this process (%s); is its state file "
          "gone?",
          strerror(errno));
      }
      return false;
    }
  }

  {
    std::vector<AgnocastRestoreMapping> mappings;
    {
      std::lock_guard<std::mutex> lock(g_region_mtx);
      const uint32_t n = g_regions_owner == self ? g_region_num.load() : 0;
      for (uint32_t i = 0; i < n; i++) {
        if (g_regions[i].addr == g_process_record.shm_addr) continue;
        AgnocastRestoreMapping m{};
        m.pid = g_regions[i].pid;
        m.shm_addr = g_regions[i].addr;
        mappings.push_back(m);
      }
    }
    RestoreMappingsRequest req{};
    for (size_t i = 0; i < mappings.size(); i++) {
      req.mappings[req.mapping_num++] = mappings[i];
      if (req.mapping_num == AGNOCAST_PROTO_RESTORE_BATCH || i + 1 == mappings.size()) {
        if (restore_call(AGNOCAST_CMD_RESTORE_MAPPINGS, &req, sizeof(req), -1, "mappings") == -2) {
          return false;
        }
        req.mapping_num = 0;
      }
    }
  }

  std::vector<std::shared_ptr<PublisherState>> pubs;
  std::vector<std::shared_ptr<SubscriberState>> subs;
  std::unordered_map<std::string, BridgeRecord> bridges;
  std::unordered_map<std::string, uint32_t> ros2_sub;
  std::unordered_map<std::string, uint32_t> ros2_pub;
  {
    std::lock_guard<std::mutex> lock(state_mtx);
    ClientState & st = state_locked();
    for (const auto & [k, p] : st.publishers) {
      (void)k;
      pubs.push_back(p);
    }
    for (const auto & [k, sub] : st.subscribers) {
      (void)k;
      subs.push_back(sub);
    }
    bridges = st.bridges;
    ros2_sub = st.ros2_subscriber_num;
    ros2_pub = st.ros2_publisher_num;
  }

  for (const auto & p : pubs) {
    std::vector<std::pair<int64_t, uint64_t>> entries;
    std::vector<std::pair<int64_t, uint64_t>> pending;
    RestorePublisherRequest req{};
    {
      std::lock_guard<std::mutex> lock(p->mtx);
      std::memcpy(req.topic_name, p->req.topic_name, sizeof(req.topic_name));
      std::memcpy(req.node_name, p->req.node_name, sizeof(req.node_name));
      req.publisher_id = p->id;
      req.qos_depth = p->req.qos_depth;
      req.last_entry_id = p->last_entry_id;
      req.qos_is_transient_local = p->req.qos_is_transient_local;
      req.is_bridge = p->req.is_bridge;
      entries.assign(p->entries.begin(), p->entries.end());
      for (const uint64_t addr : p->pending) pending.emplace_back(-1, addr);
    }
    if (restore_call(AGNOCAST_CMD_RESTORE_PUBLISHER, &req, sizeof(req), -1, "a publisher") == -2) {
      return false;
    }
    if (
      restore_entries(p->topic_name, p->id, AGNOCAST_RESTORE_ENTRY_PUBLISHED, entries) == -2 ||
      restore_entries(p->topic_name, p->id, AGNOCAST_RESTORE_ENTRY_PENDING, pending) == -2) {
      return false;
    }
  }

  for (const auto & sub : subs) {
    std::vector<std::pair<int64_t, uint64_t>> held;
    std::vector<std::pair<int64_t, uint64_t>> recent;
    RestoreSubscriberRequest req{};
    {
      // Retire the channel and read the record at once: a receive or release
      // either happened before (and is in the record) or comes after, over a
      // request to the restored daemon. Pushed entries the user has not taken
      // yet are not held: the daemon delivers them again.
      std::lock_guard<std::mutex> lock(sub->mtx);
      sub->channel.reset();
      std::memcpy(req.topic_name, sub->req.topic_name, sizeof(req.topic_name));
      std::memcpy(req.node_name, sub->req.node_name, sizeof(req.node_name));
      req.subscriber_id = sub->id;
      req.qos_depth = sub->req.qos_depth;
      req.latest_received_entry_id = sub->latest_received_entry_id;
      req.qos_is_transient_local = sub->req.qos_is_transient_local;
      req.qos_is_reliable = sub->req.qos_is_reliable;
      req.is_take_sub = sub->req.is_take_sub;
      req.ignore_local_publications = sub->req.ignore_local_publications;
      req.is_bridge = sub->req.is_bridge;
      held.assign(sub->held.begin(), sub->held.end());
      for (const auto & [pub_id, newest] : sub->newest_by_publisher) {
        (void)pub_id;
        recent.push_back(newest);
      }
    }
    if (
      restore_call(
        AGNOCAST_CMD_RESTORE_SUBSCRIBER, &req, sizeof(req), sub->eventfd, "a subscriber") == -2) {
      return false;
    }
    if (
      restore_entries(sub->topic_name, sub->id, AGNOCAST_RESTORE_ENTRY_HELD, held) == -2 ||
      restore_entries(sub->topic_name, sub->id, AGNOCAST_RESTORE_ENTRY_DELIVERED, recent) == -2) {
      return false;
    }

    int memfd = -1;
    auto channel = make_channel(&memfd);
    if (channel) {
      AttachSubscriberChannelRequest areq{};
      std::strncpy(areq.topic_name, sub->topic_name.c_str(), sizeof(areq.topic_name) - 1);
      areq.subscriber_id = sub->id;
      const int r = restore_call(
        AGNOCAST_CMD_ATTACH_SUBSCRIBER_CHANNEL, &areq, sizeof(areq), memfd, "a channel");
      close(memfd);
      if (r == -2) return false;
      if (r == 0) {
        std::lock_guard<std::mutex> lock(sub->mtx);
        sub->channel = std::move(channel);
      }
    }
  }

  for (const auto & [name, br] : bridges) {
    for (const bool is_r2a : {true, false}) {
      if (is_r2a ? !br.has_r2a : !br.has_a2r) continue;
      AddBridgeRequest req{};
      std::strncpy(req.topic_name, name.c_str(), sizeof(req.topic_name) - 1);
      req.is_r2a = is_r2a;
      if (restore_call(AGNOCAST_CMD_ADD_BRIDGE, &req, sizeof(req), -1, "a bridge") == -2) {
        return false;
      }
    }
  }
  for (const auto & [name, num] : ros2_sub) {
    SetRos2SubscriberNumRequest req{};
    std::strncpy(req.topic_name, name.c_str(), sizeof(req.topic_name) - 1);
    req.ros2_subscriber_num = num;
    if (
      restore_call(AGNOCAST_CMD_SET_ROS2_SUBSCRIBER_NUM, &req, sizeof(req), -1, "a count") == -2) {
      return false;
    }
  }
  for (const auto & [name, num] : ros2_pub) {
    SetRos2PublisherNumRequest req{};
    std::strncpy(req.topic_name, name.c_str(), sizeof(req.topic_name) - 1);
    req.ros2_publisher_num = num;
    if (restore_call(AGNOCAST_CMD_SET_ROS2_PUBLISHER_NUM, &req, sizeof(req), -1, "a count") == -2) {
      return false;
    }
  }
  return restore_call(AGNOCAST_CMD_RESTORE_DONE, nullptr, 0, -1, "the process") == 0;
}

// Connects to the daemon until deadline_ns (< 0: forever) and restores this
// process over the new connection.
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
          logger, "reconnected to agnocast_daemon and restored in %.3f ms",
          static_cast<double>(monotonic_now_ns() - begin) / 1e6);
        return true;
      }
    }
    if (deadline_ns >= 0 && monotonic_now_ns() >= deadline_ns) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
}

// Called by a thread whose call broke on connection generation failed_gen.
// True once a newer connection is in place.
bool recover_connection(uint64_t failed_gen, int64_t timeout_ms)
{
  std::unique_lock<std::mutex> lk(g_recover_mtx);
  if (g_conn_generation.load() != failed_gen) return true;
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
// restarted daemon hears from this process promptly.
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
      recover_connection(gen, -1);
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

// Returns 0 on success, sets errno on failure. See daemon_call_locked. When
// the daemon has gone away, waits for it to come back, restores, and repeats.
static int daemon_call(
  uint32_t cmd, const void * req_payload, uint32_t req_size, void * resp_payload,
  uint32_t resp_size, int send_fd = -1, const CallOptions * opts = nullptr)
{
  while (true) {
    const uint64_t gen = g_conn_generation.load();
    bool transport_failed = false;
    int r = 0;
    int saved_errno = 0;
    {
      std::lock_guard<std::mutex> lock(socket_mtx);
      r = daemon_call_locked(
        cmd, req_payload, req_size, resp_payload, resp_size, send_fd, &transport_failed);
      saved_errno = errno;
      if (r == 0 && opts != nullptr && opts->on_success != nullptr) opts->on_success(opts->ctx);
    }
    if (r == 0 || !transport_failed || !reconnect_enabled()) {
      errno = saved_errno;
      return r;
    }
    if (!recover_connection(gen, reconnect_timeout_ms())) {
      errno = saved_errno;
      return -1;
    }
  }
}

void daemon_note_mapped_region(pid_t pid, uint64_t addr, uint64_t size)
{
  std::lock_guard<std::mutex> lock(g_region_mtx);
  if (g_regions_owner != getpid()) {
    g_regions_owner = getpid();
    g_region_num.store(0);
  }
  const uint32_t n = g_region_num.load(std::memory_order_relaxed);
  for (uint32_t i = 0; i < n; i++) {
    if (g_regions[i].pid == pid && g_regions[i].addr == addr) return;
  }
  if (n == kMaxMappedRegions) return;
  g_regions[n] = MappedRegion{pid, addr, size};
  g_region_num.store(n + 1, std::memory_order_release);
}

uint64_t daemon_reconnect_count()
{
  return g_reconnect_count.load();
}

int agnocast_ipc_get_recovery_status(DaemonRecoveryStatus * out)
{
  GetRecoveryStatusResponse resp{};
  const int r = daemon_call(AGNOCAST_CMD_GET_RECOVERY_STATUS, nullptr, 0, &resp, sizeof(resp));
  if (r == 0) {
    out->recovering = resp.recovering != 0;
    out->expected_num = resp.expected_num;
    out->restored_num = resp.restored_num;
    out->dead_num = resp.dead_num;
    out->fenced_num = resp.fenced_num;
    out->claim_num = resp.claim_num;
    out->rejected_claim_num = resp.rejected_claim_num;
    out->orphan_claim_num = resp.orphan_claim_num;
    out->dedup_publish_num = resp.dedup_publish_num;
    out->recovery_num = resp.recovery_num;
    out->grace_ns = resp.grace_ns;
    out->load_ns = resp.load_ns;
  }
  return r;
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
  // No heap here: initialize_agnocast() runs before the allocator is ready.
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    // A bridge manager that is told another one exists gets no pool.
    if (c->req->is_performance_bridge_manager && c->resp->performance_bridge_daemon_exist) return;
    g_process_record.owner = getpid();
    g_process_record.domain_id = c->req->domain_id;
    g_process_record.is_performance_bridge_manager = c->req->is_performance_bridge_manager;
    g_process_record.shm_addr = c->resp->shm_addr;
  };
  int r = daemon_call(AGNOCAST_CMD_ADD_PROCESS, &req, sizeof(req), &resp, sizeof(resp), -1, &opts);
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
  ensure_watcher();
  AddPublisherRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  copy_name(req.node_name, sizeof(req.node_name), args->node_name);
  req.qos_depth = args->qos_depth;
  req.qos_is_transient_local = args->qos_is_transient_local;
  req.is_bridge = args->is_bridge;
  AddPublisherResponse resp{};
  struct Ctx
  {
    const AddPublisherRequest * req;
    const AddPublisherResponse * resp;
  } ctx{&req, &resp};
  CallOptions opts;
  opts.ctx = &ctx;
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    auto pub = std::make_shared<PublisherState>();
    pub->topic_name = c->req->topic_name;
    pub->id = c->resp->publisher_id;
    pub->req = *c->req;
    std::lock_guard<std::mutex> lock(state_mtx);
    state_locked().publishers[pubsub_key(pub->topic_name, pub->id)] = std::move(pub);
  };
  int r =
    daemon_call(AGNOCAST_CMD_ADD_PUBLISHER, &req, sizeof(req), &resp, sizeof(resp), -1, &opts);
  if (r == 0) {
    args->ret_id = resp.publisher_id;
  }
  return r;
}

int agnocast_ipc_add_subscriber(union ioctl_add_subscriber_args * args)
{
  ensure_watcher();
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
  // ret_id overlaps topic_name in the union, so take the name first.
  const struct name_info topic_name = args->topic_name;
  struct Ctx
  {
    const AddSubscriberRequest * req;
    const AddSubscriberResponse * resp;
    int eventfd;
    std::shared_ptr<SubscriberState> * out;
  };
  std::shared_ptr<SubscriberState> sub;
  Ctx ctx{&req, &resp, args->eventfd, &sub};
  CallOptions opts;
  opts.ctx = &ctx;
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    auto s = std::make_shared<SubscriberState>();
    s->topic_name = c->req->topic_name;
    s->id = c->resp->subscriber_id;
    s->req = *c->req;
    s->eventfd = c->eventfd;
    s->latest_received_entry_id = c->resp->latest_received_entry_id;
    *c->out = s;
    std::lock_guard<std::mutex> lock(state_mtx);
    state_locked().subscribers[pubsub_key(s->topic_name, s->id)] = std::move(s);
  };
  // args->eventfd is the subscriber's notification eventfd; the daemon writes
  // to it on publish, the way the kernel module signals the eventfd_ctx.
  int r = daemon_call(
    AGNOCAST_CMD_ADD_SUBSCRIBER, &req, sizeof(req), &resp, sizeof(resp), args->eventfd, &opts);
  if (r == 0) {
    args->ret_id = resp.subscriber_id;
    // A subscriber without a channel still works over plain requests, so
    // failing here is reported and otherwise ignored.
    int memfd = -1;
    auto channel = make_channel(&memfd);
    if (channel && sub) {
      AttachSubscriberChannelRequest areq{};
      copy_name(areq.topic_name, sizeof(areq.topic_name), topic_name);
      areq.subscriber_id = resp.subscriber_id;
      struct AttachCtx
      {
        SubscriberState * sub;
        std::shared_ptr<ClientChannel> * channel;
      } actx{sub.get(), &channel};
      CallOptions aopts;
      aopts.ctx = &actx;
      aopts.on_success = [](void * p) {
        const auto * c = static_cast<AttachCtx *>(p);
        std::lock_guard<std::mutex> lock(c->sub->mtx);
        c->sub->channel = std::move(*c->channel);
      };
      // A restore in between already attached a fresh channel: EEXIST.
      if (
        daemon_call(
          AGNOCAST_CMD_ATTACH_SUBSCRIBER_CHANNEL, &areq, sizeof(areq), nullptr, 0, memfd, &aopts) <
          0 &&
        errno != EEXIST) {
        RCLCPP_WARN(
          logger, "AGNOCAST_CMD_ATTACH_SUBSCRIBER_CHANNEL failed: %s; using plain requests",
          strerror(errno));
      }
    }
    if (memfd >= 0) close(memfd);
  }
  return r;
}

int agnocast_ipc_publish_msg(union ioctl_publish_msg_args * args)
{
  AGNOCAST_BENCH_CALL_ENTER();

  PublishMsgRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.publisher_id = args->publisher_id;
  req.msg_virtual_address = args->msg_virtual_address;
  PublishMsgResponse resp{};

  // Pending before the request: if the daemon commits it and dies before
  // answering, the restarted one must hear of the address.
  const auto pub = find_publisher(args->topic_name, args->publisher_id);
  std::unique_lock<std::mutex> one_at_a_time;
  if (pub) {
    one_at_a_time = std::unique_lock<std::mutex>(pub->publish_mtx);
    std::lock_guard<std::mutex> lock(pub->mtx);
    pub->pending.insert(req.msg_virtual_address);
  }
  struct Ctx
  {
    PublisherState * pub;
    const PublishMsgRequest * req;
    const PublishMsgResponse * resp;
  } ctx{pub.get(), &req, &resp};
  CallOptions opts;
  opts.ctx = &ctx;
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    if (c->pub == nullptr) return;
    std::lock_guard<std::mutex> lock(c->pub->mtx);
    c->pub->pending.erase(c->req->msg_virtual_address);
    c->pub->entries[c->resp->entry_id] = c->req->msg_virtual_address;
    c->pub->last_entry_id = std::max(c->pub->last_entry_id, c->resp->entry_id);
    for (uint32_t i = 0; i < c->resp->released_num && i < AGNOCAST_PROTO_MAX_RELEASE_NUM; i++) {
      for (auto it = c->pub->entries.begin(); it != c->pub->entries.end(); ++it) {
        if (it->second == c->resp->released_addrs[i]) {
          c->pub->entries.erase(it);
          break;
        }
      }
    }
  };
  int r = daemon_call(AGNOCAST_CMD_PUBLISH_MSG, &req, sizeof(req), &resp, sizeof(resp), -1, &opts);
  if (r == 0) {
    args->ret_entry_id = resp.entry_id;
    args->ret_released_num = resp.released_num;
    for (uint32_t i = 0; i < resp.released_num && i < AGNOCAST_PROTO_MAX_RELEASE_NUM; i++) {
      args->ret_released_addrs[i] = resp.released_addrs[i];
    }
  } else if (pub) {
    std::lock_guard<std::mutex> lock(pub->mtx);
    pub->pending.erase(req.msg_virtual_address);
  }

#ifdef AGNOCAST_BENCH_TIMING
  last_publish_notify_end_ns = g_stamps.daemon_notify_end;
#endif
  AGNOCAST_BENCH_CALL_LEAVE(publish);
  return r;
}

namespace
{

// The pools a reply asks this process to map, minus those it already has: a
// restarted daemon may list them again.
uint32_t copy_new_pools(
  const AgnocastPublisherShmInfo * infos, uint32_t num, uint64_t out_addr, uint32_t out_size)
{
  auto * out = reinterpret_cast<publisher_shm_info *>(out_addr);
  uint32_t copied = 0;
  for (uint32_t i = 0; i < num && copied < out_size; i++) {
    if (region_mapped(infos[i].pid)) continue;
    out[copied].pid = infos[i].pid;
    out[copied].shm_addr = infos[i].shm_addr;
    out[copied].shm_size = infos[i].shm_size;
    copied++;
  }
  return copied;
}

}  // namespace

int agnocast_ipc_receive_msg(union ioctl_receive_msg_args * args)
{
  AGNOCAST_BENCH_CALL_ENTER();

  // ioctl_receive_msg_args is a union: the output ret_* fields overlap the input
  // pub_shm_info_addr/size. The kernel reads inputs before writing outputs, but here
  // we must snapshot the caller-provided buffer pointer/size before writing any ret_*
  // field, otherwise writing ret_entry_ids[]/ret_entry_addrs[] clobbers them.
  const uint64_t pub_shm_info_addr = args->pub_shm_info_addr;
  const uint32_t pub_shm_info_size = args->pub_shm_info_size;
  const struct name_info topic_name = args->topic_name;
  const int32_t subscriber_id = args->subscriber_id;

  const auto sub = find_subscriber(topic_name, subscriber_id);
  if (sub) {
    std::lock_guard<std::mutex> lock(sub->mtx);
    if (take_pushed_locked(*sub, args)) {
      AGNOCAST_BENCH_CALL_LEAVE_LOCAL(receive);
      return 0;
    }
  }

  ReceiveMsgRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), topic_name);
  req.subscriber_id = subscriber_id;
  ReceiveMsgResponse resp{};
  struct Ctx
  {
    SubscriberState * sub;
    const ReceiveMsgResponse * resp;
  } ctx{sub.get(), &resp};
  CallOptions opts;
  opts.ctx = &ctx;
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    if (c->sub == nullptr) return;
    std::lock_guard<std::mutex> lock(c->sub->mtx);
    for (uint16_t i = 0; i < c->resp->entry_num && i < AGNOCAST_PROTO_MAX_RECEIVE_NUM; i++) {
      c->sub->note_delivered(
        c->resp->entry_ids[i], c->resp->entry_addrs[i], c->resp->publisher_ids[i]);
    }
  };
  int r = daemon_call(AGNOCAST_CMD_RECEIVE_MSG, &req, sizeof(req), &resp, sizeof(resp), -1, &opts);
  if (r == 0) {
    args->ret_entry_num = resp.entry_num;
    args->ret_call_again = resp.call_again;
    for (uint16_t i = 0; i < resp.entry_num && i < AGNOCAST_PROTO_MAX_RECEIVE_NUM; i++) {
      args->ret_entry_ids[i] = resp.entry_ids[i];
      args->ret_entry_addrs[i] = resp.entry_addrs[i];
    }
    // Copy pub_shm_infos into the caller-provided buffer (snapshotted above).
    args->ret_pub_shm_num =
      copy_new_pools(resp.pub_shm_infos, resp.pub_shm_num, pub_shm_info_addr, pub_shm_info_size);
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

  TakeMsgRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.subscriber_id = args->subscriber_id;
  req.allow_same_message = args->allow_same_message;
  TakeMsgResponse resp{};
  const auto sub = find_subscriber(args->topic_name, args->subscriber_id);
  struct Ctx
  {
    SubscriberState * sub;
    const TakeMsgResponse * resp;
  } ctx{sub.get(), &resp};
  CallOptions opts;
  opts.ctx = &ctx;
  opts.on_success = [](void * p) {
    const auto * c = static_cast<Ctx *>(p);
    if (c->sub == nullptr || c->resp->entry_id < 0) return;
    std::lock_guard<std::mutex> lock(c->sub->mtx);
    c->sub->note_delivered(c->resp->entry_id, c->resp->addr, c->resp->publisher_id);
  };
  int r = daemon_call(AGNOCAST_CMD_TAKE_MSG, &req, sizeof(req), &resp, sizeof(resp), -1, &opts);
  if (r == 0) {
    args->ret_addr = resp.addr;
    args->ret_entry_id = resp.entry_id;
    args->ret_pub_shm_num =
      copy_new_pools(resp.pub_shm_infos, resp.pub_shm_num, pub_shm_info_addr, pub_shm_info_size);
  }
  return r;
}

int agnocast_ipc_release_sub_ref(struct ioctl_update_entry_args * args)
{
  // Forgotten before it is reported, and the channel read under the same lock
  // a restore takes: see restore_locked().
  std::shared_ptr<ClientChannel> channel;
  if (const auto sub = find_subscriber(args->topic_name, args->pubsub_id)) {
    std::lock_guard<std::mutex> lock(sub->mtx);
    sub->held.erase(args->entry_id);
    channel = sub->channel;
  }
  if (channel && queue_release(*channel, args->entry_id)) return 0;

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
  {
    std::lock_guard<std::mutex> lock(state_mtx);
    state_locked().publishers.erase(pubsub_key(req.topic_name, req.publisher_id));
  }
  const uint64_t gen = g_conn_generation.load();
  int r = daemon_call(AGNOCAST_CMD_REMOVE_PUBLISHER, &req, sizeof(req), nullptr, 0);
  if (r < 0 && (errno == ENODATA || errno == EINVAL) && g_conn_generation.load() != gen) {
    r = 0;  // the restarted daemon never heard of it
  }
  return r;
}

int agnocast_ipc_remove_subscriber(struct ioctl_remove_subscriber_args * args)
{
  RemoveSubscriberRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.subscriber_id = args->subscriber_id;
  {
    std::lock_guard<std::mutex> lock(state_mtx);
    state_locked().subscribers.erase(pubsub_key(req.topic_name, req.subscriber_id));
  }
  const uint64_t gen = g_conn_generation.load();
  int r = daemon_call(AGNOCAST_CMD_REMOVE_SUBSCRIBER, &req, sizeof(req), nullptr, 0);
  if (r < 0 && (errno == ENODATA || errno == EINVAL) && g_conn_generation.load() != gen) {
    r = 0;  // the restarted daemon never heard of it
  }
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
    {
      std::lock_guard<std::mutex> lock(state_mtx);
      BridgeRecord & br = state_locked().bridges[req.topic_name];
      br.has_r2a = resp.has_r2a;
      br.has_a2r = resp.has_a2r;
    }
    args->ret_pid = resp.pid;
    args->ret_has_r2a = resp.has_r2a;
    args->ret_has_a2r = resp.has_a2r;
  }
  return r;
}

int agnocast_ipc_remove_bridge(struct ioctl_remove_bridge_args * args)
{
  RemoveBridgeRequest req{};
  copy_name(req.topic_name, sizeof(req.topic_name), args->topic_name);
  req.is_r2a = args->is_r2a;
  {
    std::lock_guard<std::mutex> lock(state_mtx);
    auto & bridges = state_locked().bridges;
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
    std::lock_guard<std::mutex> lock(state_mtx);
    state_locked().ros2_subscriber_num[req.topic_name] = req.ros2_subscriber_num;
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
    std::lock_guard<std::mutex> lock(state_mtx);
    state_locked().ros2_publisher_num[req.topic_name] = req.ros2_publisher_num;
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

}  // namespace agnocast

#endif  // AGNOCAST_USE_DAEMON
