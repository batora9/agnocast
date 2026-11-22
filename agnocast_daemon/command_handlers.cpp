// SPDX-License-Identifier: Apache-2.0
#include "command_handlers.hpp"

#include "bench_timing.hpp"

#include <fcntl.h>
#include <mqueue.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#ifndef AGNOCAST_VERSION
#define AGNOCAST_VERSION "2.3.5"
#endif

namespace
{

SubscriberInfo * find_subscriber_info(TopicWrapper * wrapper, int32_t subscriber_id)
{
  auto it = wrapper->topic.sub_info_map.find(subscriber_id);
  return (it != wrapper->topic.sub_info_map.end()) ? &it->second : nullptr;
}

PublisherInfo * find_publisher_info(TopicWrapper * wrapper, int32_t publisher_id)
{
  auto it = wrapper->topic.pub_info_map.find(publisher_id);
  return (it != wrapper->topic.pub_info_map.end()) ? &it->second : nullptr;
}

// Mirrors agnocastlib create_shm_name / create_mq_name_for_agnocast_publish.
std::string make_shm_name(pid_t pid)
{
  return "/agnocast@" + std::to_string(pid);
}

std::string make_mq_name_for_publish(const std::string & topic_name, int32_t subscriber_id)
{
  if (topic_name.empty() || topic_name[0] != '/') {
    return {};
  }
  std::string mq_name = topic_name;
  mq_name[0] = '@';
  mq_name = "/agnocast" + mq_name + "@" + std::to_string(subscriber_id);
  for (size_t i = 1; i < mq_name.size(); ++i) {
    if (mq_name[i] == '/') {
      mq_name[i] = '_';
    }
  }
  return mq_name;
}

int64_t monotonic_ns()
{
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
}

int64_t env_ms(const char * name, int64_t fallback)
{
  const char * v = std::getenv(name);
  if (v == nullptr || v[0] == '\0') return fallback;
  const long long n = std::strtoll(v, nullptr, 10);
  return n >= 0 ? static_cast<int64_t>(n) : fallback;
}

// Field 22 of /proc/<pid>/stat and the state letter (field 3), or 0 and '?'
// when they cannot be read (another pid namespace, no /proc).
uint64_t proc_starttime(pid_t pid, char * state = nullptr)
{
  if (state != nullptr) *state = '?';
  char path[64];
  std::snprintf(path, sizeof(path), "/proc/%d/stat", static_cast<int>(pid));
  std::ifstream ifs(path);
  std::string line;
  if (!std::getline(ifs, line)) return 0;
  // The command name may contain spaces and parentheses: start after the last ')'.
  const size_t rp = line.rfind(')');
  if (rp == std::string::npos || rp + 2 >= line.size()) return 0;
  const char * p = line.c_str() + rp + 2;
  if (state != nullptr) *state = *p;
  for (int field = 3; field < 22; field++) {
    p = std::strchr(p, ' ');
    if (p == nullptr) return 0;
    p++;
  }
  return std::strtoull(p, nullptr, 10);
}

// False only when the process (pid, starttime) is known to be gone: kill()
// says ESRCH, the pid now names a process started at another time, or it is
// a zombie. What cannot be told counts as alive (taking a live client for
// dead would let a publisher reuse memory it still reads).
bool proc_alive(pid_t pid, uint64_t starttime)
{
  if (pid <= 0) return false;
  if (kill(pid, 0) != 0 && errno == ESRCH) return false;
  char state = '?';
  const uint64_t now = proc_starttime(pid, &state);
  if (state == 'Z' || state == 'X') return false;
  if (starttime != 0 && now != 0 && now != starttime) return false;
  return true;
}

// Fault injection for the restart recovery evaluation: with
// AGNOCAST_DAEMON_FAULT_POINT=<name> and AGNOCAST_DAEMON_FAULT_HIT=<n>, the
// daemon SIGKILLs itself the n-th time it passes that point. Points:
//   publish_before_reply  a publish committed and pushed, its reply not sent
//   receive_before_reply  a receive committed, its reply not sent
void fault_point(const char * name)
{
  static const char * armed = std::getenv("AGNOCAST_DAEMON_FAULT_POINT");
  if (armed == nullptr || std::strcmp(armed, name) != 0) return;
  static const long hit = std::max(
    1L,
    std::strtol(
      std::getenv("AGNOCAST_DAEMON_FAULT_HIT") != nullptr ? std::getenv("AGNOCAST_DAEMON_FAULT_HIT")
                                                          : "1",
      nullptr, 10));
  static std::atomic<long> passed{0};
  if (passed.fetch_add(1) + 1 == hit) {
    fprintf(stderr, "agnocast_daemon: fault point %s reached; killing itself\n", name);
    kill(getpid(), SIGKILL);
  }
}

// Notification targets collected under the topic lock, written to after it is
// released. Reused across calls so the publish path allocates nothing once the
// high-water mark is reached.
thread_local std::vector<int> t_notify_fds;

// Mirrors agnocast_kmod's notify loop over publisher_info::notify_ctxs. The
// kernel module signals each subscriber's eventfd_ctx; the daemon holds the
// descriptors themselves (received over SCM_RIGHTS at registration) and writes
// to them, which is the same wakeup with one write(2) per subscriber.
void notify_subscribers(const std::vector<int> & notify_fds)
{
  const uint64_t one = 1;
  for (const int efd : notify_fds) {
    // An eventfd counter saturates at UINT64_MAX-1 and only then returns EAGAIN,
    // which means the subscriber has not drained an earlier notification yet —
    // it is already awake, so there is nothing to report.
    if (write(efd, &one, sizeof(one)) == -1 && errno != EAGAIN) {
      fprintf(stderr, "agnocast_daemon: eventfd write failed (fd=%d): %s\n", efd, strerror(errno));
    }
  }
}

// Unix seqpacket messages are capped by net.core.wmem_max (~208 KiB here). The
// inline query responses are much larger if sent in full, so send only the
// prefix that contains topic_num/entry_num and the used slots.
static_assert(offsetof(GetTopicListResponse, topic_names) == sizeof(uint32_t));
static_assert(offsetof(GetNodeTopicsResponse, topic_names) == sizeof(uint32_t));
static_assert(offsetof(GetTopicSubscriberInfoResponse, entries) == sizeof(uint32_t));
static_assert(offsetof(GetTopicPublisherInfoResponse, entries) == sizeof(uint32_t));

uint32_t packed_topic_list_bytes(uint32_t topic_num)
{
  return static_cast<uint32_t>(
    sizeof(uint32_t) + static_cast<size_t>(topic_num) * AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE);
}

uint32_t packed_topic_info_bytes(uint32_t entry_num)
{
  return static_cast<uint32_t>(
    sizeof(uint32_t) + static_cast<size_t>(entry_num) * sizeof(AgnocastTopicInfoEntry));
}

void unlink_posix_resources_for_exit(
  pid_t pid, const std::vector<ExitSubscriptionEntry> & exit_subscriptions)
{
  const std::string shm_name = make_shm_name(pid);
  if (shm_unlink(shm_name.c_str()) < 0 && errno != ENOENT) {
    fprintf(
      stderr, "agnocast_daemon: shm_unlink(%s) failed (pid=%d): %s\n", shm_name.c_str(), pid,
      strerror(errno));
  }

  for (const auto & entry : exit_subscriptions) {
    const std::string mq_name = make_mq_name_for_publish(entry.topic_name, entry.subscriber_id);
    if (mq_name.empty()) continue;
    if (mq_unlink(mq_name.c_str()) < 0 && errno != ENOENT) {
      fprintf(
        stderr, "agnocast_daemon: mq_unlink(%s) failed (pid=%d): %s\n", mq_name.c_str(), pid,
        strerror(errno));
    }
  }
}

EntryNode * find_message_entry(TopicStruct & topic, int64_t entry_id)
{
  auto it = topic.entries.find(entry_id);
  return (it != topic.entries.end()) ? &it->second : nullptr;
}

bool is_referenced(const EntryNode & en)
{
  return en.referencing_subscribers.any();
}

int add_subscriber_reference(EntryNode & en, int32_t id)
{
  if (id < 0 || id >= static_cast<int32_t>(AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID)) {
    return -EINVAL;
  }
  if (en.referencing_subscribers.test_and_set(static_cast<size_t>(id))) {
    return -EALREADY;
  }
  return 0;
}

// Applies the releases a subscriber has queued in its channel's release ring.
// The caller holds the topic lock (shared is enough: the reference bits are
// atomic and the entries map is not modified); the channel mutex keeps two
// shared holders from consuming the same slots.
void drain_release_ring(TopicWrapper * wrapper, SubscriberInfo & sub)
{
  SubscriberChannel * channel = sub.channel.get();
  if (channel == nullptr) return;

  std::lock_guard<std::mutex> lock(channel->mtx);
  AgnocastReleaseRing & ring = channel->shm->release;
  const uint64_t head = __atomic_load_n(&ring.head, __ATOMIC_ACQUIRE);
  uint64_t tail = channel->release_tail;
  if (head - tail > AGNOCAST_RELEASE_RING_SLOTS) {
    // head is the subscriber's to write; one behind tail or more than a ring
    // ahead is not something a well-behaved subscriber produces.
    tail = head;
  }
  for (; tail != head; ++tail) {
    const int64_t entry_id =
      __atomic_load_n(&ring.entry_ids[tail % AGNOCAST_RELEASE_RING_SLOTS], __ATOMIC_RELAXED);
    EntryNode * en = find_message_entry(wrapper->topic, entry_id);
    if (en != nullptr) en->referencing_subscribers.test_and_clear(static_cast<size_t>(sub.id));
  }
  channel->release_tail = tail;
  __atomic_store_n(&ring.tail, tail, __ATOMIC_RELEASE);
}

void drain_release_rings(TopicWrapper * wrapper)
{
  for (auto & [id, sub] : wrapper->topic.sub_info_map) {
    (void)id;
    drain_release_ring(wrapper, sub);
  }
}

int insert_subscriber_info(
  TopicWrapper * wrapper, const char * node_name, pid_t subscriber_pid, uint32_t qos_depth,
  bool qos_is_transient_local, bool qos_is_reliable, bool is_take_sub,
  bool ignore_local_publications, bool is_bridge, SubscriberInfo ** out)
{
  if (wrapper->topic.sub_info_map.size() >= AGNOCAST_PROTO_MAX_SUBSCRIBER_NUM) {
    return -ENOBUFS;
  }
  if (wrapper->topic.current_pubsub_id >= static_cast<int32_t>(AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID)) {
    return -ENOSPC;
  }

  const int32_t new_id = wrapper->topic.current_pubsub_id++;
  SubscriberInfo info;
  info.id = new_id;
  info.domain_id = wrapper->domain_id;
  info.pid = subscriber_pid;
  info.qos_depth = qos_depth;
  info.qos_is_transient_local = qos_is_transient_local;
  info.qos_is_reliable = qos_is_reliable;
  info.latest_received_entry_id =
    qos_is_transient_local ? -1 : static_cast<int64_t>(wrapper->topic.current_entry_id++);
  info.node_name = node_name;
  info.is_take_sub = is_take_sub;
  info.ignore_local_publications = ignore_local_publications;
  info.need_mmap_update = true;
  info.is_bridge = is_bridge;

  auto [it, inserted] = wrapper->topic.sub_info_map.emplace(new_id, std::move(info));
  if (!inserted) return -ENOMEM;
  *out = &it->second;
  return 0;
}

int insert_publisher_info(
  TopicWrapper * wrapper, const char * node_name, pid_t publisher_pid, uint32_t qos_depth,
  bool qos_is_transient_local, bool is_bridge, PublisherInfo ** out)
{
  if (wrapper->topic.pub_info_map.size() >= AGNOCAST_PROTO_MAX_PUBLISHER_NUM) {
    return -ENOBUFS;
  }
  if (wrapper->topic.current_pubsub_id >= static_cast<int32_t>(AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID)) {
    return -ENOSPC;
  }

  const int32_t new_id = wrapper->topic.current_pubsub_id++;
  PublisherInfo info;
  info.id = new_id;
  info.domain_id = wrapper->domain_id;
  info.pid = publisher_pid;
  info.node_name = node_name;
  info.qos_depth = qos_depth;
  info.qos_is_transient_local = qos_is_transient_local;
  info.entries_num = 0;
  info.is_bridge = is_bridge;

  auto [it, inserted] = wrapper->topic.pub_info_map.emplace(new_id, std::move(info));
  if (!inserted) return -ENOMEM;
  *out = &it->second;
  return 0;
}

int set_publisher_shm_info(
  const TopicWrapper * wrapper, pid_t subscriber_pid, MemoryAllocator & allocator,
  AgnocastPublisherShmInfo * pub_shm_infos, uint32_t pub_shm_infos_size, uint32_t * ret_pub_shm_num,
  MetadataStore & store)
{
  uint32_t publisher_num = 0;
  for (const auto & [id, pub_info] : wrapper->topic.pub_info_map) {
    (void)id;
    if (subscriber_pid == pub_info.pid) continue;

    ProcessInfo * proc_info = store.find_process(pub_info.pid);
    if (!proc_info || proc_info->exited) continue;

    const int ret = allocator.reference_memory(proc_info->mempool_entry, subscriber_pid);
    if (ret < 0) {
      if (ret == -EEXIST) continue;
      return ret;
    }

    if (publisher_num == pub_shm_infos_size) return -ENOBUFS;

    pub_shm_infos[publisher_num].pid = pub_info.pid;
    pub_shm_infos[publisher_num].shm_addr = proc_info->mempool_entry->addr;
    pub_shm_infos[publisher_num].shm_size = allocator.mempool_size_bytes();
    publisher_num++;
  }
  *ret_pub_shm_num = publisher_num;
  return 0;
}

int release_msgs_to_meet_depth(
  TopicWrapper * wrapper, PublisherInfo * pub_info, PublishMsgResponse * resp)
{
  resp->released_num = 0;
  if (pub_info->entries_num <= pub_info->qos_depth) return 0;

  uint32_t num_search_entries = pub_info->entries_num - pub_info->qos_depth;
  auto it = wrapper->topic.entries.begin();

  while (num_search_entries > 0 && resp->released_num < AGNOCAST_PROTO_MAX_RELEASE_NUM) {
    if (it == wrapper->topic.entries.end()) return -ENODATA;

    EntryNode & en = it->second;
    const int64_t entry_id = en.entry_id;
    ++it;

    if (en.publisher_id != pub_info->id) continue;
    num_search_entries--;

    if (is_referenced(en)) continue;

    resp->released_addrs[resp->released_num++] = en.msg_virtual_address;
    wrapper->topic.entries.erase(entry_id);
    pub_info->entries_num--;
  }
  return 0;
}

int receive_msg_core(
  TopicWrapper * wrapper, SubscriberInfo * sub_info, int32_t subscriber_id,
  ReceiveMsgResponse * resp, MetadataStore & store)
{
  resp->entry_num = 0;
  resp->call_again = false;

  if (wrapper->topic.entries.empty()) return 0;

  const int64_t newest_entry_id = wrapper->topic.entries.rbegin()->first;
  const int64_t latest_received_entry_id = sub_info->latest_received_entry_id;
  const int64_t qos_start = newest_entry_id - static_cast<int64_t>(sub_info->qos_depth) + 1;
  const int64_t start_entry_id =
    (qos_start > latest_received_entry_id) ? qos_start : (latest_received_entry_id + 1);

  auto it = wrapper->topic.entries.lower_bound(start_entry_id);
  for (; it != wrapper->topic.entries.end(); ++it) {
    EntryNode & en = it->second;

    if (resp->entry_num == AGNOCAST_PROTO_MAX_RECEIVE_NUM) {
      resp->call_again = true;
      break;
    }

    PublisherInfo * pub_info = find_publisher_info(wrapper, en.publisher_id);
    if (!pub_info) return -ENODATA;

    ProcessInfo * proc_info = store.find_process(pub_info->pid);
    if (!proc_info || proc_info->exited) continue;

    if (sub_info->ignore_local_publications && (sub_info->pid == pub_info->pid)) {
      continue;
    }

    const int ret = add_subscriber_reference(en, subscriber_id);
    if (ret < 0) return ret;

    resp->entry_ids[resp->entry_num] = en.entry_id;
    resp->entry_addrs[resp->entry_num] = en.msg_virtual_address;
    resp->entry_num++;
  }

  if (resp->entry_num > 0) {
    sub_info->latest_received_entry_id = resp->entry_ids[resp->entry_num - 1];
  }
  return 0;
}

// Caller holds channel.mtx.
void set_ipc_pending(SubscriberChannel & channel, bool pending)
{
  channel.ipc_pending = pending;
  __atomic_store_n(&channel.shm->push.ipc_pending, pending ? 1U : 0U, __ATOMIC_RELEASE);
}

// Runs the subscriber's receive at publish time and leaves the result in its
// channel's push ring (see AgnocastPushRing). The caller holds the topic lock
// exclusively and writes the subscriber's eventfd afterwards.
void push_receive(TopicWrapper * wrapper, SubscriberInfo & sub, MetadataStore & store)
{
  SubscriberChannel * channel = sub.channel.get();
  if (channel == nullptr) return;

  std::lock_guard<std::mutex> lock(channel->mtx);
  if (channel->ipc_pending) return;
  if (sub.need_mmap_update) {
    // Publisher shm infos travel only in a receive reply.
    set_ipc_pending(*channel, true);
    return;
  }

  AgnocastPushRing & ring = channel->shm->push;
  const uint64_t tail = __atomic_load_n(&ring.tail, __ATOMIC_ACQUIRE);
  const uint64_t unread = channel->push_head - tail;
  // An implausible tail (the subscriber can write it) lands here as well. The
  // depth check keeps a stalled subscriber from pinning more pushed entries
  // than its keep-last depth; the request path then applies keep-last itself.
  if (
    unread > AGNOCAST_PUSH_RING_SLOTS || unread >= sub.qos_depth ||
    AGNOCAST_PUSH_RING_SLOTS - unread < AGNOCAST_PROTO_MAX_RECEIVE_NUM) {
    set_ipc_pending(*channel, true);
    return;
  }

  thread_local ReceiveMsgResponse resp;
  if (receive_msg_core(wrapper, &sub, sub.id, &resp, store) < 0) {
    // Nothing was delivered, so give back the references it did take.
    for (uint16_t i = 0; i < resp.entry_num; i++) {
      EntryNode * en = find_message_entry(wrapper->topic, resp.entry_ids[i]);
      if (en != nullptr) en->referencing_subscribers.test_and_clear(static_cast<size_t>(sub.id));
    }
    set_ipc_pending(*channel, true);
    return;
  }

  uint64_t head = channel->push_head;
  for (uint16_t i = 0; i < resp.entry_num; i++, head++) {
    AgnocastPushSlot & slot = ring.slots[head % AGNOCAST_PUSH_RING_SLOTS];
    __atomic_store_n(&slot.entry_id, resp.entry_ids[i], __ATOMIC_RELAXED);
    __atomic_store_n(&slot.entry_addr, resp.entry_addrs[i], __ATOMIC_RELAXED);
  }
  channel->push_head = head;
  __atomic_store_n(&ring.head, head, __ATOMIC_RELEASE);
  if (resp.call_again) set_ipc_pending(*channel, true);
}

void cleanup_unreferenced_entries_from_exited_publishers(
  TopicWrapper * wrapper, int32_t subscriber_id, MetadataStore & store)
{
  std::vector<int64_t> to_remove;
  for (auto & [entry_id, en] : wrapper->topic.entries) {
    (void)entry_id;
    en.referencing_subscribers.test_and_clear(static_cast<size_t>(subscriber_id));

    if (is_referenced(en)) continue;

    PublisherInfo * pub_info = find_publisher_info(wrapper, en.publisher_id);
    if (!pub_info) continue;

    ProcessInfo * proc_info = store.find_process(pub_info->pid);
    if (proc_info && !proc_info->exited) continue;

    to_remove.push_back(en.entry_id);
    pub_info->entries_num--;
    if (pub_info->entries_num == 0) {
      wrapper->topic.pub_info_map.erase(pub_info->id);
    }
  }
  for (int64_t eid : to_remove) {
    wrapper->topic.entries.erase(eid);
  }
}

void try_remove_empty_topic(MetadataStore & store, const TopicKey & key)
{
  auto it = store.topic_map_.find(key);
  if (it == store.topic_map_.end()) return;
  TopicWrapper * wrapper = it->second.get();
  if (!wrapper->topic.pub_info_map.empty() || !wrapper->topic.sub_info_map.empty()) return;
  wrapper->topic.entries.clear();
  store.topic_map_.erase(it);
}

const char * notify_mq_topic_name(const TopicWrapper * wrapper)
{
  // Without a domain-bridge rename rule, the canonical MQ topic name equals the wrapper key.
  return wrapper->key.c_str();
}

int get_process_num(const MetadataStore & store)
{
  return static_cast<int>(store.proc_info_map_.size());
}

int get_process_num_in_domain(const MetadataStore & store, uint32_t domain_id)
{
  int count = 0;
  for (const auto & [pid, proc] : store.proc_info_map_) {
    (void)pid;
    if (!proc.exited && proc.domain_id == domain_id) {
      count++;
    }
  }
  return count;
}

bool has_alive_performance_bridge_manager(const MetadataStore & store, uint32_t domain_id)
{
  for (const auto & [pid, proc] : store.proc_info_map_) {
    (void)pid;
    if (proc.domain_id == domain_id && proc.is_performance_bridge_manager && !proc.exited) {
      return true;
    }
  }
  return false;
}

BridgeInfo * find_bridge_info(MetadataStore & store, const std::string & topic_name)
{
  auto it = store.bridge_map_.find(topic_name);
  return (it != store.bridge_map_.end()) ? &it->second : nullptr;
}

}  // namespace

// ============================================================
// CommandHandlers
// ============================================================

CommandHandlers::CommandHandlers(MetadataStore & store, MemoryAllocator & allocator)
: store_(store), allocator_(allocator)
{
}

void CommandHandlers::send_response(int fd, int32_t error_code)
{
  send_response(fd, error_code, nullptr, 0);
}

void CommandHandlers::send_response(
  int fd, int32_t error_code, const void * payload, uint32_t payload_size)
{
  ResponseHeader hdr{error_code, payload_size};
  iovec iov[3];
  int n_iov = 0;
  iov[n_iov++] = {const_cast<ResponseHeader *>(&hdr), sizeof(hdr)};
  if (payload != nullptr && payload_size > 0) {
    iov[n_iov++] = {const_cast<void *>(payload), payload_size};
  }
#ifdef AGNOCAST_BENCH_TIMING
  BenchTimingTrailer trailer{};
  ::agnocast_daemon_bench::fill_trailer(trailer);
  iov[n_iov++] = {&trailer, sizeof(trailer)};
#endif
  msghdr msg{};
  msg.msg_iov = iov;
  msg.msg_iovlen = static_cast<size_t>(n_iov);
  if (sendmsg(fd, &msg, MSG_NOSIGNAL) < 0) {
    std::fprintf(
      stderr, "agnocast_daemon: send_response failed: %s (payload_size=%u)\n", std::strerror(errno),
      payload_size);
  }
}

void CommandHandlers::dispatch(
  int client_fd, pid_t client_pid, const RequestHeader & hdr, const void * payload, int recv_fd)
{
  // Only these two commands have a use for an attached descriptor. Close
  // anything that arrives on another command rather than letting it accumulate.
  if (
    recv_fd >= 0 && hdr.command != AGNOCAST_CMD_ADD_SUBSCRIBER &&
    hdr.command != AGNOCAST_CMD_ATTACH_SUBSCRIBER_CHANNEL &&
    hdr.command != AGNOCAST_CMD_RESTORE_SUBSCRIBER) {
    close(recv_fd);
    recv_fd = -1;
  }

  gate_on_recovery(client_pid, hdr.command);
  static const bool debug_recovery = std::getenv("AGNOCAST_DAEMON_RECOVERY_DEBUG") != nullptr;
  if (debug_recovery && hdr.command >= AGNOCAST_CMD_RESTORE_PROCESS) {
    int32_t id = -1;
    if (hdr.command == AGNOCAST_CMD_RESTORE_PUBLISHER && payload) {
      id = static_cast<const RestorePublisherRequest *>(payload)->publisher_id;
    } else if (hdr.command == AGNOCAST_CMD_RESTORE_SUBSCRIBER && payload) {
      id = static_cast<const RestoreSubscriberRequest *>(payload)->subscriber_id;
    } else if (hdr.command == AGNOCAST_CMD_RESTORE_ENTRIES && payload) {
      id = static_cast<const RestoreEntriesRequest *>(payload)->pubsub_id;
    }
    fprintf(
      stderr, "agnocast_daemon: [debug] fd=%d pid=%d cmd=%u id=%d\n", client_fd, client_pid,
      hdr.command, id);
  }

  switch (static_cast<CommandType>(hdr.command)) {
    case AGNOCAST_CMD_GET_VERSION:
      handle_get_version(client_fd);
      break;
    case AGNOCAST_CMD_ADD_PROCESS:
      handle_add_process(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_ADD_SUBSCRIBER:
      handle_add_subscriber(client_fd, client_pid, payload, recv_fd);
      break;
    case AGNOCAST_CMD_ADD_PUBLISHER:
      handle_add_publisher(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RELEASE_SUB_REF:
      handle_release_sub_ref(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_PUBLISH_MSG:
      handle_publish_msg(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RECEIVE_MSG:
      handle_receive_msg(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_TAKE_MSG:
      handle_take_msg(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_GET_SUBSCRIBER_NUM:
      handle_get_subscriber_num(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_GET_EXIT_PROCESS:
      handle_get_exit_process(client_fd);
      break;
    case AGNOCAST_CMD_GET_SUBSCRIBER_QOS:
      handle_get_subscriber_qos(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_GET_PUBLISHER_QOS:
      handle_get_publisher_qos(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_ADD_BRIDGE:
      handle_add_bridge(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_REMOVE_BRIDGE:
      handle_remove_bridge(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_GET_PUBLISHER_NUM:
      handle_get_publisher_num(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_REMOVE_SUBSCRIBER:
      handle_remove_subscriber(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_REMOVE_PUBLISHER:
      handle_remove_publisher(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_CHECK_AND_REQUEST_BRIDGE_SHUTDOWN:
      handle_check_and_request_bridge_shutdown(client_fd, client_pid);
      break;
    case AGNOCAST_CMD_GET_TOPIC_LIST:
      handle_get_topic_list(client_fd);
      break;
    case AGNOCAST_CMD_GET_TOPIC_SUBSCRIBER_INFO:
      handle_get_topic_subscriber_info(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_GET_TOPIC_PUBLISHER_INFO:
      handle_get_topic_publisher_info(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_GET_NODE_SUBSCRIBER_TOPICS:
      handle_get_node_subscriber_topics(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_GET_NODE_PUBLISHER_TOPICS:
      handle_get_node_publisher_topics(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_SET_ROS2_SUBSCRIBER_NUM:
      handle_set_ros2_subscriber_num(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_SET_ROS2_PUBLISHER_NUM:
      handle_set_ros2_publisher_num(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_NOTIFY_BRIDGE_SHUTDOWN:
      handle_notify_bridge_shutdown(client_fd, client_pid);
      break;
    case AGNOCAST_CMD_ATTACH_SUBSCRIBER_CHANNEL:
      handle_attach_subscriber_channel(client_fd, client_pid, payload, recv_fd);
      break;
    case AGNOCAST_CMD_RESTORE_PROCESS:
      handle_restore_process(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RESTORE_MAPPINGS:
      handle_restore_mappings(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RESTORE_PUBLISHER:
      handle_restore_publisher(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RESTORE_SUBSCRIBER:
      handle_restore_subscriber(client_fd, client_pid, payload, recv_fd);
      break;
    case AGNOCAST_CMD_RESTORE_ENTRIES:
      handle_restore_entries(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RESTORE_DONE:
      handle_restore_done(client_fd, client_pid);
      break;
    case AGNOCAST_CMD_GET_RECOVERY_STATUS:
      handle_get_recovery_status(client_fd);
      break;
    default:
      send_response(client_fd, EINVAL);
      break;
  }
}

void CommandHandlers::handle_get_version(int fd)
{
  GetVersionResponse resp{};
  strncpy(resp.version, AGNOCAST_VERSION, sizeof(resp.version) - 1);
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_add_process(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const AddProcessRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);

  if (store_.find_process(pid)) {
    send_response(fd, EINVAL);
    return;
  }

  AddProcessResponse resp{};
  resp.unlink_daemon_exist = (get_process_num(store_) > 0);
  resp.performance_bridge_daemon_exist =
    has_alive_performance_bridge_manager(store_, req->domain_id);
  resp.discovery_agent_exist = false;

  if (req->is_performance_bridge_manager && resp.performance_bridge_daemon_exist) {
    send_response(fd, 0, &resp, sizeof(resp));
    return;
  }

  ProcessInfo proc;
  proc.exited = false;
  proc.is_performance_bridge_manager = req->is_performance_bridge_manager;
  proc.domain_id = req->domain_id;
  proc.pid = pid;
  proc.starttime = proc_starttime(pid);
  proc.mempool_entry = allocator_.assign_memory(pid);
  if (!proc.mempool_entry) {
    send_response(fd, ENOMEM);
    return;
  }

  resp.shm_addr = proc.mempool_entry->addr;
  resp.shm_size = allocator_.mempool_size_bytes();

  store_.proc_info_map_.emplace(pid, std::move(proc));
  // Before the reply: a client this process knows of is in the state file.
  persist_state_locked();
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_add_subscriber(
  int fd, pid_t pid, const void * payload, int notify_eventfd)
{
  if (!payload) {
    if (notify_eventfd >= 0) close(notify_eventfd);
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const AddSubscriberRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_or_create_topic_for_process(pid, req->topic_name);

  SubscriberInfo * sub_info = nullptr;
  const int ret = insert_subscriber_info(
    wrapper, req->node_name, pid, req->qos_depth, req->qos_is_transient_local, req->qos_is_reliable,
    req->is_take_sub, req->ignore_local_publications, req->is_bridge, &sub_info);
  if (ret < 0) {
    if (notify_eventfd >= 0) close(notify_eventfd);
    send_response(fd, -ret);
    return;
  }

  // The daemon owns this descriptor from here on; it is closed when the
  // subscriber is removed or its process exits.
  sub_info->notify_eventfd = notify_eventfd;

  AddSubscriberResponse resp{};
  resp.subscriber_id = sub_info->id;
  resp.latest_received_entry_id = sub_info->latest_received_entry_id;
  strncpy(
    resp.mq_topic_name, notify_mq_topic_name(wrapper), AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE - 1);
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_add_publisher(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const AddPublisherRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_or_create_topic_for_process(pid, req->topic_name);

  PublisherInfo * pub_info = nullptr;
  const int ret = insert_publisher_info(
    wrapper, req->node_name, pid, req->qos_depth, req->qos_is_transient_local, req->is_bridge,
    &pub_info);
  if (ret < 0) {
    send_response(fd, -ret);
    return;
  }

  for (auto & [id, sub] : wrapper->topic.sub_info_map) {
    (void)id;
    sub.need_mmap_update = true;
  }

  AddPublisherResponse resp{};
  resp.publisher_id = pub_info->id;
  strncpy(
    resp.mq_topic_name, notify_mq_topic_name(wrapper), AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE - 1);
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_release_sub_ref(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const ReleaseSubRefRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  // After a restart, a reference the daemon rejected (see finish_recovery_locked)
  // may still be released: nothing to do, and no reason to fail the client.
  const bool after_recovery = recovery_.status.recovery_num > 0;
  EntryNode * en = find_message_entry(wrapper->topic, req->entry_id);
  if (!en) {
    send_response(fd, after_recovery ? 0 : EINVAL);
    return;
  }

  if (
    req->pubsub_id < 0 ||
    req->pubsub_id >= static_cast<int32_t>(AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID)) {
    send_response(fd, EINVAL);
    return;
  }

  if (!en->referencing_subscribers.test_and_clear(static_cast<size_t>(req->pubsub_id))) {
    send_response(fd, after_recovery ? 0 : EINVAL);
    return;
  }

  send_response(fd, 0);
}

void CommandHandlers::handle_publish_msg(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const PublishMsgRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock tlock(wrapper->topic_rwsem);
  AGNOCAST_DAEMON_BENCH_STAMP_WORK();

  PublisherInfo * pub_info = find_publisher_info(wrapper, req->publisher_id);
  if (!pub_info) {
    send_response(fd, EINVAL);
    return;
  }

  ProcessInfo * proc_info = store_.find_process(pub_info->pid);
  if (!proc_info) {
    send_response(fd, EINVAL);
    return;
  }

  const uint64_t mempool_start = proc_info->mempool_entry->addr;
  const uint64_t mempool_end = mempool_start + allocator_.mempool_size_bytes();
  if (req->msg_virtual_address < mempool_start || req->msg_virtual_address >= mempool_end) {
    send_response(fd, EINVAL);
    return;
  }

  PublishMsgResponse resp{};
  // A publish repeated after a daemon restart: the predecessor may have
  // committed it, and a subscriber that got it reported the entry.
  EntryNode * repeated = nullptr;
  if (!pub_info->pending_addrs.empty() && pub_info->pending_addrs.erase(req->msg_virtual_address)) {
    for (auto & [eid, en] : wrapper->topic.entries) {
      (void)eid;
      if (en.publisher_id == pub_info->id && en.msg_virtual_address == req->msg_virtual_address) {
        repeated = &en;
        break;
      }
    }
  }
  if (repeated != nullptr) {
    resp.entry_id = repeated->entry_id;
    fprintf(
      stderr, "agnocast_daemon: a repeated publish answered from entry %lld\n",
      static_cast<long long>(repeated->entry_id));
    std::lock_guard<std::mutex> rlock(recovery_mtx_);
    recovery_.status.dedup_publish_num++;
  } else {
    const int64_t entry_id = wrapper->topic.current_entry_id++;
    auto [it, inserted] = wrapper->topic.entries.try_emplace(entry_id);
    if (!inserted) {
      send_response(fd, EEXIST);
      return;
    }
    it->second.entry_id = entry_id;
    it->second.publisher_id = pub_info->id;
    it->second.msg_virtual_address = req->msg_virtual_address;
    resp.entry_id = entry_id;
    pub_info->entries_num++;
  }

  drain_release_rings(wrapper);
  int ret = release_msgs_to_meet_depth(wrapper, pub_info, &resp);
  if (ret < 0) {
    send_response(fd, -ret);
    return;
  }

  uint32_t subscriber_num = 0;
  t_notify_fds.clear();
  for (auto & [id, sub] : wrapper->topic.sub_info_map) {
    (void)id;
    if (sub.is_take_sub) continue;
    if (sub.ignore_local_publications && (sub.pid == pub_info->pid)) continue;
    resp.subscriber_ids[subscriber_num++] = sub.id;
    push_receive(wrapper, sub, store_);
    // A subscriber registered before the eventfd handshake has no descriptor;
    // it polls instead of being woken, so there is nothing to signal.
    if (sub.notify_eventfd >= 0) t_notify_fds.push_back(sub.notify_eventfd);
  }
  resp.subscriber_num = subscriber_num;

  // The entry is committed, so a woken subscriber will find it. Drop the topic
  // lock first so concurrent publishers on this topic are not blocked by the
  // notification.
  //
  // glock stays held, as a shared lock, for the whole notify: the descriptors
  // in t_notify_fds are owned by SubscriberInfo, and the two paths that close
  // them (handle_remove_subscriber, process_exit_cleanup) both take
  // global_mutex_ exclusively. Holding it shared keeps them out until the last
  // write(2) lands, without blocking any other publisher or receiver, all of
  // which take it shared too.
  tlock.unlock();

  AGNOCAST_DAEMON_BENCH_STAMP_NOTIFY_BEGIN();
  notify_subscribers(t_notify_fds);
  AGNOCAST_DAEMON_BENCH_STAMP_NOTIFY_END();

  fault_point("publish_before_reply");
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_receive_msg(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const ReceiveMsgRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  AGNOCAST_DAEMON_BENCH_STAMP_WORK();

  SubscriberInfo * sub_info = find_subscriber_info(wrapper, req->subscriber_id);
  if (!sub_info) {
    send_response(fd, EINVAL);
    return;
  }

  ReceiveMsgResponse resp{};
  int ret = receive_msg_core(wrapper, sub_info, req->subscriber_id, &resp, store_);
  if (ret < 0) {
    send_response(fd, -ret);
    return;
  }

  if (!sub_info->need_mmap_update) {
    resp.pub_shm_num = 0;
  } else {
    ret = set_publisher_shm_info(
      wrapper, sub_info->pid, allocator_, resp.pub_shm_infos, AGNOCAST_PROTO_MAX_PUBLISHER_NUM,
      &resp.pub_shm_num, store_);
    if (ret < 0) {
      send_response(fd, -ret);
      return;
    }
    sub_info->need_mmap_update = false;
  }

  // Pushing resumes once this reply has caught the subscriber up. With
  // call_again set, the rest still has to come through the request.
  if (sub_info->channel && !resp.call_again) {
    std::lock_guard<std::mutex> lock(sub_info->channel->mtx);
    set_ipc_pending(*sub_info->channel, false);
  }
  fault_point("receive_before_reply");
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_take_msg(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const TakeMsgRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  SubscriberInfo * sub_info = find_subscriber_info(wrapper, req->subscriber_id);
  if (!sub_info) {
    send_response(fd, EINVAL);
    return;
  }
  // A queued release of the message about to be taken again must land first,
  // or allow_same_message would see the stale bit, skip re-referencing, and the
  // late release would then drop the reference the caller now holds.
  drain_release_ring(wrapper, *sub_info);

  TakeMsgResponse resp{};
  resp.addr = 0;
  resp.entry_id = -1;

  uint32_t searched_count = 0;
  EntryNode * candidate_en = nullptr;

  for (auto rit = wrapper->topic.entries.rbegin(); rit != wrapper->topic.entries.rend(); ++rit) {
    EntryNode & en = rit->second;
    if (searched_count >= sub_info->qos_depth) break;

    if (!req->allow_same_message && en.entry_id == sub_info->latest_received_entry_id) {
      break;
    }
    if (en.entry_id < sub_info->latest_received_entry_id) break;

    PublisherInfo * pub_info = find_publisher_info(wrapper, en.publisher_id);
    if (!pub_info) {
      send_response(fd, ENODATA);
      return;
    }

    ProcessInfo * proc_info = store_.find_process(pub_info->pid);
    if (!proc_info || proc_info->exited) continue;

    if (sub_info->ignore_local_publications && (sub_info->pid == pub_info->pid)) continue;

    candidate_en = &en;
    searched_count++;
  }

  if (candidate_en) {
    bool already_referenced = false;
    if (req->allow_same_message) {
      already_referenced =
        candidate_en->referencing_subscribers.test(static_cast<size_t>(req->subscriber_id));
    }
    if (!already_referenced) {
      const int ret = add_subscriber_reference(*candidate_en, req->subscriber_id);
      if (ret < 0) {
        send_response(fd, -ret);
        return;
      }
    }
    resp.addr = candidate_en->msg_virtual_address;
    resp.entry_id = candidate_en->entry_id;
    sub_info->latest_received_entry_id = resp.entry_id;
  }

  if (!sub_info->need_mmap_update) {
    resp.pub_shm_num = 0;
    send_response(fd, 0, &resp, sizeof(resp));
    return;
  }

  const int ret = set_publisher_shm_info(
    wrapper, sub_info->pid, allocator_, resp.pub_shm_infos, AGNOCAST_PROTO_MAX_PUBLISHER_NUM,
    &resp.pub_shm_num, store_);
  if (ret < 0) {
    send_response(fd, -ret);
    return;
  }
  sub_info->need_mmap_update = false;
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_get_subscriber_num(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetSubscriberNumRequest *>(payload);

  GetSubscriberNumResponse resp{};

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, 0, &resp, sizeof(resp));
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  for (const auto & [id, sub] : wrapper->topic.sub_info_map) {
    (void)id;
    if (sub.is_bridge) resp.a2r_bridge_exist = true;
    if (sub.pid == pid) {
      resp.same_process_subscriber_num++;
    } else {
      resp.other_process_subscriber_num++;
    }
  }
  for (const auto & [id, pub] : wrapper->topic.pub_info_map) {
    (void)id;
    if (pub.is_bridge) {
      resp.r2a_bridge_exist = true;
      break;
    }
  }
  resp.ros2_subscriber_num = wrapper->topic.ros2_subscriber_num;
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_get_exit_process(int fd)
{
  pid_t global_pid = -1;
  uint32_t committed_count = 0;

  GetExitProcessResponse resp{};
  resp.pid = -1;
  resp.daemon_should_exit = false;

  {
    std::unique_lock glock(store_.global_mutex_);
    for (auto & [pid, proc] : store_.proc_info_map_) {
      if (!proc.exited) continue;

      global_pid = pid;
      resp.pid = pid;

      committed_count = 0;
      for (const auto & entry : proc.exit_subscriptions) {
        if (committed_count >= AGNOCAST_PROTO_MAX_SUBSCRIPTION_NUM_PER_PROCESS) break;
        auto & out = resp.subscription_mq_infos[committed_count++];
        strncpy(
          out.topic_name, entry.topic_name.c_str(), AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE - 1);
        out.subscriber_id = entry.subscriber_id;
      }
      resp.subscription_mq_info_num = committed_count;
      break;
    }
  }

  // Commit phase (mirrors agnocast_commit_exit_process).
  {
    std::unique_lock glock(store_.global_mutex_);
    if (global_pid >= 0) {
      auto it = store_.proc_info_map_.find(global_pid);
      if (it != store_.proc_info_map_.end()) {
        auto & proc = it->second;
        if (committed_count > 0) {
          proc.exit_subscriptions.erase(
            proc.exit_subscriptions.begin(),
            proc.exit_subscriptions.begin() +
              std::min(committed_count, static_cast<uint32_t>(proc.exit_subscriptions.size())));
        }
        if (proc.exit_subscriptions.empty()) {
          store_.proc_info_map_.erase(it);
        }
      }
    }
    resp.daemon_should_exit = store_.proc_info_map_.empty();
  }

  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_get_subscriber_qos(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetSubscriberQosRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  SubscriberInfo * sub = find_subscriber_info(wrapper, req->subscriber_id);
  if (!sub) {
    send_response(fd, EINVAL);
    return;
  }

  GetSubscriberQosResponse resp{};
  resp.depth = sub->qos_depth;
  resp.is_transient_local = sub->qos_is_transient_local;
  resp.is_reliable = sub->qos_is_reliable;
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_get_publisher_qos(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetPublisherQosRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  PublisherInfo * pub = find_publisher_info(wrapper, req->publisher_id);
  if (!pub) {
    send_response(fd, EINVAL);
    return;
  }

  GetPublisherQosResponse resp{};
  resp.depth = pub->qos_depth;
  resp.is_transient_local = pub->qos_is_transient_local;
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_add_bridge(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const AddBridgeRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);
  BridgeInfo * existing = find_bridge_info(store_, req->topic_name);

  AddBridgeResponse resp{};
  if (existing) {
    if (existing->pid != pid) {
      resp.pid = existing->pid;
      resp.has_r2a = existing->has_r2a;
      resp.has_a2r = existing->has_a2r;
      send_response(fd, EEXIST, &resp, sizeof(resp));
      return;
    }
    if (req->is_r2a) {
      existing->has_r2a = true;
    } else {
      existing->has_a2r = true;
    }
    resp.pid = existing->pid;
    resp.has_r2a = existing->has_r2a;
    resp.has_a2r = existing->has_a2r;
    send_response(fd, 0, &resp, sizeof(resp));
    return;
  }

  BridgeInfo br;
  br.topic_name = req->topic_name;
  br.pid = pid;
  br.has_r2a = req->is_r2a;
  br.has_a2r = !req->is_r2a;
  resp.pid = pid;
  resp.has_r2a = br.has_r2a;
  resp.has_a2r = br.has_a2r;
  store_.bridge_map_.emplace(req->topic_name, std::move(br));
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_remove_bridge(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RemoveBridgeRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);
  BridgeInfo * br = find_bridge_info(store_, req->topic_name);
  if (!br) {
    send_response(fd, ENOENT);
    return;
  }
  if (br->pid != pid) {
    send_response(fd, EPERM);
    return;
  }

  if (req->is_r2a) {
    br->has_r2a = false;
  } else {
    br->has_a2r = false;
  }

  if (!br->has_r2a && !br->has_a2r) {
    store_.bridge_map_.erase(req->topic_name);
  }
  send_response(fd, 0);
}

void CommandHandlers::handle_get_publisher_num(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetPublisherNumRequest *>(payload);

  GetPublisherNumResponse resp{};

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, 0, &resp, sizeof(resp));
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  resp.publisher_num = static_cast<uint32_t>(wrapper->topic.pub_info_map.size());
  resp.ros2_publisher_num = wrapper->topic.ros2_publisher_num;
  for (const auto & [id, pub] : wrapper->topic.pub_info_map) {
    (void)id;
    if (pub.is_bridge) {
      resp.r2a_bridge_exist = true;
      break;
    }
  }
  for (const auto & [id, sub] : wrapper->topic.sub_info_map) {
    (void)id;
    if (sub.is_bridge) {
      resp.a2r_bridge_exist = true;
      break;
    }
  }
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_remove_subscriber(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RemoveSubscriberRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock tlock(wrapper->topic_rwsem);
  SubscriberInfo * sub_info = find_subscriber_info(wrapper, req->subscriber_id);
  if (!sub_info) {
    send_response(fd, ENODATA);
    return;
  }

  if (sub_info->notify_eventfd >= 0) close(sub_info->notify_eventfd);
  // The cleanup below frees entries no one references, so the other
  // subscribers' queued releases have to be in the bits first.
  drain_release_rings(wrapper);
  wrapper->topic.sub_info_map.erase(req->subscriber_id);
  cleanup_unreferenced_entries_from_exited_publishers(wrapper, req->subscriber_id, store_);
  try_remove_empty_topic(store_, TopicKey{req->topic_name, store_.get_process_domain_id(pid)});
  send_response(fd, 0);
}

void CommandHandlers::handle_remove_publisher(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RemovePublisherRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock tlock(wrapper->topic_rwsem);
  PublisherInfo * pub_info = find_publisher_info(wrapper, req->publisher_id);
  if (!pub_info) {
    send_response(fd, ENODATA);
    return;
  }

  drain_release_rings(wrapper);
  std::vector<int64_t> to_remove;
  for (auto & [entry_id, en] : wrapper->topic.entries) {
    if (en.publisher_id != req->publisher_id) continue;
    if (!is_referenced(en)) {
      to_remove.push_back(entry_id);
    }
  }
  for (int64_t eid : to_remove) {
    wrapper->topic.entries.erase(eid);
    pub_info->entries_num--;
  }

  if (pub_info->entries_num == 0) {
    wrapper->topic.pub_info_map.erase(req->publisher_id);
  }
  try_remove_empty_topic(store_, TopicKey{req->topic_name, store_.get_process_domain_id(pid)});
  send_response(fd, 0);
}

void CommandHandlers::handle_check_and_request_bridge_shutdown(int fd, pid_t pid)
{
  CheckAndRequestBridgeShutdownResponse resp{};

  std::unique_lock glock(store_.global_mutex_);
  ProcessInfo * proc = store_.find_process(pid);
  const uint32_t domain_id = proc ? proc->domain_id : 0;
  if (get_process_num_in_domain(store_, domain_id) <= 1) {
    if (proc) proc->is_performance_bridge_manager = false;
    resp.should_shutdown = true;
  } else {
    resp.should_shutdown = false;
  }
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_get_topic_list(int fd)
{
  auto resp = std::make_unique<GetTopicListResponse>();
  std::memset(resp.get(), 0, sizeof(*resp));

  std::shared_lock glock(store_.global_mutex_);
  for (const auto & [key, wrapper] : store_.topic_map_) {
    (void)wrapper;
    if (resp->topic_num >= AGNOCAST_PROTO_MAX_TOPIC_NUM) {
      send_response(fd, ENOBUFS);
      return;
    }
    strncpy(
      resp->topic_names[resp->topic_num], key.name.c_str(),
      AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE - 1);
    resp->topic_num++;
  }
  send_response(fd, 0, resp.get(), packed_topic_list_bytes(resp->topic_num));
}

void CommandHandlers::handle_get_topic_subscriber_info(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetTopicSubscriberInfoRequest *>(payload);

  auto resp = std::make_unique<GetTopicSubscriberInfoResponse>();
  std::memset(resp.get(), 0, sizeof(*resp));

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, 0, resp.get(), packed_topic_info_bytes(0));
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  if (wrapper->topic.sub_info_map.size() > AGNOCAST_PROTO_MAX_SUBSCRIBER_NUM) {
    send_response(fd, ENOBUFS);
    return;
  }

  uint32_t idx = 0;
  for (const auto & [id, sub] : wrapper->topic.sub_info_map) {
    (void)id;
    auto & e = resp->entries[idx++];
    strncpy(e.node_name, sub.node_name.c_str(), AGNOCAST_PROTO_NODE_NAME_BUFFER_SIZE - 1);
    e.qos_depth = sub.qos_depth;
    e.qos_is_transient_local = sub.qos_is_transient_local;
    e.qos_is_reliable = sub.qos_is_reliable;
    e.is_bridge = sub.is_bridge;
  }
  resp->entry_num = idx;
  send_response(fd, 0, resp.get(), packed_topic_info_bytes(idx));
}

void CommandHandlers::handle_get_topic_publisher_info(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetTopicPublisherInfoRequest *>(payload);

  auto resp = std::make_unique<GetTopicPublisherInfoResponse>();
  std::memset(resp.get(), 0, sizeof(*resp));

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, 0, resp.get(), packed_topic_info_bytes(0));
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  if (wrapper->topic.pub_info_map.size() > AGNOCAST_PROTO_MAX_PUBLISHER_NUM) {
    send_response(fd, ENOBUFS);
    return;
  }

  uint32_t idx = 0;
  for (const auto & [id, pub] : wrapper->topic.pub_info_map) {
    (void)id;
    auto & e = resp->entries[idx++];
    strncpy(e.node_name, pub.node_name.c_str(), AGNOCAST_PROTO_NODE_NAME_BUFFER_SIZE - 1);
    e.qos_depth = pub.qos_depth;
    e.qos_is_transient_local = pub.qos_is_transient_local;
    e.qos_is_reliable = false;
    e.is_bridge = pub.is_bridge;
  }
  resp->entry_num = idx;
  send_response(fd, 0, resp.get(), packed_topic_info_bytes(idx));
}

void CommandHandlers::handle_get_node_subscriber_topics(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetNodeSubscriberTopicsRequest *>(payload);

  GetNodeTopicsResponse resp{};
  resp.topic_num = 0;

  std::shared_lock glock(store_.global_mutex_);
  for (const auto & [key, wrapper_ptr] : store_.topic_map_) {
    TopicWrapper * wrapper = wrapper_ptr.get();
    std::shared_lock tlock(wrapper->topic_rwsem);
    bool found = false;
    for (const auto & [id, sub] : wrapper->topic.sub_info_map) {
      (void)id;
      if (sub.node_name == req->node_name) {
        found = true;
        break;
      }
    }
    tlock.unlock();

    if (!found) continue;
    if (resp.topic_num >= AGNOCAST_PROTO_MAX_TOPIC_NUM) {
      send_response(fd, ENOBUFS);
      return;
    }
    strncpy(
      resp.topic_names[resp.topic_num], key.name.c_str(),
      AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE - 1);
    resp.topic_num++;
  }
  send_response(fd, 0, &resp, packed_topic_list_bytes(resp.topic_num));
}

void CommandHandlers::handle_get_node_publisher_topics(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetNodePublisherTopicsRequest *>(payload);

  GetNodeTopicsResponse resp{};
  resp.topic_num = 0;

  std::shared_lock glock(store_.global_mutex_);
  for (const auto & [key, wrapper_ptr] : store_.topic_map_) {
    TopicWrapper * wrapper = wrapper_ptr.get();
    std::shared_lock tlock(wrapper->topic_rwsem);
    bool found = false;
    for (const auto & [id, pub] : wrapper->topic.pub_info_map) {
      (void)id;
      if (pub.node_name == req->node_name) {
        found = true;
        break;
      }
    }
    tlock.unlock();

    if (!found) continue;
    if (resp.topic_num >= AGNOCAST_PROTO_MAX_TOPIC_NUM) {
      send_response(fd, ENOBUFS);
      return;
    }
    strncpy(
      resp.topic_names[resp.topic_num], key.name.c_str(),
      AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE - 1);
    resp.topic_num++;
  }
  send_response(fd, 0, &resp, packed_topic_list_bytes(resp.topic_num));
}

void CommandHandlers::handle_set_ros2_subscriber_num(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const SetRos2SubscriberNumRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, ENOENT);
    return;
  }

  std::unique_lock tlock(wrapper->topic_rwsem);
  wrapper->topic.ros2_subscriber_num = req->ros2_subscriber_num;
  send_response(fd, 0);
}

void CommandHandlers::handle_set_ros2_publisher_num(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const SetRos2PublisherNumRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, ENOENT);
    return;
  }

  std::unique_lock tlock(wrapper->topic_rwsem);
  wrapper->topic.ros2_publisher_num = req->ros2_publisher_num;
  send_response(fd, 0);
}

void CommandHandlers::handle_notify_bridge_shutdown(int fd, pid_t pid)
{
  std::unique_lock glock(store_.global_mutex_);
  ProcessInfo * proc = store_.find_process(pid);
  if (proc) proc->is_performance_bridge_manager = false;
  send_response(fd, 0);
}

void CommandHandlers::handle_attach_subscriber_channel(
  int fd, pid_t pid, const void * payload, int channel_fd)
{
  // Owns channel_fd on every path: the mapping, once made, does not need it.
  struct FdCloser
  {
    int fd;
    ~FdCloser()
    {
      if (fd >= 0) close(fd);
    }
  } closer{channel_fd};

  if (!payload || channel_fd < 0) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const AttachSubscriberChannelRequest *>(payload);

  constexpr size_t kChannelSize = sizeof(AgnocastSubscriberChannel);
  struct stat st
  {
  };
  if (fstat(channel_fd, &st) < 0 || st.st_size != static_cast<off_t>(kChannelSize)) {
    send_response(fd, EINVAL);
    return;
  }
  const int seals = fcntl(channel_fd, F_GET_SEALS);
  if (seals < 0 || (seals & F_SEAL_SHRINK) == 0) {
    send_response(fd, EPERM);
    return;
  }

  void * mapped = mmap(nullptr, kChannelSize, PROT_READ | PROT_WRITE, MAP_SHARED, channel_fd, 0);
  if (mapped == MAP_FAILED) {
    send_response(fd, errno);
    return;
  }
  auto channel =
    std::make_unique<SubscriberChannel>(static_cast<AgnocastSubscriberChannel *>(mapped));
  if (__atomic_load_n(&channel->shm->magic, __ATOMIC_ACQUIRE) != AGNOCAST_SUB_CHANNEL_MAGIC) {
    send_response(fd, EINVAL);
    return;
  }

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock tlock(wrapper->topic_rwsem);
  SubscriberInfo * sub_info = find_subscriber_info(wrapper, req->subscriber_id);
  if (!sub_info) {
    send_response(fd, ENODATA);
    return;
  }
  if (sub_info->pid != pid) {
    send_response(fd, EPERM);
    return;
  }
  if (sub_info->channel) {
    send_response(fd, EEXIST);
    return;
  }

  sub_info->channel = std::move(channel);
  {
    std::lock_guard<std::mutex> channel_lock(sub_info->channel->mtx);
    set_ipc_pending(*sub_info->channel, true);
  }
  send_response(fd, 0);
}

void CommandHandlers::process_exit_cleanup(pid_t pid)
{
  std::vector<ExitSubscriptionEntry> unlink_subscriptions;

  {
    std::unique_lock glock(store_.global_mutex_);

    ProcessInfo * proc_info = store_.find_process(pid);
    if (!proc_info) return;

    proc_info->exited = true;
    allocator_.free_memory(pid);

    std::vector<TopicKey> topics_to_remove;

    for (auto & [key, wrapper_ptr] : store_.topic_map_) {
      TopicWrapper * wrapper = wrapper_ptr.get();
      std::unique_lock tlock(wrapper->topic_rwsem);
      drain_release_rings(wrapper);

      // Publisher exit cleanup
      std::vector<int32_t> pubs_to_remove;
      for (auto & [pub_id, pub_info] : wrapper->topic.pub_info_map) {
        if (pub_info.pid != pid) continue;

        std::vector<int64_t> entries_to_remove;
        for (auto & [entry_id, en] : wrapper->topic.entries) {
          if (en.publisher_id != pub_id) continue;
          if (!is_referenced(en)) entries_to_remove.push_back(entry_id);
        }
        for (int64_t eid : entries_to_remove) {
          wrapper->topic.entries.erase(eid);
          pub_info.entries_num--;
        }
        if (pub_info.entries_num == 0) pubs_to_remove.push_back(pub_id);
      }
      for (int32_t pub_id : pubs_to_remove) {
        wrapper->topic.pub_info_map.erase(pub_id);
      }

      // Subscriber exit cleanup
      std::vector<int32_t> subs_to_remove;
      for (auto & [sub_id, sub_info] : wrapper->topic.sub_info_map) {
        if (sub_info.pid != pid) continue;

        if (
          proc_info->exit_subscriptions.size() < AGNOCAST_PROTO_MAX_SUBSCRIPTION_NUM_PER_PROCESS) {
          ExitSubscriptionEntry entry;
          entry.topic_name = key.name;
          entry.subscriber_id = sub_id;
          proc_info->exit_subscriptions.push_back(std::move(entry));
        }

        subs_to_remove.push_back(sub_id);
      }
      for (int32_t sub_id : subs_to_remove) {
        if (SubscriberInfo * dying = find_subscriber_info(wrapper, sub_id);
            dying != nullptr && dying->notify_eventfd >= 0) {
          close(dying->notify_eventfd);
        }
        wrapper->topic.sub_info_map.erase(sub_id);
        cleanup_unreferenced_entries_from_exited_publishers(wrapper, sub_id, store_);
      }

      if (wrapper->topic.pub_info_map.empty() && wrapper->topic.sub_info_map.empty()) {
        wrapper->topic.entries.clear();
        topics_to_remove.push_back(key);
      }
    }

    for (const auto & key : topics_to_remove) {
      store_.topic_map_.erase(key);
    }

    std::vector<std::string> bridges_to_remove;
    for (auto & [name, br] : store_.bridge_map_) {
      if (br.pid == pid) bridges_to_remove.push_back(name);
    }
    for (const auto & name : bridges_to_remove) {
      store_.bridge_map_.erase(name);
    }

    // Snapshot for POSIX unlink after releasing metadata locks.
    unlink_subscriptions = proc_info->exit_subscriptions;
    persist_state_locked();
  }

  unlink_posix_resources_for_exit(pid, unlink_subscriptions);
  forget_restoring_client(pid);
}

void CommandHandlers::on_client_disconnect(pid_t client_pid)
{
  process_exit_cleanup(client_pid);
}

// ============================================================
// Daemon restart recovery (see protocol.h)
// ============================================================

CommandHandlers::~CommandHandlers()
{
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    stop_watchdog_ = true;
  }
  recovery_cv_.notify_all();
  if (watchdog_.joinable()) watchdog_.join();
}

void CommandHandlers::persist_state_locked()
{
  if (state_path_.empty()) return;
  const std::string tmp = state_path_ + ".tmp";
  {
    std::ofstream ofs(tmp, std::ios::trunc);
    if (!ofs) {
      fprintf(stderr, "agnocast_daemon: cannot write %s: %s\n", tmp.c_str(), strerror(errno));
      return;
    }
    for (const auto & [pid, proc] : store_.proc_info_map_) {
      if (!proc.exited) ofs << pid << " " << proc.starttime << "\n";
    }
    // Clients of a predecessor that have not settled yet stay listed, so a
    // daemon that crashes during the grace period still waits for them.
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    for (const auto & [pid, starttime] : recovery_.expected) {
      if (store_.proc_info_map_.count(pid) == 0) ofs << pid << " " << starttime << "\n";
    }
  }
  if (rename(tmp.c_str(), state_path_.c_str()) != 0) {
    fprintf(
      stderr, "agnocast_daemon: cannot replace %s: %s\n", state_path_.c_str(), strerror(errno));
  }
}

size_t CommandHandlers::begin_recovery(const std::string & state_path)
{
  std::unique_lock glock(store_.global_mutex_);
  state_path_ = state_path;

  std::unordered_map<pid_t, uint64_t> alive;
  std::ifstream ifs(state_path_);
  long long pid = 0;
  unsigned long long starttime = 0;
  while (ifs >> pid >> starttime) {
    if (pid > 0 && pid != getpid() && proc_alive(static_cast<pid_t>(pid), starttime)) {
      alive[static_cast<pid_t>(pid)] = starttime;
    }
  }

  std::lock_guard<std::mutex> lock(recovery_mtx_);
  if (alive.empty()) {
    recovery_ = Recovery{};
  } else {
    recovery_.expected = alive;
    recovery_.begin_ns = monotonic_ns();
    recovery_.status.recovering = 1;
    recovery_.status.expected_num = static_cast<uint32_t>(alive.size());
    recovery_.status.recovery_num = 1;
    recovering_.store(true, std::memory_order_release);
    watchdog_ = std::thread([this] { recovery_watchdog_main(); });
    fprintf(
      stderr, "agnocast_daemon: %zu client(s) of a previous daemon are alive; waiting for them\n",
      alive.size());
  }
  // Rewrite the file now: dead predecessors' clients must not be waited for
  // by a later daemon.
  recovery_mtx_.unlock();
  persist_state_locked();
  recovery_mtx_.lock();
  return alive.size();
}

void CommandHandlers::gate_on_recovery(pid_t pid, uint32_t command)
{
  if (!recovering_.load(std::memory_order_acquire)) return;
  switch (command) {
    case AGNOCAST_CMD_GET_VERSION:
    case AGNOCAST_CMD_GET_RECOVERY_STATUS:
    case AGNOCAST_CMD_RESTORE_PROCESS:
    case AGNOCAST_CMD_RESTORE_MAPPINGS:
    case AGNOCAST_CMD_RESTORE_PUBLISHER:
    case AGNOCAST_CMD_RESTORE_SUBSCRIBER:
    case AGNOCAST_CMD_RESTORE_ENTRIES:
    case AGNOCAST_CMD_RESTORE_DONE:
      return;
    default:
      break;
  }
  std::unique_lock<std::mutex> lock(recovery_mtx_);
  // A client in the middle of restoring may replay what has no RESTORE_*
  // counterpart. None of these hands memory back to a publisher.
  if (
    recovery_.restoring.count(pid) != 0 &&
    (command == AGNOCAST_CMD_ADD_BRIDGE || command == AGNOCAST_CMD_SET_ROS2_SUBSCRIBER_NUM ||
     command == AGNOCAST_CMD_SET_ROS2_PUBLISHER_NUM ||
     command == AGNOCAST_CMD_ATTACH_SUBSCRIBER_CHANNEL)) {
    return;
  }
  recovery_cv_.wait(lock, [this] { return !recovering_.load(std::memory_order_acquire); });
}

void CommandHandlers::handle_restore_process(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestoreProcessRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    if (!recovering_.load(std::memory_order_acquire)) {
      send_response(fd, EBUSY);
      return;
    }
  }
  if (store_.find_process(pid)) {
    send_response(fd, EEXIST);
    return;
  }
  MemoryAllocator::MempoolEntry * entry = allocator_.restore_assignment(pid, req->shm_addr);
  if (entry == nullptr) {
    send_response(fd, EINVAL);
    return;
  }

  ProcessInfo proc;
  proc.pid = pid;
  proc.starttime = proc_starttime(pid);
  proc.domain_id = req->domain_id;
  proc.is_performance_bridge_manager = req->is_performance_bridge_manager;
  proc.mempool_entry = entry;
  store_.proc_info_map_.emplace(pid, std::move(proc));
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    recovery_.restoring.insert(pid);
  }
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_mappings(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestoreMappingsRequest *>(payload);
  if (req->mapping_num > AGNOCAST_PROTO_RESTORE_BATCH) {
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock glock(store_.global_mutex_);
  std::lock_guard<std::mutex> lock(recovery_mtx_);
  if (recovery_.restoring.count(pid) == 0) {
    send_response(fd, EPERM);
    return;
  }
  for (uint32_t i = 0; i < req->mapping_num; i++) {
    const AgnocastRestoreMapping & m = req->mappings[i];
    MemoryAllocator::MempoolEntry * entry = allocator_.find_entry(m.shm_addr);
    if (entry == nullptr) continue;
    allocator_.reference_memory(entry, pid);  // -EEXIST: reported twice, fine
    recovery_.pool_owner[m.shm_addr] = m.pid;
  }
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_publisher(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestorePublisherRequest *>(payload);
  if (
    req->publisher_id < 0 ||
    req->publisher_id >= static_cast<int32_t>(AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID)) {
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock glock(store_.global_mutex_);
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    if (recovery_.restoring.count(pid) == 0) {
      send_response(fd, EPERM);
      return;
    }
  }
  TopicWrapper * wrapper = store_.find_or_create_topic_for_process(pid, req->topic_name);
  TopicStruct & topic = wrapper->topic;
  if (
    topic.pub_info_map.count(req->publisher_id) != 0 ||
    topic.sub_info_map.count(req->publisher_id) != 0) {
    send_response(fd, EEXIST);
    return;
  }

  PublisherInfo info;
  info.id = req->publisher_id;
  info.domain_id = wrapper->domain_id;
  info.pid = pid;
  info.node_name = req->node_name;
  info.qos_depth = req->qos_depth;
  info.qos_is_transient_local = req->qos_is_transient_local;
  info.is_bridge = req->is_bridge;
  info.restored_last_entry_id = req->last_entry_id;
  topic.pub_info_map.emplace(info.id, std::move(info));
  topic.current_pubsub_id = std::max(topic.current_pubsub_id, req->publisher_id + 1);
  topic.current_entry_id = std::max(topic.current_entry_id, req->last_entry_id + 1);
  for (auto & [id, sub] : topic.sub_info_map) {
    (void)id;
    sub.need_mmap_update = true;
  }
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_subscriber(
  int fd, pid_t pid, const void * payload, int notify_eventfd)
{
  if (!payload) {
    if (notify_eventfd >= 0) close(notify_eventfd);
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestoreSubscriberRequest *>(payload);
  if (
    req->subscriber_id < 0 ||
    req->subscriber_id >= static_cast<int32_t>(AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID)) {
    if (notify_eventfd >= 0) close(notify_eventfd);
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock glock(store_.global_mutex_);
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    if (recovery_.restoring.count(pid) == 0) {
      if (notify_eventfd >= 0) close(notify_eventfd);
      send_response(fd, EPERM);
      return;
    }
  }
  TopicWrapper * wrapper = store_.find_or_create_topic_for_process(pid, req->topic_name);
  TopicStruct & topic = wrapper->topic;
  if (
    topic.pub_info_map.count(req->subscriber_id) != 0 ||
    topic.sub_info_map.count(req->subscriber_id) != 0) {
    if (notify_eventfd >= 0) close(notify_eventfd);
    send_response(fd, EEXIST);
    return;
  }

  SubscriberInfo info;
  info.id = req->subscriber_id;
  info.domain_id = wrapper->domain_id;
  info.pid = pid;
  info.qos_depth = req->qos_depth;
  info.qos_is_transient_local = req->qos_is_transient_local;
  info.qos_is_reliable = req->qos_is_reliable;
  info.latest_received_entry_id = req->latest_received_entry_id;
  info.node_name = req->node_name;
  info.is_take_sub = req->is_take_sub;
  info.ignore_local_publications = req->ignore_local_publications;
  info.is_bridge = req->is_bridge;
  // The pools it mapped were restored; set_publisher_shm_info() skips those.
  info.need_mmap_update = true;
  info.notify_eventfd = notify_eventfd;
  topic.sub_info_map.emplace(info.id, std::move(info));
  topic.current_pubsub_id = std::max(topic.current_pubsub_id, req->subscriber_id + 1);
  topic.current_entry_id = std::max(topic.current_entry_id, req->latest_received_entry_id + 1);
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_entries(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestoreEntriesRequest *>(payload);
  if (req->entry_num > AGNOCAST_PROTO_RESTORE_BATCH) {
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock glock(store_.global_mutex_);
  std::lock_guard<std::mutex> lock(recovery_mtx_);
  if (recovery_.restoring.count(pid) == 0) {
    send_response(fd, EPERM);
    return;
  }
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }
  TopicStruct & topic = wrapper->topic;

  if (req->kind == AGNOCAST_RESTORE_ENTRY_HELD || req->kind == AGNOCAST_RESTORE_ENTRY_DELIVERED) {
    SubscriberInfo * sub = find_subscriber_info(wrapper, req->pubsub_id);
    if (!sub || sub->pid != pid) {
      send_response(fd, EPERM);
      return;
    }
    for (uint32_t i = 0; i < req->entry_num; i++) {
      const AgnocastRestoreEntry & e = req->entries[i];
      if (e.entry_id < 0) continue;
      topic.current_entry_id = std::max(topic.current_entry_id, e.entry_id + 1);
      if (req->kind == AGNOCAST_RESTORE_ENTRY_DELIVERED) {
        recovery_.delivered[TopicKey{req->topic_name, wrapper->domain_id}][e.entry_id] = e.addr;
        continue;
      }
      recovery_.claims.push_back(
        Claim{TopicKey{req->topic_name, wrapper->domain_id}, req->pubsub_id, e.entry_id, e.addr});
      recovery_.status.claim_num++;
      topic.current_entry_id = std::max(topic.current_entry_id, e.entry_id + 1);
    }
    send_response(fd, 0);
    return;
  }

  PublisherInfo * pub = find_publisher_info(wrapper, req->pubsub_id);
  ProcessInfo * proc = store_.find_process(pid);
  if (!pub || pub->pid != pid || !proc) {
    send_response(fd, EPERM);
    return;
  }
  const uint64_t pool_start = proc->mempool_entry->addr;
  const uint64_t pool_end = pool_start + allocator_.mempool_size_bytes();
  for (uint32_t i = 0; i < req->entry_num; i++) {
    const AgnocastRestoreEntry & e = req->entries[i];
    if (e.addr < pool_start || e.addr >= pool_end) continue;  // not its memory
    if (req->kind == AGNOCAST_RESTORE_ENTRY_PENDING) {
      pub->pending_addrs.insert(e.addr);
      continue;
    }
    if (req->kind != AGNOCAST_RESTORE_ENTRY_PUBLISHED || e.entry_id < 0) continue;
    auto [it, inserted] = topic.entries.try_emplace(e.entry_id);
    if (!inserted) continue;  // two publishers cannot share an id
    it->second.entry_id = e.entry_id;
    it->second.publisher_id = pub->id;
    it->second.msg_virtual_address = e.addr;
    pub->entries_num++;
    topic.current_entry_id = std::max(topic.current_entry_id, e.entry_id + 1);
  }
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_done(int fd, pid_t pid)
{
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    if (!recovering_.load(std::memory_order_acquire)) {
      send_response(fd, EBUSY);
      return;
    }
    recovery_.restoring.erase(pid);
    recovery_.restored.insert(pid);
    recovery_.expected.erase(pid);
    recovery_.status.restored_num++;
  }
  try_finish_recovery();
  send_response(fd, 0);
}

void CommandHandlers::forget_restoring_client(pid_t pid)
{
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    if (!recovering_.load(std::memory_order_acquire)) return;
    const bool was_restoring = recovery_.restoring.erase(pid) != 0;
    const bool was_expected = recovery_.expected.erase(pid) != 0;
    if (!was_restoring && !was_expected) return;
    recovery_.dead.insert(pid);
    recovery_.status.dead_num++;
  }
  try_finish_recovery();
}

void CommandHandlers::try_finish_recovery()
{
  std::unique_lock glock(store_.global_mutex_);
  std::lock_guard<std::mutex> lock(recovery_mtx_);
  if (!recovering_.load(std::memory_order_acquire)) return;
  if (!recovery_.expected.empty() || !recovery_.restoring.empty()) return;
  finish_recovery_locked();
}

void CommandHandlers::finish_recovery_locked()
{
  const uint64_t pool_size = allocator_.mempool_size_bytes();
  auto owner_of = [&](uint64_t addr) -> pid_t {
    for (const auto & [pool, owner] : recovery_.pool_owner) {
      if (addr >= pool && addr < pool + pool_size) return owner;
    }
    return 0;
  };
  auto alive_client = [&](pid_t p) {
    const ProcessInfo * proc = store_.find_process(p);
    return proc != nullptr && !proc->exited;
  };

  // Every subscriber has reported: give each reference back to its entry.
  for (const Claim & c : recovery_.claims) {
    TopicWrapper * wrapper = store_.find_topic(c.key);
    if (!wrapper) continue;
    TopicStruct & topic = wrapper->topic;
    if (find_subscriber_info(wrapper, c.subscriber_id) == nullptr) continue;  // gone since
    EntryNode * en = find_message_entry(topic, c.entry_id);
    if (en == nullptr) {
      // No publisher reported it as an entry. A live one reports every entry
      // it was not told to free, so either its publish reply was lost (the
      // address is pending: the entry is its own, and the repeated publish
      // will be answered from it), or the publisher died: then the entry is
      // kept for the subscriber holding it under a record of the dead
      // process, as exit cleanup would have left it.
      const pid_t owner = owner_of(c.addr);
      // A process may have several publishers on the topic: the one whose
      // publish is pending, else any of them.
      PublisherInfo * holder = nullptr;
      for (auto & [id, pub] : topic.pub_info_map) {
        (void)id;
        if (pub.pid != owner) continue;
        if (pub.pending_addrs.count(c.addr) != 0) {
          holder = &pub;
          break;
        }
        if (holder == nullptr) holder = &pub;
      }
      const bool pending = holder != nullptr && holder->pending_addrs.count(c.addr) != 0;
      if (owner == 0 || (alive_client(owner) && !pending)) {
        recovery_.status.rejected_claim_num++;
        continue;
      }
      if (holder == nullptr) {
        if (topic.current_pubsub_id >= static_cast<int32_t>(AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID)) {
          recovery_.status.rejected_claim_num++;
          continue;
        }
        PublisherInfo info;
        info.id = topic.current_pubsub_id++;
        info.domain_id = wrapper->domain_id;
        info.pid = owner;
        info.node_name = "(exited during a daemon restart)";
        holder = &topic.pub_info_map.emplace(info.id, std::move(info)).first->second;
      }
      auto [it, inserted] = topic.entries.try_emplace(c.entry_id);
      (void)inserted;
      it->second.entry_id = c.entry_id;
      it->second.publisher_id = holder->id;
      it->second.msg_virtual_address = c.addr;
      holder->entries_num++;
      en = &it->second;
      if (!pending) recovery_.status.orphan_claim_num++;
    } else if (en->msg_virtual_address != c.addr) {
      recovery_.status.rejected_claim_num++;
      continue;
    }
    en->referencing_subscribers.test_and_set(static_cast<size_t>(c.subscriber_id));
  }
  recovery_.claims.clear();

  // A pending publish no subscriber holds may still have been committed and
  // delivered (and released since): then a subscriber was recently handed an
  // entry at that address. Put the entry back under its id, so the repeated
  // publish is answered from it and the subscribers that already got it do
  // not get it again.
  for (auto & [key, delivered] : recovery_.delivered) {
    TopicWrapper * wrapper = store_.find_topic(key);
    if (!wrapper) continue;
    TopicStruct & topic = wrapper->topic;
    for (auto & [pub_id, pub] : topic.pub_info_map) {
      (void)pub_id;
      for (const uint64_t addr : pub.pending_addrs) {
        bool present = false;
        for (const auto & [eid, en] : topic.entries) {
          (void)eid;
          if (en.publisher_id == pub.id && en.msg_virtual_address == addr) present = true;
        }
        if (present) continue;
        // The publisher reuses an address once told to free it, so an older
        // message may have had the same one: only an id newer than the last
        // one the publisher was answered with can be the pending publish.
        int64_t found = -1;
        for (const auto & [eid, daddr] : delivered) {
          if (daddr != addr || eid <= pub.restored_last_entry_id) continue;
          if (topic.entries.count(eid) == 0) found = std::max(found, eid);
        }
        if (found < 0) continue;
        auto [it, inserted] = topic.entries.try_emplace(found);
        (void)inserted;
        it->second.entry_id = found;
        it->second.publisher_id = pub.id;
        it->second.msg_virtual_address = addr;
        pub.entries_num++;
      }
    }
  }
  recovery_.delivered.clear();

  // The predecessor's clients that died while it was down never got an exit
  // cleanup: their shared memory objects are still linked.
  for (const pid_t p : recovery_.dead) {
    if (!alive_client(p)) unlink_posix_resources_for_exit(p, {});
  }

  recovery_.status.recovering = 0;
  recovery_.status.grace_ns = monotonic_ns() - recovery_.begin_ns;
  recovering_.store(false, std::memory_order_release);
  fprintf(
    stderr,
    "agnocast_daemon: recovery done in %.3f ms: %u restored, %u dead, %u fenced, %u references "
    "(%u of exited publishers, %u rejected)\n",
    static_cast<double>(recovery_.status.grace_ns) / 1e6, recovery_.status.restored_num,
    recovery_.status.dead_num, recovery_.status.fenced_num, recovery_.status.claim_num,
    recovery_.status.orphan_claim_num, recovery_.status.rejected_claim_num);
  recovery_cv_.notify_all();

  recovery_mtx_.unlock();
  persist_state_locked();
  recovery_mtx_.lock();
}

// Watches the expected clients that have not restored: one known to be dead
// is settled, and one still not restored after AGNOCAST_DAEMON_RECOVERY_FENCE_MS
// (0: never) is killed, which settles it too. The daemon learned of these
// pids from its own clients' sockets, so they name processes it may act on.
void CommandHandlers::recovery_watchdog_main()
{
  const int64_t fence_ms = env_ms("AGNOCAST_DAEMON_RECOVERY_FENCE_MS", 3000);
  std::unordered_set<pid_t> fenced;
  std::unique_lock<std::mutex> lock(recovery_mtx_);
  while (!stop_watchdog_ && recovering_.load(std::memory_order_acquire)) {
    recovery_cv_.wait_for(lock, std::chrono::milliseconds(20));
    if (stop_watchdog_ || !recovering_.load(std::memory_order_acquire)) break;

    std::vector<pid_t> gone;
    for (const auto & [p, starttime] : recovery_.expected) {
      if (!proc_alive(p, starttime)) gone.push_back(p);
    }
    const bool overdue = fence_ms > 0 && monotonic_ns() - recovery_.begin_ns > fence_ms * 1000000LL;
    if (overdue) {
      for (const auto & [p, starttime] : recovery_.expected) {
        if (fenced.count(p) != 0 || !proc_alive(p, starttime)) continue;
        fprintf(
          stderr, "agnocast_daemon: pid %d did not restore within %lld ms; killing it\n", p,
          static_cast<long long>(fence_ms));
        if (kill(p, SIGKILL) == 0) {
          fenced.insert(p);
          recovery_.status.fenced_num++;
        }
      }
    }
    if (gone.empty()) continue;
    for (const pid_t p : gone) {
      recovery_.expected.erase(p);
      recovery_.dead.insert(p);
      recovery_.status.dead_num++;
    }
    lock.unlock();
    try_finish_recovery();
    lock.lock();
  }
}

void CommandHandlers::handle_get_recovery_status(int fd)
{
  GetRecoveryStatusResponse resp{};
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    resp = recovery_.status;
    if (recovering_.load(std::memory_order_acquire)) {
      resp.grace_ns = monotonic_ns() - recovery_.begin_ns;
    }
  }
  send_response(fd, 0, &resp, sizeof(resp));
}
