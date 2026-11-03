// SPDX-License-Identifier: Apache-2.0
#include "command_handlers.hpp"

#include "bench_timing.hpp"

#include <fcntl.h>
#include <mqueue.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
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

int insert_subscriber_info(
  TopicWrapper * wrapper, const char * node_name, pid_t subscriber_pid, uint32_t qos_depth,
  bool qos_is_transient_local, bool qos_is_reliable, bool is_take_sub,
  bool ignore_local_publications, bool is_bridge, SubscriberInfo ** out)
{
  if (wrapper->topic.sub_info_map.size() >= AGNOCAST_PROTO_MAX_SUBSCRIBER_NUM) {
    return -ENOBUFS;
  }

  int32_t new_id = -1;
  {
    TopicShmLock lock(wrapper->shm);
    const int ret = topic_shm_add_subscriber(
      wrapper->shm, subscriber_pid, qos_depth, qos_is_transient_local, is_take_sub,
      ignore_local_publications, &new_id);
    if (ret < 0) return ret;
  }
  SubscriberInfo info;
  info.id = new_id;
  info.domain_id = wrapper->domain_id;
  info.pid = subscriber_pid;
  info.qos_depth = qos_depth;
  info.qos_is_transient_local = qos_is_transient_local;
  info.qos_is_reliable = qos_is_reliable;
  info.node_name = node_name;
  info.is_take_sub = is_take_sub;
  info.ignore_local_publications = ignore_local_publications;
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

  int32_t new_id = -1;
  {
    TopicShmLock lock(wrapper->shm);
    const int ret = topic_shm_add_publisher(
      wrapper->shm, publisher_pid, qos_depth, qos_is_transient_local, &new_id);
    if (ret < 0) return ret;
  }
  PublisherInfo info;
  info.id = new_id;
  info.domain_id = wrapper->domain_id;
  info.pid = publisher_pid;
  info.node_name = node_name;
  info.qos_depth = qos_depth;
  info.qos_is_transient_local = qos_is_transient_local;
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

// Removes the subscriber, its references, and the entries of exited publishers
// nobody references any more, as one segment operation. Caller holds the topic
// lock exclusively.
void remove_subscriber_info(TopicWrapper * wrapper, int32_t subscriber_id)
{
  thread_local std::vector<int32_t> emptied(kTopicShmIdNum);
  uint32_t emptied_num = 0;
  {
    TopicShmLock lock(wrapper->shm);
    emptied_num = topic_shm_remove_subscriber(wrapper->shm, subscriber_id, emptied.data());
  }
  wrapper->topic.sub_info_map.erase(subscriber_id);
  for (uint32_t i = 0; i < emptied_num; i++) {
    wrapper->topic.pub_info_map.erase(emptied[i]);
  }
}

// Drops the publisher's unreferenced entries, and the publisher itself once it
// has none. Caller holds the topic lock exclusively.
void remove_publisher_entries(TopicWrapper * wrapper, int32_t publisher_id, bool exited)
{
  uint32_t remaining = 0;
  {
    TopicShmLock lock(wrapper->shm);
    remaining = topic_shm_remove_publisher(wrapper->shm, publisher_id, exited);
  }
  if (remaining == 0) wrapper->topic.pub_info_map.erase(publisher_id);
}

int64_t monotonic_ns()
{
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
}

bool pid_alive(pid_t pid)
{
  return pid > 0 && (kill(pid, 0) == 0 || errno == EPERM);
}

void unlink_process_shm(pid_t pid)
{
  const std::string shm_name = make_shm_name(pid);
  if (shm_unlink(shm_name.c_str()) < 0 && errno != ENOENT) {
    fprintf(
      stderr, "agnocast_daemon: shm_unlink(%s) failed (pid=%d): %s\n", shm_name.c_str(), pid,
      strerror(errno));
  }
}

void try_remove_empty_topic(MetadataStore & store, const TopicKey & key)
{
  auto it = store.topic_map_.find(key);
  if (it == store.topic_map_.end()) return;
  TopicWrapper * wrapper = it->second.get();
  if (!wrapper->topic.pub_info_map.empty() || !wrapper->topic.sub_info_map.empty()) return;
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

CommandHandlers::~CommandHandlers()
{
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    stop_recovery_thread_ = true;
  }
  recovery_cv_.notify_all();
  if (recovery_thread_.joinable()) recovery_thread_.join();
}

void CommandHandlers::send_response(int fd, int32_t error_code)
{
  send_response(fd, error_code, nullptr, 0);
}

void CommandHandlers::send_response(
  int fd, int32_t error_code, const void * payload, uint32_t payload_size, const int * send_fds,
  uint32_t send_fd_num)
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
  union {
    cmsghdr align;
    char bytes[CMSG_SPACE(sizeof(int) * AGNOCAST_SUBSCRIBER_EVENTFD_BATCH)];
  } cmsg_buf{};
  if (send_fds != nullptr && send_fd_num > 0) {
    send_fd_num = std::min<uint32_t>(send_fd_num, AGNOCAST_SUBSCRIBER_EVENTFD_BATCH);
    msg.msg_control = cmsg_buf.bytes;
    msg.msg_controllen = CMSG_SPACE(sizeof(int) * send_fd_num);
    cmsghdr * cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int) * send_fd_num);
    std::memcpy(CMSG_DATA(cmsg), send_fds, sizeof(int) * send_fd_num);
  }
  if (sendmsg(fd, &msg, MSG_NOSIGNAL) < 0) {
    std::fprintf(
      stderr, "agnocast_daemon: send_response failed: %s (payload_size=%u)\n", std::strerror(errno),
      payload_size);
  }
}

void CommandHandlers::dispatch(
  int client_fd, pid_t client_pid, const RequestHeader & hdr, const void * payload, int recv_fd)
{
  // Only these commands have a use for an attached descriptor. Close anything
  // that arrives on another command rather than letting it accumulate.
  const bool takes_fd = hdr.command == AGNOCAST_CMD_ADD_SUBSCRIBER ||
                        hdr.command == AGNOCAST_CMD_RESTORE_SUBSCRIBER ||
                        hdr.command == AGNOCAST_CMD_RESTORE_TOPIC;
  if (recv_fd >= 0 && !takes_fd) {
    close(recv_fd);
    recv_fd = -1;
  }

  // While a restarted daemon waits for its predecessor's clients, a new
  // registration could create a fresh segment for a topic whose old segment is
  // still to be handed back. Hold those back until the recovery is over.
  switch (static_cast<CommandType>(hdr.command)) {
    case AGNOCAST_CMD_ADD_PROCESS:
    case AGNOCAST_CMD_ADD_PUBLISHER:
    case AGNOCAST_CMD_ADD_SUBSCRIBER:
      wait_for_recovery_before_registration(client_pid);
      break;
    default:
      break;
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
    case AGNOCAST_CMD_GET_PUBLISHER_SHM_INFOS:
      handle_get_publisher_shm_infos(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RESTORE_PROCESS:
      handle_restore_process(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RESTORE_MAPPINGS:
      handle_restore_mappings(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RESTORE_TOPIC:
      handle_restore_topic(client_fd, client_pid, payload, recv_fd);
      break;
    case AGNOCAST_CMD_RESTORE_PUBLISHER:
      handle_restore_publisher(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_RESTORE_SUBSCRIBER:
      handle_restore_subscriber(client_fd, client_pid, payload, recv_fd);
      break;
    case AGNOCAST_CMD_RESTORE_DONE:
      handle_restore_done(client_fd, client_pid);
      break;
    case AGNOCAST_CMD_DEBUG_CHECK_TOPIC:
      handle_debug_check_topic(client_fd, client_pid, payload);
      break;
    case AGNOCAST_CMD_GET_RECOVERY_STATUS:
      handle_get_recovery_status(client_fd);
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
    case AGNOCAST_CMD_GET_SUBSCRIBER_EVENTFDS:
      handle_get_subscriber_eventfds(client_fd, client_pid, payload);
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
  proc.mempool_entry = allocator_.assign_memory(pid);
  if (!proc.mempool_entry) {
    send_response(fd, ENOMEM);
    return;
  }

  resp.shm_addr = proc.mempool_entry->addr;
  resp.shm_size = allocator_.mempool_size_bytes();

  store_.proc_info_map_.emplace(pid, std::move(proc));
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
  if (!wrapper) {
    if (notify_eventfd >= 0) close(notify_eventfd);
    send_response(fd, ENOMEM);
    return;
  }

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
  strncpy(
    resp.mq_topic_name, notify_mq_topic_name(wrapper), AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE - 1);
  TOPIC_SHM_FAULT_POINT("reply.add_subscriber");
  send_response(fd, 0, &resp, sizeof(resp), &wrapper->shm_fd, 1);
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
  if (!wrapper) {
    send_response(fd, ENOMEM);
    return;
  }

  PublisherInfo * pub_info = nullptr;
  const int ret = insert_publisher_info(
    wrapper, req->node_name, pid, req->qos_depth, req->qos_is_transient_local, req->is_bridge,
    &pub_info);
  if (ret < 0) {
    send_response(fd, -ret);
    return;
  }

  AddPublisherResponse resp{};
  resp.publisher_id = pub_info->id;
  strncpy(
    resp.mq_topic_name, notify_mq_topic_name(wrapper), AGNOCAST_PROTO_TOPIC_NAME_BUFFER_SIZE - 1);
  TOPIC_SHM_FAULT_POINT("reply.add_publisher");
  send_response(fd, 0, &resp, sizeof(resp), &wrapper->shm_fd, 1);
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
  int ret = 0;
  {
    TopicShmLock lock(wrapper->shm);
    ret = topic_shm_release_ref(wrapper->shm, req->pubsub_id, req->entry_id);
  }
  send_response(fd, -ret);
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

  std::shared_lock tlock(wrapper->topic_rwsem);
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
  int ret = 0;
  {
    TopicShmLock lock(wrapper->shm);
    ret = topic_shm_publish(
      wrapper->shm, req->publisher_id, req->msg_virtual_address, &resp.entry_id,
      resp.released_addrs, &resp.released_num, resp.subscriber_ids, &resp.subscriber_num);
  }
  if (ret < 0) {
    send_response(fd, -ret);
    return;
  }

  t_notify_fds.clear();
  for (uint32_t i = 0; i < resp.subscriber_num; i++) {
    const SubscriberInfo * sub = find_subscriber_info(wrapper, resp.subscriber_ids[i]);
    // A subscriber registered without an eventfd polls instead of being woken.
    if (sub != nullptr && sub->notify_eventfd >= 0) t_notify_fds.push_back(sub->notify_eventfd);
  }

  // The descriptors are owned by SubscriberInfo, and the two paths that close
  // them (handle_remove_subscriber, process_exit_cleanup) take global_mutex_
  // exclusively, so the shared glock keeps them open until the last write(2).
  tlock.unlock();

  AGNOCAST_DAEMON_BENCH_STAMP_NOTIFY_BEGIN();
  notify_subscribers(t_notify_fds);
  AGNOCAST_DAEMON_BENCH_STAMP_NOTIFY_END();

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
  // Publishers are added only under global_mutex_ held exclusively, so
  // need_mmap_update cannot be set again between the receive and the mapping.
  TopicShmLock lock(wrapper->shm);
  int ret = topic_shm_receive(
    wrapper->shm, req->subscriber_id, TopicShmAlwaysMapped{}, resp.entry_ids, resp.entry_addrs,
    &resp.entry_num, &resp.call_again);
  if (ret < 0) {
    send_response(fd, -ret);
    return;
  }

  TopicShmId & sub_shm = wrapper->shm->ids[req->subscriber_id];
  if (!sub_shm.need_mmap_update) {
    resp.pub_shm_num = 0;
  } else {
    ret = set_publisher_shm_info(
      wrapper, sub_info->pid, allocator_, resp.pub_shm_infos, AGNOCAST_PROTO_MAX_PUBLISHER_NUM,
      &resp.pub_shm_num, store_);
    if (ret < 0) {
      send_response(fd, -ret);
      return;
    }
    sub_shm.need_mmap_update = 0;
  }
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
  TakeMsgResponse resp{};
  TopicShmLock lock(wrapper->shm);
  const int ret = topic_shm_take(
    wrapper->shm, req->subscriber_id, req->allow_same_message, TopicShmAlwaysMapped{}, &resp.addr,
    &resp.entry_id);
  if (ret < 0) {
    send_response(fd, -ret);
    return;
  }

  TopicShmId & sub_shm = wrapper->shm->ids[req->subscriber_id];
  if (!sub_shm.need_mmap_update) {
    resp.pub_shm_num = 0;
    send_response(fd, 0, &resp, sizeof(resp));
    return;
  }

  const int mret = set_publisher_shm_info(
    wrapper, sub_info->pid, allocator_, resp.pub_shm_infos, AGNOCAST_PROTO_MAX_PUBLISHER_NUM,
    &resp.pub_shm_num, store_);
  if (mret < 0) {
    send_response(fd, -mret);
    return;
  }
  sub_shm.need_mmap_update = 0;
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
  remove_subscriber_info(wrapper, req->subscriber_id);
  tlock.unlock();
  try_remove_empty_topic(store_, TopicKey{req->topic_name, store_.get_process_domain_id(pid)});
  TOPIC_SHM_FAULT_POINT("reply.remove_subscriber");
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

  (void)pub_info;
  remove_publisher_entries(wrapper, req->publisher_id, false);
  tlock.unlock();
  try_remove_empty_topic(store_, TopicKey{req->topic_name, store_.get_process_domain_id(pid)});
  TOPIC_SHM_FAULT_POINT("reply.remove_publisher");
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

void CommandHandlers::handle_get_subscriber_eventfds(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetSubscriberEventfdsRequest *>(payload);

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  // Subscribers change only under the topic lock held exclusively, so the
  // generation read here matches the set listed.
  std::shared_lock tlock(wrapper->topic_rwsem);
  GetSubscriberEventfdsResponse resp{};
  resp.sub_generation = __atomic_load_n(&wrapper->shm->sub_generation, __ATOMIC_ACQUIRE);

  int fds[AGNOCAST_SUBSCRIBER_EVENTFD_BATCH];
  uint32_t index = 0;
  for (const auto & [id, sub] : wrapper->topic.sub_info_map) {
    if (sub.notify_eventfd < 0) continue;
    if (index >= req->offset && resp.num < AGNOCAST_SUBSCRIBER_EVENTFD_BATCH) {
      resp.subscriber_ids[resp.num] = id;
      fds[resp.num] = sub.notify_eventfd;
      resp.num++;
    }
    index++;
  }
  resp.total = index;
  send_response(fd, 0, &resp, sizeof(resp), fds, resp.num);
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

      // Publisher exit cleanup
      std::vector<int32_t> pubs_of_pid;
      for (const auto & [pub_id, pub_info] : wrapper->topic.pub_info_map) {
        if (pub_info.pid == pid) pubs_of_pid.push_back(pub_id);
      }
      for (int32_t pub_id : pubs_of_pid) {
        remove_publisher_entries(wrapper, pub_id, true);
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
        remove_subscriber_info(wrapper, sub_id);
      }

      if (wrapper->topic.pub_info_map.empty() && wrapper->topic.sub_info_map.empty()) {
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
    persist_state_locked();

    // Snapshot for POSIX unlink after releasing metadata locks.
    unlink_subscriptions = proc_info->exit_subscriptions;
  }

  unlink_posix_resources_for_exit(pid, unlink_subscriptions);
}

void CommandHandlers::on_client_disconnect(pid_t client_pid)
{
  process_exit_cleanup(client_pid);
}

// ============================================================
// Publisher memory lookup
// ============================================================

void CommandHandlers::handle_get_publisher_shm_infos(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const GetPublisherShmInfosRequest *>(payload);

  auto resp = std::make_unique<GetPublisherShmInfosResponse>();
  std::memset(resp.get(), 0, sizeof(*resp));

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(pid, req->topic_name);
  if (!wrapper) {
    send_response(fd, EINVAL);
    return;
  }

  std::shared_lock tlock(wrapper->topic_rwsem);
  for (const auto & [id, pub_info] : wrapper->topic.pub_info_map) {
    (void)id;
    if (pub_info.pid == pid) continue;
    ProcessInfo * proc_info = store_.find_process(pub_info.pid);
    if (!proc_info || proc_info->exited || !proc_info->mempool_entry) continue;
    bool listed = false;
    for (uint32_t i = 0; i < resp->pub_shm_num; i++) {
      if (resp->pub_shm_infos[i].pid == pub_info.pid) listed = true;
    }
    if (listed) continue;
    if (resp->pub_shm_num == AGNOCAST_PROTO_MAX_PUBLISHER_NUM) {
      send_response(fd, ENOBUFS);
      return;
    }
    // -EEXIST only means the daemon already counts the caller as a user of the
    // pool; the client decides for itself whether it still has to map it.
    allocator_.reference_memory(proc_info->mempool_entry, pid);
    auto & info = resp->pub_shm_infos[resp->pub_shm_num++];
    info.pid = pub_info.pid;
    info.shm_addr = proc_info->mempool_entry->addr;
    info.shm_size = allocator_.mempool_size_bytes();
  }
  const uint32_t bytes = static_cast<uint32_t>(
    offsetof(GetPublisherShmInfosResponse, pub_shm_infos) +
    resp->pub_shm_num * sizeof(AgnocastPublisherShmInfo));
  send_response(fd, 0, resp.get(), bytes);
}

// ============================================================
// Daemon restart: re-registration
// ============================================================

namespace
{

std::string bounded_name(const char * name, size_t size)
{
  return std::string(name, strnlen(name, size));
}

}  // namespace

void CommandHandlers::handle_restore_process(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestoreProcessRequest *>(payload);

  std::unique_lock glock(store_.global_mutex_);
  if (ProcessInfo * existing = store_.find_process(pid)) {
    if (!existing->exited) {
      send_response(fd, EEXIST);
      return;
    }
    // A stale record of an earlier process that had the same pid.
    store_.proc_info_map_.erase(pid);
  }

  MemoryAllocator::MempoolEntry * entry = allocator_.claim_memory(pid, req->shm_addr);
  if (entry == nullptr) {
    send_response(fd, EINVAL);
    return;
  }

  ProcessInfo proc;
  proc.pid = pid;
  proc.domain_id = req->domain_id;
  proc.is_performance_bridge_manager = req->is_performance_bridge_manager;
  proc.mempool_entry = entry;
  store_.proc_info_map_.emplace(pid, std::move(proc));
  persist_state_locked();
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_mappings(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestoreMappingsRequest *>(payload);
  const uint32_t num = std::min<uint32_t>(req->num, AGNOCAST_RESTORE_MAPPING_BATCH);
  for (uint32_t i = 0; i < num; i++) {
    // -EEXIST (already counted) and -EINVAL (not a pool start) change nothing.
    allocator_.reference_memory_at(req->shm_addrs[i], pid);
  }
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_topic(int fd, pid_t pid, const void * payload, int shm_fd)
{
  if (!payload || shm_fd < 0) {
    if (shm_fd >= 0) close(shm_fd);
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestoreTopicRequest *>(payload);
  const std::string topic_name = bounded_name(req->topic_name, sizeof(req->topic_name));

  std::unique_lock glock(store_.global_mutex_);
  const size_t topic_num_before = store_.topic_map_.size();
  TopicWrapper * wrapper = nullptr;
  const int ret =
    store_.adopt_topic(TopicKey{topic_name, store_.get_process_domain_id(pid)}, shm_fd, &wrapper);
  if (ret == 0 && store_.topic_map_.size() > topic_num_before) {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    recovery_.adopted_topic_num++;
  }
  if (ret < 0) {
    fprintf(
      stderr, "agnocast_daemon: restoring topic %s for pid=%d failed: %s\n", topic_name.c_str(),
      pid, strerror(-ret));
  }
  send_response(fd, -ret);
}

void CommandHandlers::handle_restore_publisher(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const RestorePublisherRequest *>(payload);
  const int32_t id = req->publisher_id;
  if (!topic_shm_valid_id(id)) {
    send_response(fd, EINVAL);
    return;
  }

  std::unique_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(
    pid, bounded_name(req->reg.topic_name, sizeof(req->reg.topic_name)));
  if (!wrapper) {
    send_response(fd, ENOENT);
    return;
  }
  std::unique_lock tlock(wrapper->topic_rwsem);
  if (wrapper->topic.pub_info_map.count(id) > 0) {
    send_response(fd, EEXIST);
    return;
  }
  {
    TopicShmLock lock(wrapper->shm);
    // The segment is the record of what was committed: a client can only know
    // an id whose registration was committed before it was told.
    const TopicShmId & rec = wrapper->shm->ids[id];
    if (rec.kind != TOPIC_SHM_ID_PUBLISHER || rec.pid != pid) {
      send_response(fd, EINVAL);
      return;
    }
    // Subscribers that restored first may have been told nothing about this
    // process's pool; have them ask again.
    topic_shm_request_mmap_update_all(wrapper->shm);
  }

  PublisherInfo info;
  info.id = id;
  info.domain_id = wrapper->domain_id;
  info.pid = pid;
  info.node_name = bounded_name(req->reg.node_name, sizeof(req->reg.node_name));
  info.qos_depth = req->reg.qos_depth;
  info.qos_is_transient_local = req->reg.qos_is_transient_local;
  info.is_bridge = req->reg.is_bridge;
  wrapper->topic.pub_info_map.emplace(id, std::move(info));
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_subscriber(
  int fd, pid_t pid, const void * payload, int notify_eventfd)
{
  auto fail = [&](int err) {
    if (notify_eventfd >= 0) close(notify_eventfd);
    send_response(fd, err);
  };
  if (!payload) {
    fail(EINVAL);
    return;
  }
  const auto * req = static_cast<const RestoreSubscriberRequest *>(payload);
  const int32_t id = req->subscriber_id;
  if (!topic_shm_valid_id(id)) {
    fail(EINVAL);
    return;
  }

  std::unique_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper = store_.find_topic_for_process(
    pid, bounded_name(req->reg.topic_name, sizeof(req->reg.topic_name)));
  if (!wrapper) {
    fail(ENOENT);
    return;
  }
  std::unique_lock tlock(wrapper->topic_rwsem);
  if (wrapper->topic.sub_info_map.count(id) > 0) {
    fail(EEXIST);
    return;
  }
  {
    TopicShmLock lock(wrapper->shm);
    const TopicShmId & rec = wrapper->shm->ids[id];
    if (rec.kind != TOPIC_SHM_ID_SUBSCRIBER || rec.pid != pid) {
      fail(EINVAL);
      return;
    }
    topic_shm_request_mmap_update(wrapper->shm, id);
    // Publishers refetch the eventfds, now including this one.
    wrapper->shm->sub_generation++;
  }

  SubscriberInfo info;
  info.id = id;
  info.domain_id = wrapper->domain_id;
  info.pid = pid;
  info.qos_depth = req->reg.qos_depth;
  info.qos_is_transient_local = req->reg.qos_is_transient_local;
  info.qos_is_reliable = req->reg.qos_is_reliable;
  info.node_name = bounded_name(req->reg.node_name, sizeof(req->reg.node_name));
  info.is_take_sub = req->reg.is_take_sub;
  info.ignore_local_publications = req->reg.ignore_local_publications;
  info.is_bridge = req->reg.is_bridge;
  info.notify_eventfd = notify_eventfd;
  wrapper->topic.sub_info_map.emplace(id, std::move(info));
  send_response(fd, 0);
}

void CommandHandlers::handle_restore_done(int fd, pid_t pid)
{
  bool in_recovery = false;
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    in_recovery = recovery_.in_recovery;
    if (in_recovery) {
      auto it = recovery_.expected.find(pid);
      if (it != recovery_.expected.end() && it->second == RestoreState::Pending) {
        it->second = RestoreState::Restored;
      }
    }
  }
  if (in_recovery) {
    // The recovery thread decides whether this was the last one.
    recovery_cv_.notify_all();
  } else {
    // A client that restored after the recovery ended (or to a daemon that
    // never knew of it): what it did not claim of its own is an orphan now.
    std::unique_lock glock(store_.global_mutex_);
    reconcile_topics_locked([pid](pid_t p) { return p == pid || !pid_alive(p); });
  }
  send_response(fd, 0);
}

void CommandHandlers::reconcile_topics_locked(const std::function<bool(pid_t)> & is_settled)
{
  thread_local std::vector<int32_t> emptied(kTopicShmIdNum);
  uint32_t orphan_pub = 0;
  uint32_t orphan_sub = 0;
  std::vector<TopicKey> empty_topics;

  for (auto & [key, wrapper_ptr] : store_.topic_map_) {
    TopicWrapper * wrapper = wrapper_ptr.get();
    std::unique_lock tlock(wrapper->topic_rwsem);
    {
      TopicShmLock lock(wrapper->shm);
      AgnocastTopicShm * shm = wrapper->shm;
      for (int32_t id = 0; id < static_cast<int32_t>(kTopicShmIdNum); id++) {
        const TopicShmId & rec = shm->ids[id];
        if (rec.kind == TOPIC_SHM_ID_FREE) continue;
        const bool claimed = rec.kind == TOPIC_SHM_ID_PUBLISHER
                               ? wrapper->topic.pub_info_map.count(id) > 0
                               : wrapper->topic.sub_info_map.count(id) > 0;
        if (claimed || !is_settled(rec.pid)) continue;

        if (rec.kind == TOPIC_SHM_ID_SUBSCRIBER) {
          const uint32_t emptied_num = topic_shm_remove_subscriber(shm, id, emptied.data());
          for (uint32_t i = 0; i < emptied_num; i++) {
            wrapper->topic.pub_info_map.erase(emptied[i]);
          }
          orphan_sub++;
        } else {
          // Its client either never learned of it or is gone: no one will
          // publish on it again. Entries still referenced stay until their
          // subscribers let go, as for a publisher whose process exited.
          if (!rec.exited) orphan_pub++;
          topic_shm_remove_publisher(shm, id, true);
        }
      }
    }
    if (wrapper->topic.pub_info_map.empty() && wrapper->topic.sub_info_map.empty()) {
      empty_topics.push_back(key);
    }
  }
  for (const auto & key : empty_topics) {
    store_.topic_map_.erase(key);
  }

  std::lock_guard<std::mutex> lock(recovery_mtx_);
  recovery_.orphan_publisher_num += orphan_pub;
  recovery_.orphan_subscriber_num += orphan_sub;
}

// ============================================================
// Daemon restart: recovery state
// ============================================================

void CommandHandlers::persist_state_locked()
{
  if (state_path_.empty()) return;
  const std::string tmp = state_path_ + ".tmp";
  {
    std::ofstream ofs(tmp, std::ios::trunc);
    if (!ofs) return;
    ofs << "agnocast_daemon " << getpid() << "\n";
    for (const auto & [pid, proc] : store_.proc_info_map_) {
      if (!proc.exited) ofs << pid << "\n";
    }
    // A crash during the recovery must not forget the clients still to restore.
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    if (recovery_.in_recovery) {
      for (const auto & [pid, state] : recovery_.expected) {
        if (state == RestoreState::Pending) ofs << pid << "\n";
      }
    }
  }
  if (rename(tmp.c_str(), state_path_.c_str()) < 0) {
    fprintf(
      stderr, "agnocast_daemon: writing %s failed: %s\n", state_path_.c_str(), strerror(errno));
  }
}

void CommandHandlers::start_recovery(const std::vector<pid_t> & expected_pids, int64_t timeout_ms)
{
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    recovery_ = Recovery{};
    recovery_.recovered = true;
    recovery_.in_recovery = true;
    recovery_.start_ns = monotonic_ns();
    recovery_.deadline_ns = recovery_.start_ns + timeout_ms * 1000000LL;
    for (const pid_t pid : expected_pids) {
      recovery_.expected[pid] = RestoreState::Pending;
    }
  }
  fprintf(
    stderr, "agnocast_daemon: recovering %zu client(s) of the previous daemon\n",
    expected_pids.size());
  recovery_thread_ = std::thread(&CommandHandlers::recovery_thread_main, this);
}

void CommandHandlers::recovery_thread_main()
{
  std::unique_lock lk(recovery_mtx_);
  while (!stop_recovery_thread_ && recovery_.in_recovery) {
    bool settled = true;
    for (auto & [pid, state] : recovery_.expected) {
      if (state != RestoreState::Pending) continue;
      if (pid_alive(pid)) {
        settled = false;
      } else {
        state = RestoreState::Dead;
      }
    }
    if (settled || monotonic_ns() >= recovery_.deadline_ns) {
      lk.unlock();
      finish_recovery();
      return;
    }
    recovery_cv_.wait_for(lk, std::chrono::milliseconds(2));
  }
}

void CommandHandlers::finish_recovery()
{
  std::vector<pid_t> dead;
  {
    std::unique_lock glock(store_.global_mutex_);
    {
      std::lock_guard<std::mutex> lock(recovery_mtx_);
      if (!recovery_.in_recovery || recovery_.finishing) return;
      recovery_.finishing = true;
      for (auto & [pid, state] : recovery_.expected) {
        if (state == RestoreState::Pending && !pid_alive(pid)) state = RestoreState::Dead;
        if (state == RestoreState::Pending) recovery_.timed_out_num++;
        if (state == RestoreState::Dead) dead.push_back(pid);
      }
    }
    // Everyone has had their chance: whatever is unclaimed now is an orphan.
    reconcile_topics_locked([](pid_t) { return true; });
    for (const pid_t pid : dead) {
      allocator_.free_memory(pid);
    }
    {
      std::lock_guard<std::mutex> lock(recovery_mtx_);
      recovery_.in_recovery = false;
      recovery_.finishing = false;
      recovery_.end_ns = monotonic_ns();
    }
    persist_state_locked();
  }
  // The pools of processes that died while the daemon was down were never
  // unlinked by an exit cleanup.
  for (const pid_t pid : dead) {
    unlink_process_shm(pid);
  }
  recovery_cv_.notify_all();

  std::lock_guard<std::mutex> lock(recovery_mtx_);
  uint32_t restored = 0;
  for (const auto & [pid, state] : recovery_.expected) {
    (void)pid;
    if (state == RestoreState::Restored) restored++;
  }
  fprintf(
    stderr,
    "agnocast_daemon: recovery done in %.3f ms (restored %u, dead %zu, timed out %u, topics %u, "
    "orphan pub %u, orphan sub %u)\n",
    static_cast<double>(recovery_.end_ns - recovery_.start_ns) / 1e6, restored, dead.size(),
    recovery_.timed_out_num, recovery_.adopted_topic_num, recovery_.orphan_publisher_num,
    recovery_.orphan_subscriber_num);
}

void CommandHandlers::wait_for_recovery_before_registration(pid_t pid)
{
  std::unique_lock lk(recovery_mtx_);
  if (!recovery_.in_recovery) return;
  (void)pid;
  recovery_cv_.wait(lk, [this] { return !recovery_.in_recovery || stop_recovery_thread_; });
}

void CommandHandlers::wait_recovery_end()
{
  std::unique_lock lk(recovery_mtx_);
  recovery_cv_.wait(lk, [this] { return !recovery_.in_recovery || stop_recovery_thread_; });
}

void CommandHandlers::handle_get_recovery_status(int fd)
{
  GetRecoveryStatusResponse resp{};
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    resp.recovered = recovery_.recovered;
    resp.in_recovery = recovery_.in_recovery;
    resp.expected_num = static_cast<uint32_t>(recovery_.expected.size());
    for (const auto & [pid, state] : recovery_.expected) {
      (void)pid;
      if (state == RestoreState::Restored) resp.restored_num++;
      if (state == RestoreState::Dead) resp.dead_num++;
    }
    resp.timed_out_num = recovery_.timed_out_num;
    resp.adopted_topic_num = recovery_.adopted_topic_num;
    resp.orphan_publisher_num = recovery_.orphan_publisher_num;
    resp.orphan_subscriber_num = recovery_.orphan_subscriber_num;
    resp.start_ns = recovery_.start_ns;
    resp.end_ns = recovery_.end_ns;
  }
  send_response(fd, 0, &resp, sizeof(resp));
}

void CommandHandlers::handle_debug_check_topic(int fd, pid_t pid, const void * payload)
{
  if (!payload) {
    send_response(fd, EINVAL);
    return;
  }
  const auto * req = static_cast<const DebugCheckTopicRequest *>(payload);
  DebugCheckTopicResponse resp{};

  bool in_recovery = false;
  {
    std::lock_guard<std::mutex> lock(recovery_mtx_);
    in_recovery = recovery_.in_recovery;
  }

  std::shared_lock glock(store_.global_mutex_);
  TopicWrapper * wrapper =
    store_.find_topic_for_process(pid, bounded_name(req->topic_name, sizeof(req->topic_name)));
  if (!wrapper) {
    send_response(fd, ENOENT);
    return;
  }
  std::shared_lock tlock(wrapper->topic_rwsem);
  TopicShmLock lock(wrapper->shm);
  AgnocastTopicShm * shm = wrapper->shm;
  resp.shm_violations = topic_shm_check(shm, resp.message, sizeof(resp.message));

  auto fail = [&](const char * what, int32_t id) {
    if (resp.shm_violations == 0 && resp.registry_violations == 0) {
      snprintf(resp.message, sizeof(resp.message), "%s (id %d)", what, id);
    }
    resp.registry_violations++;
  };
  for (int32_t id = 0; id < static_cast<int32_t>(kTopicShmIdNum); id++) {
    const TopicShmId & rec = shm->ids[id];
    if (rec.kind == TOPIC_SHM_ID_PUBLISHER) {
      // An exited publisher stays in the segment, unlisted, until its entries go.
      if (wrapper->topic.pub_info_map.count(id) == 0 && !rec.exited && !in_recovery) {
        fail("publisher in segment but not registered", id);
      }
    } else if (rec.kind == TOPIC_SHM_ID_SUBSCRIBER) {
      if (wrapper->topic.sub_info_map.count(id) == 0 && !in_recovery) {
        fail("subscriber in segment but not registered", id);
      }
    }
  }
  for (const auto & [id, info] : wrapper->topic.pub_info_map) {
    const TopicShmId & rec = shm->ids[id];
    if (rec.kind != TOPIC_SHM_ID_PUBLISHER || rec.pid != info.pid) {
      fail("registered publisher missing from segment", id);
    }
  }
  for (const auto & [id, info] : wrapper->topic.sub_info_map) {
    const TopicShmId & rec = shm->ids[id];
    if (rec.kind != TOPIC_SHM_ID_SUBSCRIBER || rec.pid != info.pid) {
      fail("registered subscriber missing from segment", id);
    }
  }
  resp.rollback_count = shm->rollback_count;
  resp.rollback_record_num = shm->rollback_record_num;
  resp.entry_num = shm->entry_num;
  resp.sub_num = shm->sub_num;
  send_response(fd, 0, &resp, sizeof(resp));
}
