// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "memory_allocator.hpp"
#include "topic_shm.hpp"

#include <sys/types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// ============================================================
// Data structures — userspace equivalents of agnocast_internal.h
// ============================================================

struct TopicKey
{
  std::string name;
  uint32_t domain_id = 0;

  bool operator==(const TopicKey & other) const
  {
    return domain_id == other.domain_id && name == other.name;
  }
};

struct TopicKeyHash
{
  std::size_t operator()(const TopicKey & key) const
  {
    return std::hash<std::string>{}(key.name) ^ (static_cast<std::size_t>(key.domain_id) << 1);
  }
};

struct ExitSubscriptionEntry
{
  std::string topic_name;
  int32_t subscriber_id = 0;  // topic_local_id_t
};

// Equivalent of agnocast_kmod::process_info.
// ipc_namespace is omitted: container isolation is a future concern.
struct ProcessInfo
{
  bool exited = false;
  bool is_performance_bridge_manager = false;
  pid_t pid = 0;
  // The process's ROS_DOMAIN_ID (0 if unset), fixed for the process's lifetime.
  uint32_t domain_id = 0;
  MemoryAllocator::MempoolEntry * mempool_entry = nullptr;  // owned by MemoryAllocator
  std::vector<ExitSubscriptionEntry> exit_subscriptions;
  // /proc/<pid>/stat start time, so a reused pid is not taken for this process.
  uint64_t starttime = 0;
};

// Equivalent of agnocast_kmod::publisher_info.
struct PublisherInfo
{
  int32_t id = 0;  // topic_local_id_t
  uint32_t domain_id = 0;
  pid_t pid = 0;
  std::string node_name;
  uint32_t qos_depth = 0;
  bool qos_is_transient_local = false;
  bool is_bridge = false;
};

// Equivalent of agnocast_kmod::subscriber_info.
struct SubscriberInfo
{
  int32_t id = 0;  // topic_local_id_t
  uint32_t domain_id = 0;
  pid_t pid = 0;
  uint32_t qos_depth = 0;
  bool qos_is_transient_local = false;
  bool qos_is_reliable = false;
  std::string node_name;
  bool is_take_sub = false;
  bool ignore_local_publications = false;
  bool is_bridge = false;
  // The subscriber's notification eventfd, received over SCM_RIGHTS at
  // registration and owned by the daemon from then on (-1 when none was sent).
  // The kernel module resolves the caller's fd to an eventfd_ctx instead; a
  // userspace daemon cannot, so the descriptor itself has to be passed.
  int notify_eventfd = -1;
};

// Equivalent of agnocast_kmod::topic_struct, minus what the data plane needs:
// the entries (the kernel module's red-black tree), the reference bits, and the
// per-publisher and per-subscriber counters live in the topic's shared segment.
struct TopicStruct
{
  std::unordered_map<int32_t, PublisherInfo> pub_info_map;
  std::unordered_map<int32_t, SubscriberInfo> sub_info_map;
  // Ids come from the segment's next_pubsub_id, which survives a daemon restart.
  uint32_t ros2_subscriber_num = 0;
  uint32_t ros2_publisher_num = 0;
};

// Equivalent of agnocast_kmod::topic_wrapper.
// Heap-allocated (via unique_ptr in MetadataStore::topic_map_) because
// std::shared_mutex is neither copyable nor movable.
struct TopicWrapper
{
  std::string key;
  uint32_t domain_id = 0;

  // Lock ordering: global_mutex_ (MetadataStore) → topic_rwsem → shm->mutex →
  // mutex_ (MemoryAllocator).
  // shared_lock  for queries and for anything that touches only the segment.
  // unique_lock  for changes to pub_info_map / sub_info_map.
  mutable std::shared_mutex topic_rwsem;

  TopicStruct topic;

  // The topic's shared segment (topic_shm.hpp), sealed against resizing so a
  // client holding the descriptor cannot turn the daemon's accesses into SIGBUS.
  int shm_fd = -1;
  AgnocastTopicShm * shm = nullptr;

  // Anomaly handling (topic_rwsem). A quarantined process damaged the segment
  // and is no longer served on this topic.
  std::set<pid_t> quarantined;
  uint32_t rotation_num = 0;
  uint32_t stall_kill_num = 0;
  uint32_t anomaly_report_num = 0;

  TopicWrapper() = default;
  ~TopicWrapper();
  TopicWrapper(const TopicWrapper &) = delete;
  TopicWrapper & operator=(const TopicWrapper &) = delete;
  TopicWrapper(TopicWrapper &&) = delete;
  TopicWrapper & operator=(TopicWrapper &&) = delete;
};

// Equivalent of agnocast_kmod::bridge_info.
struct BridgeInfo
{
  std::string topic_name;
  pid_t pid = 0;
  bool has_r2a = false;  // ROS2 → Agnocast
  bool has_a2r = false;  // Agnocast → ROS2
};

// ============================================================
// MetadataStore — owns all runtime state
// ============================================================

// Equivalent of the three global hashtables in agnocast_kmod/agnocast_internal.c
// (topic_hashtable, proc_info_htable, bridge_htable) plus global_htables_rwsem.
//
// Lock ordering (to prevent deadlocks, always acquire in this order):
//   1. global_mutex_  (this class)
//   2. topic_rwsem    (per-topic, in TopicWrapper)
//   3. mutex_         (MemoryAllocator)
class MetadataStore
{
public:
  // shared_lock  when searching / reading without modifying the maps.
  // unique_lock  when inserting or erasing entries from the maps.
  mutable std::shared_mutex global_mutex_;

  std::unordered_map<TopicKey, std::unique_ptr<TopicWrapper>, TopicKeyHash> topic_map_;
  std::unordered_map<pid_t, ProcessInfo> proc_info_map_;
  std::unordered_map<std::string, BridgeInfo> bridge_map_;

  // Caller must hold global_mutex_ (unique_lock) before calling. nullptr when
  // a new topic's segment could not be created.
  TopicWrapper * find_or_create_topic(const TopicKey & key);

  // Daemon restart: takes over a segment a client still has, given as shm_fd
  // (always consumed). The same segment again is fine; a different one for a
  // topic that already has one is -EEXIST. Caller holds global_mutex_ (unique).
  int adopt_topic(const TopicKey & key, int shm_fd, TopicWrapper ** out);

  // A new, initialized, sealed segment. False when it could not be created.
  static bool create_segment(int * fd, AgnocastTopicShm ** shm);

  // Caller must hold global_mutex_ (at least shared_lock) before calling.
  TopicWrapper * find_topic(const TopicKey & key) const;
  TopicWrapper * find_or_create_topic_for_process(pid_t pid, const std::string & topic_name);
  TopicWrapper * find_topic_for_process(pid_t pid, const std::string & topic_name) const;
  uint32_t get_process_domain_id(pid_t pid) const;
  ProcessInfo * find_process(pid_t pid);
};
