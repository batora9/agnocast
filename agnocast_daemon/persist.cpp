// SPDX-License-Identifier: Apache-2.0
#include "persist.hpp"

namespace persist
{

Sink * g_sink = nullptr;

TopicWrapper * Builder::by_tid(int32_t tid)
{
  const auto it = topics_.find(tid);
  return it == topics_.end() ? nullptr : it->second;
}

TopicWrapper * Builder::topic(
  int32_t tid, const std::string & name, uint32_t domain_id, int32_t current_pubsub_id,
  int64_t current_entry_id)
{
  TopicWrapper * w = store_.find_or_create_topic(TopicKey{name, domain_id});
  w->persist_tid = tid;
  w->topic.current_pubsub_id = std::max(w->topic.current_pubsub_id, current_pubsub_id);
  w->topic.current_entry_id = std::max(w->topic.current_entry_id, current_entry_id);
  topics_[tid] = w;
  return w;
}

void Builder::topic_del(int32_t tid)
{
  TopicWrapper * w = by_tid(tid);
  if (w == nullptr) return;
  topics_.erase(tid);
  store_.topic_map_.erase(TopicKey{w->key, w->domain_id});
}

void Builder::proc(
  pid_t pid, uint64_t starttime, uint32_t domain_id, bool bridge_manager, uint64_t pool_addr)
{
  if (store_.find_process(pid) != nullptr) return;
  ProcessInfo p;
  p.pid = pid;
  p.starttime = starttime;
  p.domain_id = domain_id;
  p.is_performance_bridge_manager = bridge_manager;
  p.mempool_entry = allocator_.restore_assignment(pid, pool_addr);
  store_.proc_info_map_.emplace(pid, std::move(p));
}

void Builder::proc_del(pid_t pid)
{
  allocator_.free_memory(pid);
  store_.proc_info_map_.erase(pid);
}

void Builder::map(pid_t pid, uint64_t addr)
{
  if (MemoryAllocator::MempoolEntry * e = allocator_.find_entry(addr)) {
    allocator_.reference_memory(e, pid);
  }
}

void Builder::pub(
  int32_t tid, int32_t id, pid_t pid, const std::string & node, uint32_t depth,
  bool transient_local, bool is_bridge)
{
  TopicWrapper * w = by_tid(tid);
  if (w == nullptr) return;
  PublisherInfo info;
  info.id = id;
  info.domain_id = w->domain_id;
  info.pid = pid;
  info.node_name = node;
  info.qos_depth = depth;
  info.qos_is_transient_local = transient_local;
  info.is_bridge = is_bridge;
  w->topic.pub_info_map[id] = std::move(info);
}

void Builder::pub_del(int32_t tid, int32_t id)
{
  if (TopicWrapper * w = by_tid(tid)) w->topic.pub_info_map.erase(id);
}

void Builder::sub(
  int32_t tid, int32_t id, pid_t pid, const std::string & node, uint32_t depth,
  bool transient_local, bool reliable, bool take, bool ignore_local, bool is_bridge, int64_t cursor)
{
  TopicWrapper * w = by_tid(tid);
  if (w == nullptr) return;
  SubscriberInfo info;
  info.id = id;
  info.domain_id = w->domain_id;
  info.pid = pid;
  info.node_name = node;
  info.qos_depth = depth;
  info.qos_is_transient_local = transient_local;
  info.qos_is_reliable = reliable;
  info.is_take_sub = take;
  info.ignore_local_publications = ignore_local;
  info.is_bridge = is_bridge;
  info.latest_received_entry_id = cursor;
  info.need_mmap_update = true;
  w->topic.sub_info_map.erase(id);
  w->topic.sub_info_map.emplace(id, std::move(info));
}

void Builder::sub_cursor(int32_t tid, int32_t id, int64_t cursor)
{
  TopicWrapper * w = by_tid(tid);
  if (w == nullptr) return;
  const auto it = w->topic.sub_info_map.find(id);
  if (it != w->topic.sub_info_map.end()) it->second.latest_received_entry_id = cursor;
}

void Builder::sub_del(int32_t tid, int32_t id)
{
  if (TopicWrapper * w = by_tid(tid)) w->topic.sub_info_map.erase(id);
}

void Builder::entry(int32_t tid, int64_t id, int32_t pub_id, uint64_t addr)
{
  TopicWrapper * w = by_tid(tid);
  if (w == nullptr) return;
  auto [it, inserted] = w->topic.entries.try_emplace(id);
  (void)inserted;
  it->second.entry_id = id;
  it->second.publisher_id = pub_id;
  it->second.msg_virtual_address = addr;
}

void Builder::entry_del(int32_t tid, int64_t id)
{
  if (TopicWrapper * w = by_tid(tid)) w->topic.entries.erase(id);
}

void Builder::ref(int32_t tid, int64_t entry_id, int32_t sub_id, bool on)
{
  TopicWrapper * w = by_tid(tid);
  if (
    w == nullptr || sub_id < 0 ||
    sub_id >= static_cast<int32_t>(AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID)) {
    return;
  }
  const auto it = w->topic.entries.find(entry_id);
  if (it == w->topic.entries.end()) return;
  if (on) {
    it->second.referencing_subscribers.test_and_set(static_cast<size_t>(sub_id));
  } else {
    it->second.referencing_subscribers.test_and_clear(static_cast<size_t>(sub_id));
  }
}

const std::unordered_map<int32_t, TopicWrapper *> & Builder::finish()
{
  for (auto & [tid, w] : topics_) {
    (void)tid;
    for (auto & [id, pub] : w->topic.pub_info_map) {
      (void)id;
      pub.entries_num = 0;
    }
    for (auto & [eid, en] : w->topic.entries) {
      (void)eid;
      const auto p = w->topic.pub_info_map.find(en.publisher_id);
      if (p != w->topic.pub_info_map.end()) p->second.entries_num++;
    }
  }
  return topics_;
}

}  // namespace persist
