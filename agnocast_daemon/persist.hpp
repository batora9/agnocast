// SPDX-License-Identifier: Apache-2.0
#pragma once

// Persistence hooks for comparing the ways a restarted daemon can get its
// metadata back (protocol.h, "Daemon restart recovery"):
//
//   (A) a persistent region the daemon writes in place, with an undo log
//   (B) a redo log of every change, replayed at start
//   (C) nothing persisted: the clients report what they know
//
// The handlers call the hooks below at every change of the metadata a
// restart needs. Without a sink (C, and the unit tests) each hook is one
// pointer test. A sink buffers or applies the changes of the running request
// and makes them durable in commit(), which the daemon calls before it
// answers (send_response) while it still holds the locks of the request, so
// the order of commits is the order of the changes on every topic.
//
// Derived values (a publisher's entry count, which subscribers need a memory
// update) are not persisted: load() recomputes them.

#include "memory_allocator.hpp"
#include "metadata_store.hpp"

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

namespace persist
{

class Sink
{
public:
  virtual ~Sink() = default;

  virtual void topic_put(TopicWrapper & topic) = 0;
  virtual void topic_del(TopicWrapper & topic) = 0;
  virtual void counters(TopicWrapper & topic) = 0;  // current_pubsub_id, current_entry_id

  virtual void proc_put(ProcessInfo & proc) = 0;
  virtual void proc_del(ProcessInfo & proc) = 0;
  // pid mapped the pool at addr (its own one comes with proc_put)
  virtual void map_put(pid_t pid, uint64_t addr) = 0;

  virtual void pub_put(TopicWrapper & topic, PublisherInfo & pub) = 0;
  virtual void pub_del(TopicWrapper & topic, PublisherInfo & pub) = 0;
  virtual void sub_put(TopicWrapper & topic, SubscriberInfo & sub) = 0;
  virtual void sub_del(TopicWrapper & topic, SubscriberInfo & sub) = 0;
  virtual void sub_cursor(TopicWrapper & topic, SubscriberInfo & sub) = 0;

  virtual void entry_put(TopicWrapper & topic, EntryNode & entry) = 0;
  virtual void entry_del(TopicWrapper & topic, EntryNode & entry) = 0;
  virtual void ref(TopicWrapper & topic, EntryNode & entry, int32_t sub_id, bool on) = 0;

  // Makes the calling thread's changes since its last commit durable.
  virtual void commit() = 0;

  // Rebuilds the store from what was persisted. False when there is nothing.
  virtual bool load(MetadataStore & store, MemoryAllocator & allocator) = 0;

  // Bytes written to the persistent medium so far (for the evaluation).
  virtual uint64_t bytes_written() const = 0;
};

// The daemon's sink, or null. Set once before any client is served.
extern Sink * g_sink;

// Created by the persistence implementation linked into the daemon; null
// when none is (C). AGNOCAST_DAEMON_PERSIST=0 also yields null.
std::unique_ptr<Sink> make_sink();

// Builds a store from persisted records; shared by the implementations.
class Builder
{
public:
  Builder(MetadataStore & store, MemoryAllocator & allocator) : store_(store), allocator_(allocator)
  {
  }

  TopicWrapper * topic(
    int32_t tid, const std::string & name, uint32_t domain_id, int32_t current_pubsub_id,
    int64_t current_entry_id);
  void topic_del(int32_t tid);
  void proc(
    pid_t pid, uint64_t starttime, uint32_t domain_id, bool bridge_manager, uint64_t pool_addr);
  void proc_del(pid_t pid);
  void map(pid_t pid, uint64_t addr);
  void pub(
    int32_t tid, int32_t id, pid_t pid, const std::string & node, uint32_t depth,
    bool transient_local, bool is_bridge);
  void pub_del(int32_t tid, int32_t id);
  void sub(
    int32_t tid, int32_t id, pid_t pid, const std::string & node, uint32_t depth,
    bool transient_local, bool reliable, bool take, bool ignore_local, bool is_bridge,
    int64_t cursor);
  void sub_cursor(int32_t tid, int32_t id, int64_t cursor);
  void sub_del(int32_t tid, int32_t id);
  void entry(int32_t tid, int64_t id, int32_t pub_id, uint64_t addr);
  void entry_del(int32_t tid, int64_t id);
  void ref(int32_t tid, int64_t entry_id, int32_t sub_id, bool on);

  // Recomputes the derived values. Returns the topics by tid.
  const std::unordered_map<int32_t, TopicWrapper *> & finish();

private:
  TopicWrapper * by_tid(int32_t tid);

  MetadataStore & store_;
  MemoryAllocator & allocator_;
  std::unordered_map<int32_t, TopicWrapper *> topics_;
};

}  // namespace persist

#define PERSIST(call)                                      \
  do {                                                     \
    if (persist::g_sink != nullptr) persist::g_sink->call; \
  } while (0)
