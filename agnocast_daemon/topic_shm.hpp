// SPDX-License-Identifier: Apache-2.0
#pragma once

/**
 * Per-topic shared segment: the message data plane of the daemon backend.
 *
 * The daemon creates one sealed memfd per topic, initializes it, and hands it
 * to every process that registers a publisher or subscriber on the topic. From
 * then on publish, receive, take and release run in the client process itself
 * under the segment's mutex; the daemon is needed only for registration, for
 * mapping a newly appeared publisher's memory, and for exit cleanup.
 *
 * The mutex is process-shared and robust, so a process that dies while holding
 * it does not wedge the topic: the next locker gets EOWNERDEAD and carries on.
 * Everything in the segment is protected by that one mutex; nothing in it is
 * accessed without holding it.
 *
 * Per topic-local id (shared by publishers and subscribers, as in the kernel
 * module) the segment keeps the state the data plane reads and writes. The
 * rest of the registration (node names, QoS reliability, bridge flags, eventfds)
 * stays in the daemon.
 */

#include "protocol.h"

#include <pthread.h>

#include <cerrno>
#include <cstdint>
#include <cstring>

#define AGNOCAST_TOPIC_SHM_MAGIC 0x41475453u /* "AGTS" */

constexpr uint32_t kTopicShmEntrySlots = 4096;
constexpr uint32_t kTopicShmIdNum = AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID;
constexpr uint32_t kTopicShmBitmapWords = kTopicShmIdNum / 64;

enum : uint8_t {
  TOPIC_SHM_ID_FREE = 0,
  TOPIC_SHM_ID_PUBLISHER = 1,
  TOPIC_SHM_ID_SUBSCRIBER = 2,
};

struct TopicShmId
{
  uint8_t kind;
  uint8_t transient_local;
  uint8_t is_take_sub;      /* subscriber */
  uint8_t ignore_local;     /* subscriber */
  uint8_t need_mmap_update; /* subscriber: a publisher's memory is not mapped yet */
  uint8_t exited;           /* publisher: its process has exited */
  uint8_t _pad[2];
  int32_t pid;
  uint32_t qos_depth;
  uint32_t entries_num; /* publisher */
  uint32_t _pad2;
  int64_t latest_received_entry_id; /* subscriber */
};

struct TopicShmEntry
{
  int64_t entry_id;
  uint64_t msg_virtual_address;
  int32_t publisher_id;
  uint32_t ref_num;
  uint64_t refs[kTopicShmBitmapWords];
};

struct AgnocastTopicShm
{
  uint32_t magic;
  uint32_t _pad;
  pthread_mutex_t mutex;
  /* Bumped whenever a subscriber is added or removed, so a publisher knows when
   * its cached copy of the subscribers' eventfds is out of date. */
  uint64_t sub_generation;
  int64_t current_entry_id;
  uint32_t entry_num; /* live entries; order[0, entry_num) */
  uint32_t free_num;  /* free_slots[0, free_num) */
  uint32_t sub_num;   /* sub_ids[0, sub_num) */
  uint32_t _pad2;
  uint16_t order[kTopicShmEntrySlots]; /* slot indices by ascending entry_id */
  uint16_t free_slots[kTopicShmEntrySlots];
  uint16_t sub_ids[kTopicShmIdNum]; /* registered subscribers, so publish need not scan ids[] */
  TopicShmId ids[kTopicShmIdNum];
  TopicShmEntry entries[kTopicShmEntrySlots];
};

static_assert(sizeof(TopicShmId) == 32, "TopicShmId must be 32 bytes");
static_assert(kTopicShmEntrySlots <= 65536, "slot indices are 16 bit");

// ============================================================
// Setup and locking
// ============================================================

// For a zero-filled mapping that nobody else has seen yet.
inline int topic_shm_init(AgnocastTopicShm * shm)
{
  pthread_mutexattr_t attr;
  if (pthread_mutexattr_init(&attr) != 0) return -ENOMEM;
  pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
  pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
  const int ret = pthread_mutex_init(&shm->mutex, &attr);
  pthread_mutexattr_destroy(&attr);
  if (ret != 0) return -ret;

  shm->free_num = kTopicShmEntrySlots;
  for (uint32_t i = 0; i < kTopicShmEntrySlots; i++) {
    shm->free_slots[i] = static_cast<uint16_t>(kTopicShmEntrySlots - 1 - i);
  }
  __atomic_store_n(&shm->magic, AGNOCAST_TOPIC_SHM_MAGIC, __ATOMIC_RELEASE);
  return 0;
}

inline void topic_shm_lock(AgnocastTopicShm * shm)
{
  const int ret = pthread_mutex_lock(&shm->mutex);
  if (ret == EOWNERDEAD) {
    // The previous holder died mid-operation. The tables are kept usable
    // rather than consistent: an operation it left half done may leak an entry
    // until the daemon's exit cleanup for that process runs.
    pthread_mutex_consistent(&shm->mutex);
  }
}

inline void topic_shm_unlock(AgnocastTopicShm * shm)
{
  pthread_mutex_unlock(&shm->mutex);
}

class TopicShmLock
{
public:
  explicit TopicShmLock(AgnocastTopicShm * shm) : shm_(shm) { topic_shm_lock(shm_); }
  ~TopicShmLock() { topic_shm_unlock(shm_); }
  TopicShmLock(const TopicShmLock &) = delete;
  TopicShmLock & operator=(const TopicShmLock &) = delete;

private:
  AgnocastTopicShm * shm_;
};

// ============================================================
// Entry table (caller holds the mutex for everything below)
// ============================================================

inline bool topic_shm_valid_id(int32_t id)
{
  return id >= 0 && id < static_cast<int32_t>(kTopicShmIdNum);
}

inline TopicShmEntry & topic_shm_entry_at(AgnocastTopicShm * shm, uint32_t pos)
{
  return shm->entries[shm->order[pos]];
}

// Position in order[] of the first entry with entry_id >= id.
inline uint32_t topic_shm_lower_bound(const AgnocastTopicShm * shm, int64_t entry_id)
{
  uint32_t lo = 0;
  uint32_t hi = shm->entry_num;
  while (lo < hi) {
    const uint32_t mid = lo + (hi - lo) / 2;
    if (shm->entries[shm->order[mid]].entry_id < entry_id) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

inline TopicShmEntry * topic_shm_find_entry(AgnocastTopicShm * shm, int64_t entry_id)
{
  const uint32_t pos = topic_shm_lower_bound(shm, entry_id);
  if (pos == shm->entry_num) return nullptr;
  TopicShmEntry & en = topic_shm_entry_at(shm, pos);
  return en.entry_id == entry_id ? &en : nullptr;
}

inline bool topic_shm_test_ref(const TopicShmEntry & en, int32_t id)
{
  return (en.refs[id / 64] >> (id % 64)) & 1U;
}

// false when the bit was already set.
inline bool topic_shm_set_ref(TopicShmEntry & en, int32_t id)
{
  const uint64_t bit = uint64_t{1} << (id % 64);
  if (en.refs[id / 64] & bit) return false;
  en.refs[id / 64] |= bit;
  en.ref_num++;
  return true;
}

// false when the bit was not set.
inline bool topic_shm_clear_ref(TopicShmEntry & en, int32_t id)
{
  const uint64_t bit = uint64_t{1} << (id % 64);
  if ((en.refs[id / 64] & bit) == 0) return false;
  en.refs[id / 64] &= ~bit;
  en.ref_num--;
  return true;
}

// Removes the entry at order[pos] and returns its slot to the free list. The
// owning publisher's entries_num is the caller's to adjust.
inline void topic_shm_remove_at(AgnocastTopicShm * shm, uint32_t pos)
{
  const uint16_t slot = shm->order[pos];
  std::memmove(
    &shm->order[pos], &shm->order[pos + 1], (shm->entry_num - pos - 1) * sizeof(shm->order[0]));
  shm->entry_num--;
  shm->free_slots[shm->free_num++] = slot;
}

inline int topic_shm_insert(
  AgnocastTopicShm * shm, int32_t publisher_id, uint64_t msg_virtual_address, int64_t * entry_id)
{
  if (shm->free_num == 0) return -ENOSPC;
  const uint16_t slot = shm->free_slots[--shm->free_num];
  TopicShmEntry & en = shm->entries[slot];
  en.entry_id = shm->current_entry_id++;
  en.msg_virtual_address = msg_virtual_address;
  en.publisher_id = publisher_id;
  en.ref_num = 0;
  std::memset(en.refs, 0, sizeof(en.refs));
  // Entry ids only grow, so the new entry is the newest.
  shm->order[shm->entry_num++] = slot;
  shm->ids[publisher_id].entries_num++;
  *entry_id = en.entry_id;
  return 0;
}

// Mirrors release_msgs_to_meet_depth: hands back up to
// AGNOCAST_PROTO_MAX_RELEASE_NUM unreferenced entries of the publisher that are
// beyond its keep-last depth, oldest first.
inline int topic_shm_release_to_meet_depth(
  AgnocastTopicShm * shm, int32_t publisher_id, uint64_t * released_addrs, uint32_t * released_num)
{
  *released_num = 0;
  TopicShmId & pub = shm->ids[publisher_id];
  if (pub.entries_num <= pub.qos_depth) return 0;

  uint32_t num_search_entries = pub.entries_num - pub.qos_depth;
  uint32_t pos = 0;
  while (num_search_entries > 0 && *released_num < AGNOCAST_PROTO_MAX_RELEASE_NUM) {
    if (pos == shm->entry_num) return -ENODATA;
    TopicShmEntry & en = topic_shm_entry_at(shm, pos);
    if (en.publisher_id != publisher_id) {
      pos++;
      continue;
    }
    num_search_entries--;
    if (en.ref_num > 0) {
      pos++;
      continue;
    }
    released_addrs[(*released_num)++] = en.msg_virtual_address;
    topic_shm_remove_at(shm, pos);
    pub.entries_num--;
  }
  return 0;
}

// A whole publish: insert, give back what the depth no longer covers, and list
// the subscribers to wake.
inline int topic_shm_publish(
  AgnocastTopicShm * shm, int32_t publisher_id, uint64_t msg_virtual_address, int64_t * entry_id,
  uint64_t * released_addrs, uint32_t * released_num, int32_t * subscriber_ids,
  uint32_t * subscriber_num)
{
  *subscriber_num = 0;
  if (!topic_shm_valid_id(publisher_id)) return -EINVAL;
  const TopicShmId & pub = shm->ids[publisher_id];
  if (pub.kind != TOPIC_SHM_ID_PUBLISHER) return -EINVAL;

  int ret = topic_shm_insert(shm, publisher_id, msg_virtual_address, entry_id);
  if (ret < 0) return ret;
  ret = topic_shm_release_to_meet_depth(shm, publisher_id, released_addrs, released_num);
  if (ret < 0) return ret;

  for (uint32_t i = 0; i < shm->sub_num; i++) {
    const uint16_t id = shm->sub_ids[i];
    const TopicShmId & sub = shm->ids[id];
    if (sub.is_take_sub) continue;
    if (sub.ignore_local && sub.pid == pub.pid) continue;
    subscriber_ids[(*subscriber_num)++] = id;
  }
  return 0;
}

// Mirrors receive_msg_core. Returns -EAGAIN, without touching anything, when
// the subscriber has a publisher's memory still to map; that information only
// travels in the daemon's receive reply.
inline int topic_shm_receive(
  AgnocastTopicShm * shm, int32_t subscriber_id, bool via_daemon, int64_t * entry_ids,
  uint64_t * entry_addrs, uint16_t * entry_num, bool * call_again)
{
  *entry_num = 0;
  *call_again = false;
  if (!topic_shm_valid_id(subscriber_id)) return -EINVAL;
  TopicShmId & sub = shm->ids[subscriber_id];
  if (sub.kind != TOPIC_SHM_ID_SUBSCRIBER) return -EINVAL;
  if (sub.need_mmap_update && !via_daemon) return -EAGAIN;
  if (shm->entry_num == 0) return 0;

  const int64_t newest_entry_id = topic_shm_entry_at(shm, shm->entry_num - 1).entry_id;
  const int64_t qos_start = newest_entry_id - static_cast<int64_t>(sub.qos_depth) + 1;
  const int64_t start_entry_id =
    (qos_start > sub.latest_received_entry_id) ? qos_start : (sub.latest_received_entry_id + 1);

  for (uint32_t pos = topic_shm_lower_bound(shm, start_entry_id); pos < shm->entry_num; pos++) {
    TopicShmEntry & en = topic_shm_entry_at(shm, pos);
    if (*entry_num == AGNOCAST_PROTO_MAX_RECEIVE_NUM) {
      *call_again = true;
      break;
    }
    const TopicShmId & pub = shm->ids[en.publisher_id];
    if (pub.kind != TOPIC_SHM_ID_PUBLISHER) return -ENODATA;
    if (pub.exited) continue;
    if (sub.ignore_local && sub.pid == pub.pid) continue;
    if (!topic_shm_set_ref(en, subscriber_id)) return -EALREADY;

    entry_ids[*entry_num] = en.entry_id;
    entry_addrs[*entry_num] = en.msg_virtual_address;
    (*entry_num)++;
  }
  if (*entry_num > 0) sub.latest_received_entry_id = entry_ids[*entry_num - 1];
  return 0;
}

inline int topic_shm_release_ref(AgnocastTopicShm * shm, int32_t subscriber_id, int64_t entry_id)
{
  if (!topic_shm_valid_id(subscriber_id)) return -EINVAL;
  TopicShmEntry * en = topic_shm_find_entry(shm, entry_id);
  if (en == nullptr) return -EINVAL;
  if (!topic_shm_clear_ref(*en, subscriber_id)) return -EINVAL;
  return 0;
}

// Mirrors the take handler's search: within the subscriber's depth, the oldest
// eligible entry newer than what it last took.
inline int topic_shm_take(
  AgnocastTopicShm * shm, int32_t subscriber_id, bool allow_same_message, uint64_t * addr,
  int64_t * entry_id)
{
  *addr = 0;
  *entry_id = -1;
  if (!topic_shm_valid_id(subscriber_id)) return -EINVAL;
  TopicShmId & sub = shm->ids[subscriber_id];
  if (sub.kind != TOPIC_SHM_ID_SUBSCRIBER) return -EINVAL;

  uint32_t searched_count = 0;
  TopicShmEntry * candidate = nullptr;
  for (uint32_t pos = shm->entry_num; pos-- > 0;) {
    TopicShmEntry & en = topic_shm_entry_at(shm, pos);
    if (searched_count >= sub.qos_depth) break;
    if (!allow_same_message && en.entry_id == sub.latest_received_entry_id) break;
    if (en.entry_id < sub.latest_received_entry_id) break;

    const TopicShmId & pub = shm->ids[en.publisher_id];
    if (pub.kind != TOPIC_SHM_ID_PUBLISHER) return -ENODATA;
    if (pub.exited) continue;
    if (sub.ignore_local && sub.pid == pub.pid) continue;

    candidate = &en;
    searched_count++;
  }

  if (candidate != nullptr) {
    const bool already_referenced =
      allow_same_message && topic_shm_test_ref(*candidate, subscriber_id);
    if (!already_referenced && !topic_shm_set_ref(*candidate, subscriber_id)) return -EALREADY;
    *addr = candidate->msg_virtual_address;
    *entry_id = candidate->entry_id;
    sub.latest_received_entry_id = candidate->entry_id;
  }
  return 0;
}

// Drops the publisher's unreferenced entries; returns how many it still has.
inline uint32_t topic_shm_remove_unreferenced_of(AgnocastTopicShm * shm, int32_t publisher_id)
{
  TopicShmId & pub = shm->ids[publisher_id];
  for (uint32_t pos = 0; pos < shm->entry_num;) {
    TopicShmEntry & en = topic_shm_entry_at(shm, pos);
    if (en.publisher_id == publisher_id && en.ref_num == 0) {
      topic_shm_remove_at(shm, pos);
      pub.entries_num--;
    } else {
      pos++;
    }
  }
  return pub.entries_num;
}

inline void topic_shm_add_publisher(
  AgnocastTopicShm * shm, int32_t id, int32_t pid, uint32_t qos_depth, bool transient_local)
{
  TopicShmId & pub = shm->ids[id];
  std::memset(&pub, 0, sizeof(pub));
  pub.kind = TOPIC_SHM_ID_PUBLISHER;
  pub.pid = pid;
  pub.qos_depth = qos_depth;
  pub.transient_local = transient_local;
  // Its memory is new to every existing subscriber.
  for (uint32_t i = 0; i < shm->sub_num; i++) {
    shm->ids[shm->sub_ids[i]].need_mmap_update = 1;
  }
}

inline void topic_shm_add_subscriber(
  AgnocastTopicShm * shm, int32_t id, int32_t pid, uint32_t qos_depth, bool transient_local,
  bool is_take_sub, bool ignore_local)
{
  TopicShmId & sub = shm->ids[id];
  std::memset(&sub, 0, sizeof(sub));
  sub.kind = TOPIC_SHM_ID_SUBSCRIBER;
  sub.pid = pid;
  sub.qos_depth = qos_depth;
  sub.transient_local = transient_local;
  sub.is_take_sub = is_take_sub;
  sub.ignore_local = ignore_local;
  sub.need_mmap_update = 1;
  sub.latest_received_entry_id = transient_local ? -1 : shm->current_entry_id++;
  shm->sub_ids[shm->sub_num++] = static_cast<uint16_t>(id);
  shm->sub_generation++;
}

inline void topic_shm_clear_id(AgnocastTopicShm * shm, int32_t id)
{
  if (shm->ids[id].kind == TOPIC_SHM_ID_SUBSCRIBER) {
    for (uint32_t i = 0; i < shm->sub_num; i++) {
      if (shm->sub_ids[i] != id) continue;
      shm->sub_ids[i] = shm->sub_ids[--shm->sub_num];
      break;
    }
    shm->sub_generation++;
  }
  std::memset(&shm->ids[id], 0, sizeof(shm->ids[id]));
}

// Mirrors cleanup_unreferenced_entries_from_exited_publishers for a removed
// subscriber: drops its references, then any entry no one references whose
// publisher's process is gone. Publisher ids left with no entries are written
// to emptied_publishers so the caller can drop its own records of them.
inline uint32_t topic_shm_cleanup_after_subscriber(
  AgnocastTopicShm * shm, int32_t subscriber_id, int32_t * emptied_publishers)
{
  uint32_t emptied_num = 0;
  for (uint32_t pos = 0; pos < shm->entry_num;) {
    TopicShmEntry & en = topic_shm_entry_at(shm, pos);
    topic_shm_clear_ref(en, subscriber_id);
    TopicShmId & pub = shm->ids[en.publisher_id];
    if (en.ref_num > 0 || pub.kind != TOPIC_SHM_ID_PUBLISHER || !pub.exited) {
      pos++;
      continue;
    }
    const int32_t publisher_id = en.publisher_id;
    topic_shm_remove_at(shm, pos);
    if (--pub.entries_num == 0) {
      emptied_publishers[emptied_num++] = publisher_id;
      topic_shm_clear_id(shm, publisher_id);
    }
  }
  return emptied_num;
}
