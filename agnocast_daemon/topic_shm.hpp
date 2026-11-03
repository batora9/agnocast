// SPDX-License-Identifier: Apache-2.0
#pragma once

/**
 * Per-topic shared segment: the message data plane of the daemon backend.
 *
 * The daemon creates one sealed memfd per topic, initializes it, and hands it
 * to every process that registers a publisher or subscriber on the topic. From
 * then on publish, receive, take and release run in the client process itself
 * under the segment's mutex; the daemon is needed only for registration, for
 * telling a subscriber where a new publisher's memory is, and for exit cleanup.
 *
 * Everything in the segment is protected by one process-shared, robust mutex;
 * nothing in it is accessed without holding it.
 *
 * Rollback
 * --------
 * Whoever holds the mutex (a client or the daemon) may die in the middle of an
 * operation. Every operation is therefore atomic with respect to such a death:
 *
 *   - Before changing a canonical field it appends the old value to the undo
 *     log in the segment (topic_shm_set / topic_shm_log_id).
 *   - Unlocking commits: the log is truncated just before the mutex is released.
 *   - The next locker that gets EOWNERDEAD replays the log backwards, which puts
 *     every canonical field back to its value before the interrupted operation,
 *     and then recomputes the derived fields from the canonical ones
 *     (topic_shm_recover). Replay is idempotent, so a death during recovery is
 *     recovered by the next locker in turn.
 *
 * Canonical state: each slot's live flag and contents, the per-subscriber
 * reference bits, the per-id records, current_entry_id and next_pubsub_id.
 * Derived state, rebuilt on recovery and never logged: order[], free_slots[],
 * entry_num, free_num, sub_ids[], sub_num, ref_num and entries_num.
 *
 * Because the segment outlives the daemon (every client keeps it mapped), the
 * same mechanism lets a restarted daemon take a segment back in a consistent
 * state even when the old daemon died holding its mutex.
 */

#include "protocol.h"

#include <pthread.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <type_traits>

#define AGNOCAST_TOPIC_SHM_MAGIC 0x41475432u /* "AGT2": layout with the undo log */

// Crash points for the daemon's rollback evaluation (fault_injection.cpp). The
// client build never defines AGNOCAST_FAULT_INJECTION, so they cost nothing there.
#ifdef AGNOCAST_FAULT_INJECTION
void agnocast_fault_point(const char * name);
#define TOPIC_SHM_FAULT_POINT(name) agnocast_fault_point(name)
#else
#define TOPIC_SHM_FAULT_POINT(name) static_cast<void>(0)
#endif

constexpr uint32_t kTopicShmEntrySlots = 4096;
constexpr uint32_t kTopicShmIdNum = AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID;
constexpr uint32_t kTopicShmBitmapWords = kTopicShmIdNum / 64;
// The largest operation is removing a subscriber: per entry one reference word
// and one live flag, plus one id record (4 words) per publisher it empties.
constexpr uint32_t kTopicShmUndoCap = 2 * kTopicShmEntrySlots + 4 * kTopicShmIdNum + 64;

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
  uint8_t need_mmap_update; /* subscriber: a publisher's memory may not be mapped yet */
  uint8_t exited;           /* publisher: its process has exited */
  uint8_t _pad[2];
  int32_t pid;
  uint32_t qos_depth;
  uint32_t entries_num; /* publisher; derived */
  uint32_t _pad2;
  int64_t latest_received_entry_id; /* subscriber */
};

struct TopicShmEntry
{
  int64_t entry_id;
  uint64_t msg_virtual_address;
  int32_t publisher_id;
  uint32_t ref_num; /* derived: number of bits set in refs */
  uint32_t live;    /* 1 while the slot holds an entry */
  uint32_t _pad;
  uint64_t refs[kTopicShmBitmapWords];
};

struct TopicShmUndoRecord
{
  uint32_t offset; /* from the start of the segment */
  uint32_t size;   /* 1, 2, 4 or 8 */
  uint64_t old_value;
};

struct AgnocastTopicShm
{
  uint32_t magic;
  uint32_t _pad;
  pthread_mutex_t mutex;
  /* Bumped whenever a subscriber is added or removed, and on every recovery, so
   * a publisher knows when its cached copy of the subscribers' eventfds is out
   * of date. Only ever grows; not logged. */
  uint64_t sub_generation;
  int64_t current_entry_id;
  /* Topic-local ids are handed out from here and never reused, also across a
   * daemon restart. Written by the daemon only. */
  int32_t next_pubsub_id;
  uint32_t entry_num; /* live entries; order[0, entry_num) */
  uint32_t free_num;  /* free_slots[0, free_num) */
  uint32_t sub_num;   /* sub_ids[0, sub_num) */
  /* Statistics for the rollback evaluation. */
  uint64_t rollback_count;      /* holders whose death was rolled back */
  uint64_t rollback_record_num; /* undo records replayed in total */
  uint32_t undo_num;
  uint32_t undo_overflow;              /* the running operation outgrew the log */
  uint16_t order[kTopicShmEntrySlots]; /* slot indices by ascending entry_id */
  uint16_t free_slots[kTopicShmEntrySlots];
  uint16_t sub_ids[kTopicShmIdNum]; /* registered subscribers, so publish need not scan ids[] */
  TopicShmId ids[kTopicShmIdNum];
  TopicShmEntry entries[kTopicShmEntrySlots];
  TopicShmUndoRecord undo[kTopicShmUndoCap];
};

static_assert(sizeof(TopicShmId) == 32, "TopicShmId must be 32 bytes");
static_assert(sizeof(TopicShmUndoRecord) == 16, "TopicShmUndoRecord must be 16 bytes");
static_assert(kTopicShmEntrySlots <= 65536, "slot indices are 16 bit");
static_assert(sizeof(AgnocastTopicShm) < UINT32_MAX, "undo offsets are 32 bit");

inline bool topic_shm_valid_id(int32_t id)
{
  return id >= 0 && id < static_cast<int32_t>(kTopicShmIdNum);
}

// ============================================================
// Undo log (caller holds the mutex)
// ============================================================

// Records the current value of a field before it is changed.
inline void topic_shm_log_bytes(AgnocastTopicShm * shm, const void * field, uint32_t size)
{
  const uint32_t n = shm->undo_num;
  if (n >= kTopicShmUndoCap) {
    // Bounded by kTopicShmUndoCap, so this does not happen; if it ever did, the
    // rollback would be partial but the rebuild still leaves a usable segment.
    shm->undo_overflow = 1;
    return;
  }
  TopicShmUndoRecord & rec = shm->undo[n];
  rec.offset = static_cast<uint32_t>(
    reinterpret_cast<const char *>(field) - reinterpret_cast<const char *>(shm));
  rec.size = size;
  rec.old_value = 0;
  std::memcpy(&rec.old_value, field, size);
  // The record must be complete before it counts, and must count before the
  // field changes: the holder can be killed between any two instructions.
  __atomic_store_n(&shm->undo_num, n + 1, __ATOMIC_RELEASE);
  __atomic_thread_fence(__ATOMIC_RELEASE);
}

// Logged store of a canonical field.
template <typename T>
inline void topic_shm_set(AgnocastTopicShm * shm, T & field, T value)
{
  static_assert(std::is_trivially_copyable<T>::value && sizeof(T) <= 8, "word-sized fields only");
  if (field == value) return;
  topic_shm_log_bytes(shm, &field, sizeof(T));
  field = value;
}

// Logs a whole id record, after which it may be changed freely.
inline void topic_shm_log_id(AgnocastTopicShm * shm, int32_t id)
{
  const char * base = reinterpret_cast<const char *>(&shm->ids[id]);
  for (size_t off = 0; off < sizeof(TopicShmId); off += sizeof(uint64_t)) {
    topic_shm_log_bytes(shm, base + off, sizeof(uint64_t));
  }
}

// Makes the running operation permanent.
inline void topic_shm_commit(AgnocastTopicShm * shm)
{
  shm->undo_overflow = 0;
  __atomic_store_n(&shm->undo_num, 0, __ATOMIC_RELEASE);
}

// Recomputes every derived field from the canonical ones. Also drops anything
// no valid state can contain: references held by ids that are not subscribers,
// and entries whose publisher id is not a publisher.
inline void topic_shm_rebuild(AgnocastTopicShm * shm)
{
  uint64_t sub_mask[kTopicShmBitmapWords] = {};
  int32_t max_id = -1;
  shm->sub_num = 0;
  for (uint32_t id = 0; id < kTopicShmIdNum; id++) {
    TopicShmId & rec = shm->ids[id];
    if (rec.kind > TOPIC_SHM_ID_SUBSCRIBER) std::memset(&rec, 0, sizeof(rec));
    if (rec.kind == TOPIC_SHM_ID_FREE) continue;
    max_id = static_cast<int32_t>(id);
    if (rec.kind == TOPIC_SHM_ID_PUBLISHER) {
      rec.entries_num = 0;
    } else {
      shm->sub_ids[shm->sub_num++] = static_cast<uint16_t>(id);
      sub_mask[id / 64] |= uint64_t{1} << (id % 64);
    }
  }
  if (shm->next_pubsub_id <= max_id) shm->next_pubsub_id = max_id + 1;

  shm->entry_num = 0;
  for (uint32_t slot = 0; slot < kTopicShmEntrySlots; slot++) {
    TopicShmEntry & en = shm->entries[slot];
    if (
      en.live == 0 || !topic_shm_valid_id(en.publisher_id) ||
      shm->ids[en.publisher_id].kind != TOPIC_SHM_ID_PUBLISHER) {
      en.live = 0;
      continue;
    }
    uint32_t ref_num = 0;
    for (uint32_t w = 0; w < kTopicShmBitmapWords; w++) {
      en.refs[w] &= sub_mask[w];
      ref_num += static_cast<uint32_t>(__builtin_popcountll(en.refs[w]));
    }
    en.ref_num = ref_num;
    shm->ids[en.publisher_id].entries_num++;
    if (en.entry_id >= shm->current_entry_id) shm->current_entry_id = en.entry_id + 1;
    shm->order[shm->entry_num++] = static_cast<uint16_t>(slot);
  }
  std::sort(shm->order, shm->order + shm->entry_num, [shm](uint16_t a, uint16_t b) {
    return shm->entries[a].entry_id < shm->entries[b].entry_id;
  });

  shm->free_num = 0;
  for (uint32_t slot = kTopicShmEntrySlots; slot-- > 0;) {
    if (shm->entries[slot].live == 0) {
      shm->free_slots[shm->free_num++] = static_cast<uint16_t>(slot);
    }
  }
  // Publishers refetch the subscribers' eventfds after any recovery.
  shm->sub_generation++;
}

// Rolls back the operation a dead holder left half done.
inline void topic_shm_recover(AgnocastTopicShm * shm)
{
  constexpr size_t kFirstLogged = offsetof(AgnocastTopicShm, sub_generation);
  constexpr size_t kLoggedEnd = offsetof(AgnocastTopicShm, undo);
  const uint32_t n = std::min(shm->undo_num, kTopicShmUndoCap);
  for (uint32_t i = n; i-- > 0;) {
    const TopicShmUndoRecord & rec = shm->undo[i];
    // A record outside the logged fields can only come from a corrupted log.
    if (rec.size == 0 || rec.size > 8 || rec.offset < kFirstLogged) continue;
    if (rec.offset > kLoggedEnd - rec.size) continue;
    std::memcpy(reinterpret_cast<char *>(shm) + rec.offset, &rec.old_value, rec.size);
  }
  topic_shm_rebuild(shm);
  shm->rollback_record_num += n;
  shm->rollback_count++;
  topic_shm_commit(shm);
}

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
    topic_shm_recover(shm);
    pthread_mutex_consistent(&shm->mutex);
  }
}

inline void topic_shm_unlock(AgnocastTopicShm * shm)
{
  topic_shm_commit(shm);
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
inline bool topic_shm_set_ref(AgnocastTopicShm * shm, TopicShmEntry & en, int32_t id)
{
  const uint64_t bit = uint64_t{1} << (id % 64);
  uint64_t & word = en.refs[id / 64];
  if (word & bit) return false;
  topic_shm_set(shm, word, word | bit);
  en.ref_num++;
  return true;
}

// false when the bit was not set.
inline bool topic_shm_clear_ref(AgnocastTopicShm * shm, TopicShmEntry & en, int32_t id)
{
  const uint64_t bit = uint64_t{1} << (id % 64);
  uint64_t & word = en.refs[id / 64];
  if ((word & bit) == 0) return false;
  topic_shm_set(shm, word, word & ~bit);
  en.ref_num--;
  return true;
}

// Removes the entry at order[pos] and returns its slot to the free list. The
// owning publisher's entries_num is the caller's to adjust.
inline void topic_shm_remove_at(AgnocastTopicShm * shm, uint32_t pos)
{
  const uint16_t slot = shm->order[pos];
  topic_shm_set(shm, shm->entries[slot].live, 0u);
  std::memmove(
    &shm->order[pos], &shm->order[pos + 1], (shm->entry_num - pos - 1) * sizeof(shm->order[0]));
  shm->entry_num--;
  shm->free_slots[shm->free_num++] = slot;
}

inline int topic_shm_insert(
  AgnocastTopicShm * shm, int32_t publisher_id, uint64_t msg_virtual_address, int64_t * entry_id)
{
  if (shm->free_num == 0) return -ENOSPC;
  const uint16_t slot = shm->free_slots[shm->free_num - 1];
  TopicShmEntry & en = shm->entries[slot];
  // The slot is free, so its contents need no undo record: rolling back clears
  // live, and a free slot's contents mean nothing.
  en.entry_id = shm->current_entry_id;
  en.msg_virtual_address = msg_virtual_address;
  en.publisher_id = publisher_id;
  en.ref_num = 0;
  std::memset(en.refs, 0, sizeof(en.refs));
  topic_shm_set(shm, en.live, 1u);
  topic_shm_set(shm, shm->current_entry_id, shm->current_entry_id + 1);
  shm->free_num--;
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

struct TopicShmAlwaysMapped
{
  bool operator()(int32_t /*publisher_pid*/) const { return true; }
};

// Mirrors receive_msg_core. is_mapped(pid) says whether the caller can read
// that publisher's memory; the scan stops at the first entry it cannot, without
// taking it, so the entry is delivered once the memory has been mapped.
template <typename IsMapped>
inline int topic_shm_receive(
  AgnocastTopicShm * shm, int32_t subscriber_id, const IsMapped & is_mapped, int64_t * entry_ids,
  uint64_t * entry_addrs, uint16_t * entry_num, bool * call_again)
{
  *entry_num = 0;
  *call_again = false;
  if (!topic_shm_valid_id(subscriber_id)) return -EINVAL;
  TopicShmId & sub = shm->ids[subscriber_id];
  if (sub.kind != TOPIC_SHM_ID_SUBSCRIBER) return -EINVAL;
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
    if (!is_mapped(pub.pid)) break;
    if (!topic_shm_set_ref(shm, en, subscriber_id)) return -EALREADY;

    entry_ids[*entry_num] = en.entry_id;
    entry_addrs[*entry_num] = en.msg_virtual_address;
    (*entry_num)++;
  }
  if (*entry_num > 0) topic_shm_set(shm, sub.latest_received_entry_id, entry_ids[*entry_num - 1]);
  return 0;
}

inline int topic_shm_release_ref(AgnocastTopicShm * shm, int32_t subscriber_id, int64_t entry_id)
{
  if (!topic_shm_valid_id(subscriber_id)) return -EINVAL;
  TopicShmEntry * en = topic_shm_find_entry(shm, entry_id);
  if (en == nullptr) return -EINVAL;
  if (!topic_shm_clear_ref(shm, *en, subscriber_id)) return -EINVAL;
  return 0;
}

// Mirrors the take handler's search: within the subscriber's depth, the oldest
// eligible entry newer than what it last took. Entries of a publisher the
// caller cannot read yet are passed over.
template <typename IsMapped>
inline int topic_shm_take(
  AgnocastTopicShm * shm, int32_t subscriber_id, bool allow_same_message,
  const IsMapped & is_mapped, uint64_t * addr, int64_t * entry_id)
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
    if (!is_mapped(pub.pid)) continue;

    candidate = &en;
    searched_count++;
  }

  if (candidate != nullptr) {
    const bool already_referenced =
      allow_same_message && topic_shm_test_ref(*candidate, subscriber_id);
    if (!already_referenced && !topic_shm_set_ref(shm, *candidate, subscriber_id)) {
      return -EALREADY;
    }
    *addr = candidate->msg_virtual_address;
    *entry_id = candidate->entry_id;
    topic_shm_set(shm, sub.latest_received_entry_id, candidate->entry_id);
  }
  return 0;
}

// True, and the flag cleared, when a publisher appeared since the subscriber
// last asked the daemon where the publishers' memory is. The caller asks the
// daemon afterwards, so a publisher added in between sets the flag again and
// is picked up on the next call.
inline bool topic_shm_consume_mmap_update(AgnocastTopicShm * shm, int32_t subscriber_id)
{
  if (!topic_shm_valid_id(subscriber_id)) return false;
  TopicShmId & sub = shm->ids[subscriber_id];
  if (sub.kind != TOPIC_SHM_ID_SUBSCRIBER || sub.need_mmap_update == 0) return false;
  topic_shm_set(shm, sub.need_mmap_update, uint8_t{0});
  return true;
}

// Setting the flag needs no undo record: a spurious one costs one extra query.
inline void topic_shm_request_mmap_update(AgnocastTopicShm * shm, int32_t subscriber_id)
{
  if (!topic_shm_valid_id(subscriber_id)) return;
  TopicShmId & sub = shm->ids[subscriber_id];
  if (sub.kind == TOPIC_SHM_ID_SUBSCRIBER) sub.need_mmap_update = 1;
}

inline void topic_shm_request_mmap_update_all(AgnocastTopicShm * shm)
{
  for (uint32_t i = 0; i < shm->sub_num; i++) {
    shm->ids[shm->sub_ids[i]].need_mmap_update = 1;
  }
}

// ============================================================
// Registration (the daemon's side)
// ============================================================

inline int topic_shm_alloc_id(AgnocastTopicShm * shm, int32_t * id)
{
  if (shm->next_pubsub_id < 0 || shm->next_pubsub_id >= static_cast<int32_t>(kTopicShmIdNum)) {
    return -ENOSPC;
  }
  *id = shm->next_pubsub_id;
  topic_shm_set(shm, shm->next_pubsub_id, shm->next_pubsub_id + 1);
  return 0;
}

inline int topic_shm_add_publisher(
  AgnocastTopicShm * shm, int32_t pid, uint32_t qos_depth, bool transient_local, int32_t * id)
{
  const int ret = topic_shm_alloc_id(shm, id);
  if (ret < 0) return ret;
  topic_shm_log_id(shm, *id);
  TopicShmId & pub = shm->ids[*id];
  std::memset(&pub, 0, sizeof(pub));
  pub.kind = TOPIC_SHM_ID_PUBLISHER;
  pub.pid = pid;
  pub.qos_depth = qos_depth;
  pub.transient_local = transient_local;
  TOPIC_SHM_FAULT_POINT("shm.add_publisher");
  // Its memory is new to every existing subscriber.
  topic_shm_request_mmap_update_all(shm);
  return 0;
}

inline int topic_shm_add_subscriber(
  AgnocastTopicShm * shm, int32_t pid, uint32_t qos_depth, bool transient_local, bool is_take_sub,
  bool ignore_local, int32_t * id)
{
  const int ret = topic_shm_alloc_id(shm, id);
  if (ret < 0) return ret;
  topic_shm_log_id(shm, *id);
  TopicShmId & sub = shm->ids[*id];
  std::memset(&sub, 0, sizeof(sub));
  sub.kind = TOPIC_SHM_ID_SUBSCRIBER;
  sub.pid = pid;
  sub.qos_depth = qos_depth;
  sub.transient_local = transient_local;
  sub.is_take_sub = is_take_sub;
  sub.ignore_local = ignore_local;
  sub.need_mmap_update = 1;
  TOPIC_SHM_FAULT_POINT("shm.add_subscriber");
  if (transient_local) {
    sub.latest_received_entry_id = -1;
  } else {
    sub.latest_received_entry_id = shm->current_entry_id;
    topic_shm_set(shm, shm->current_entry_id, shm->current_entry_id + 1);
  }
  shm->sub_ids[shm->sub_num++] = static_cast<uint16_t>(*id);
  shm->sub_generation++;
  return 0;
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
  topic_shm_log_id(shm, id);
  std::memset(&shm->ids[id], 0, sizeof(shm->ids[id]));
}

// Mirrors remove_subscriber_info + cleanup_unreferenced_entries_from_exited_
// publishers as one operation: drops the subscriber and its references, then
// any entry no one references whose publisher's process is gone. Publisher ids
// left with no entries are cleared and written to emptied_publishers so the
// caller can drop its own records of them.
inline uint32_t topic_shm_remove_subscriber(
  AgnocastTopicShm * shm, int32_t subscriber_id, int32_t * emptied_publishers)
{
  uint32_t emptied_num = 0;
  topic_shm_clear_id(shm, subscriber_id);
  TOPIC_SHM_FAULT_POINT("shm.remove_subscriber");
  for (uint32_t pos = 0; pos < shm->entry_num;) {
    TopicShmEntry & en = topic_shm_entry_at(shm, pos);
    if (topic_shm_clear_ref(shm, en, subscriber_id)) {
      TOPIC_SHM_FAULT_POINT("shm.remove_subscriber_refs");
    }
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

// Drops the publisher's unreferenced entries, marking it exited first when its
// process is gone, and clears the id once no entry is left. Returns how many
// entries it still has.
inline uint32_t topic_shm_remove_publisher(
  AgnocastTopicShm * shm, int32_t publisher_id, bool exited)
{
  TopicShmId & pub = shm->ids[publisher_id];
  if (exited) topic_shm_set(shm, pub.exited, uint8_t{1});
  for (uint32_t pos = 0; pos < shm->entry_num;) {
    TopicShmEntry & en = topic_shm_entry_at(shm, pos);
    if (en.publisher_id == publisher_id && en.ref_num == 0) {
      topic_shm_remove_at(shm, pos);
      pub.entries_num--;
    } else {
      pos++;
    }
  }
  TOPIC_SHM_FAULT_POINT("shm.remove_publisher");
  const uint32_t remaining = pub.entries_num;
  if (remaining == 0) topic_shm_clear_id(shm, publisher_id);
  return remaining;
}

// ============================================================
// Invariants
// ============================================================

// Counts the violations of the segment's invariants and describes the first
// in msg. Meant for a holder that has not changed anything yet (empty log).
inline uint32_t topic_shm_check(AgnocastTopicShm * shm, char * msg, size_t msg_size)
{
  uint32_t violations = 0;
  auto fail = [&](const char * fmt, long a, long b) {
    if (violations++ == 0 && msg != nullptr && msg_size > 0) {
      std::snprintf(msg, msg_size, fmt, a, b);
    }
  };

  if (shm->undo_num != 0) fail("undo log not empty (%ld records)", shm->undo_num, 0);
  if (shm->entry_num + shm->free_num != kTopicShmEntrySlots) {
    fail("entry_num %ld + free_num %ld != slots", shm->entry_num, shm->free_num);
  }

  static thread_local uint8_t seen[kTopicShmEntrySlots];
  std::memset(seen, 0, sizeof(seen));
  uint32_t entries_of[kTopicShmIdNum] = {};
  for (uint32_t pos = 0; pos < shm->entry_num && pos < kTopicShmEntrySlots; pos++) {
    const uint16_t slot = shm->order[pos];
    const TopicShmEntry & en = shm->entries[slot];
    if (seen[slot]++) fail("slot %ld listed twice in order[] (pos %ld)", slot, pos);
    if (en.live == 0) fail("slot %ld in order[] is not live (pos %ld)", slot, pos);
    if (pos > 0 && shm->entries[shm->order[pos - 1]].entry_id >= en.entry_id) {
      fail("order[] not ascending at pos %ld (entry %ld)", pos, en.entry_id);
    }
    if (en.entry_id >= shm->current_entry_id) {
      fail("entry %ld not below current_entry_id %ld", en.entry_id, shm->current_entry_id);
    }
    if (
      !topic_shm_valid_id(en.publisher_id) ||
      shm->ids[en.publisher_id].kind != TOPIC_SHM_ID_PUBLISHER) {
      fail("entry %ld owned by non-publisher id %ld", en.entry_id, en.publisher_id);
    } else {
      entries_of[en.publisher_id]++;
    }
    uint32_t ref_num = 0;
    for (uint32_t w = 0; w < kTopicShmBitmapWords; w++) {
      uint64_t bits = en.refs[w];
      ref_num += static_cast<uint32_t>(__builtin_popcountll(bits));
      while (bits != 0) {
        const uint32_t id = w * 64 + static_cast<uint32_t>(__builtin_ctzll(bits));
        bits &= bits - 1;
        if (shm->ids[id].kind != TOPIC_SHM_ID_SUBSCRIBER) {
          fail("entry %ld referenced by non-subscriber id %ld", en.entry_id, id);
        }
      }
    }
    if (ref_num != en.ref_num) fail("entry %ld ref_num %ld wrong", en.entry_id, en.ref_num);
  }
  for (uint32_t i = 0; i < shm->free_num && i < kTopicShmEntrySlots; i++) {
    const uint16_t slot = shm->free_slots[i];
    if (seen[slot]++) fail("free slot %ld also listed elsewhere (index %ld)", slot, i);
    if (shm->entries[slot].live != 0) fail("free slot %ld is live (index %ld)", slot, i);
  }

  uint32_t sub_count = 0;
  for (uint32_t id = 0; id < kTopicShmIdNum; id++) {
    const TopicShmId & rec = shm->ids[id];
    if (rec.kind == TOPIC_SHM_ID_FREE) continue;
    if (static_cast<int32_t>(id) >= shm->next_pubsub_id) {
      fail("id %ld not below next_pubsub_id %ld", id, shm->next_pubsub_id);
    }
    if (rec.kind == TOPIC_SHM_ID_PUBLISHER && rec.entries_num != entries_of[id]) {
      fail("publisher %ld entries_num %ld wrong", id, rec.entries_num);
    }
    if (rec.kind == TOPIC_SHM_ID_SUBSCRIBER) sub_count++;
    if (rec.kind > TOPIC_SHM_ID_SUBSCRIBER) fail("id %ld has bad kind %ld", id, rec.kind);
  }
  if (sub_count != shm->sub_num) fail("sub_num %ld != subscribers %ld", shm->sub_num, sub_count);
  for (uint32_t i = 0; i < shm->sub_num && i < kTopicShmIdNum; i++) {
    if (shm->ids[shm->sub_ids[i]].kind != TOPIC_SHM_ID_SUBSCRIBER) {
      fail("sub_ids[%ld] = %ld is not a subscriber", i, shm->sub_ids[i]);
    }
  }
  return violations;
}
