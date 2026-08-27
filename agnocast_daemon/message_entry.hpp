// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "protocol.h"

#include <array>
#include <atomic>
#include <cstdint>

// Equivalent of agnocast_kmod::DECLARE_BITMAP(referencing_subscribers, MAX_TOPIC_LOCAL_ID)
// with test_and_set_bit / test_and_clear_bit.  std::bitset is not atomic; concurrent
// receive/take/release on the same entry require word-sized atomic RMW.
struct AtomicSubscriberBitmap
{
  static constexpr size_t kBits = AGNOCAST_PROTO_MAX_TOPIC_LOCAL_ID;
  static constexpr size_t kWordBits = 64;
  static constexpr size_t kWords = kBits / kWordBits;
  static_assert(kBits % kWordBits == 0, "bitmap size must be a multiple of 64");

  std::array<std::atomic<uint64_t>, kWords> words{};

  // Returns true if the bit was already set.
  bool test_and_set(size_t bit) noexcept
  {
    const size_t word = bit / kWordBits;
    const uint64_t mask = uint64_t{1} << (bit % kWordBits);
    const uint64_t prev = words[word].fetch_or(mask, std::memory_order_acq_rel);
    return (prev & mask) != 0;
  }

  // Returns true if the bit was previously set.
  bool test_and_clear(size_t bit) noexcept
  {
    const size_t word = bit / kWordBits;
    const uint64_t mask = uint64_t{1} << (bit % kWordBits);
    const uint64_t prev = words[word].fetch_and(~mask, std::memory_order_acq_rel);
    return (prev & mask) != 0;
  }

  bool test(size_t bit) const noexcept
  {
    const size_t word = bit / kWordBits;
    const uint64_t mask = uint64_t{1} << (bit % kWordBits);
    return (words[word].load(std::memory_order_acquire) & mask) != 0;
  }

  bool any() const noexcept
  {
    for (const auto & word : words) {
      if (word.load(std::memory_order_acquire) != 0) {
        return true;
      }
    }
    return false;
  }
};

// Equivalent of agnocast_kmod::entry_node.
// The red-black tree (struct rb_root) is replaced by std::map<int64_t, EntryNode>
// in TopicStruct, keyed by entry_id.
//
// Not copyable or movable: AtomicSubscriberBitmap contains std::atomic.  Insert
// into TopicStruct::entries with try_emplace so the node is constructed in place.
struct EntryNode
{
  int64_t entry_id = 0;
  int32_t publisher_id = 0;  // topic_local_id_t
  uint64_t msg_virtual_address = 0;

  AtomicSubscriberBitmap referencing_subscribers;

  bool is_referenced() const noexcept { return referencing_subscribers.any(); }
};
