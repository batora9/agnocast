// SPDX-License-Identifier: Apache-2.0
#include "channel_test_util.hpp"

#include <gtest/gtest.h>

using agnocast_test::attach_channel;
using agnocast_test::ChannelFixture;
using agnocast_test::DaemonHarness;
using agnocast_test::kChannelPubPid;
using agnocast_test::kChannelSubPid;
using agnocast_test::kChannelTopic;
using agnocast_test::TestChannel;

namespace
{

class ReleaseRingTest : public ChannelFixture
{
protected:
  void SetUp() override
  {
    ChannelFixture::SetUp();
    ASSERT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), 0);
  }

  // What agnocastlib's queue_release does.
  void queue_release(int64_t entry_id)
  {
    AgnocastReleaseRing & ring = ch.shm->release;
    const uint64_t head = __atomic_load_n(&ring.head, __ATOMIC_RELAXED);
    ring.entry_ids[head % AGNOCAST_RELEASE_RING_SLOTS] = entry_id;
    __atomic_store_n(&ring.head, head + 1, __ATOMIC_RELEASE);
  }

  uint64_t shared_tail() const { return __atomic_load_n(&ch.shm->release.tail, __ATOMIC_ACQUIRE); }

  int64_t publish_and_receive()
  {
    PublishMsgResponse pub_resp{};
    EXPECT_EQ(publish(0, &pub_resp), 0);
    ReceiveMsgResponse recv_resp{};
    EXPECT_EQ(h.receive(kChannelTopic, sub_id, &recv_resp, kChannelSubPid), 0);
    EXPECT_EQ(recv_resp.entry_num, 1U);
    return pub_resp.entry_id;
  }

  TestChannel ch;
};

}  // namespace

TEST_F(ReleaseRingTest, QueuedReleaseIsAppliedOnNextPublish)
{
  const int64_t first = publish_and_receive();
  queue_release(first);
  // Nothing consumes the ring until a path that reads the reference bits runs.
  EXPECT_EQ(h.entry_rc(kChannelTopic, first, sub_id), 1);

  PublishMsgResponse pub_resp{};
  ASSERT_EQ(publish(64, &pub_resp), 0);
  ASSERT_EQ(pub_resp.released_num, 1U);
  EXPECT_EQ(pub_resp.released_addrs[0], pub_addr);
  EXPECT_EQ(h.entry_rc(kChannelTopic, first, sub_id), -1);
  EXPECT_EQ(shared_tail(), 1U);
}

TEST_F(ReleaseRingTest, UnreleasedEntryIsKept)
{
  const int64_t first = publish_and_receive();

  PublishMsgResponse pub_resp{};
  ASSERT_EQ(publish(64, &pub_resp), 0);
  EXPECT_EQ(pub_resp.released_num, 0U);
  EXPECT_EQ(h.entry_rc(kChannelTopic, first, sub_id), 1);
}

TEST_F(ReleaseRingTest, TakeAppliesQueuedReleaseBeforeReferencingAgain)
{
  PublishMsgResponse pub_resp{};
  ASSERT_EQ(publish(0, &pub_resp), 0);
  TakeMsgResponse take_resp{};
  ASSERT_EQ(h.take(kChannelTopic, sub_id, true, &take_resp, kChannelSubPid), 0);
  ASSERT_EQ(take_resp.entry_id, pub_resp.entry_id);

  queue_release(pub_resp.entry_id);
  ASSERT_EQ(h.take(kChannelTopic, sub_id, true, &take_resp, kChannelSubPid), 0);
  ASSERT_EQ(take_resp.entry_id, pub_resp.entry_id);
  EXPECT_EQ(shared_tail(), 1U);

  // The second take holds a reference again, so the next publish keeps it.
  PublishMsgResponse next{};
  ASSERT_EQ(publish(64, &next), 0);
  EXPECT_EQ(next.released_num, 0U);
  EXPECT_EQ(h.entry_rc(kChannelTopic, pub_resp.entry_id, sub_id), 1);
}

TEST_F(ReleaseRingTest, ImplausibleHeadIsSkipped)
{
  const int64_t first = publish_and_receive();
  __atomic_store_n(&ch.shm->release.head, 100000, __ATOMIC_RELEASE);

  PublishMsgResponse pub_resp{};
  ASSERT_EQ(publish(64, &pub_resp), 0);
  EXPECT_EQ(pub_resp.released_num, 0U);
  EXPECT_EQ(h.entry_rc(kChannelTopic, first, sub_id), 1);
  EXPECT_EQ(shared_tail(), 100000U);
}

TEST_F(ReleaseRingTest, RemovePublisherAppliesQueuedReleases)
{
  const int64_t first = publish_and_receive();
  queue_release(first);

  RemovePublisherRequest req{};
  DaemonHarness::set_topic(req.topic_name, kChannelTopic);
  req.publisher_id = pub_id;
  ASSERT_EQ(h.call(AGNOCAST_CMD_REMOVE_PUBLISHER, kChannelPubPid, &req, sizeof(req)), 0);
  EXPECT_EQ(h.topic_entries_num(kChannelTopic), 0);
  EXPECT_FALSE(h.publisher_exists(kChannelTopic, pub_id));
}

TEST_F(ReleaseRingTest, PublisherExitAppliesQueuedReleases)
{
  const int64_t first = publish_and_receive();
  queue_release(first);

  h.disconnect(kChannelPubPid);
  EXPECT_EQ(h.topic_entries_num(kChannelTopic), 0);
}
