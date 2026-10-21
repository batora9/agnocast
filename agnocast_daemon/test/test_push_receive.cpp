// SPDX-License-Identifier: Apache-2.0
#include "channel_test_util.hpp"

#include <gtest/gtest.h>

using agnocast_test::attach_channel;
using agnocast_test::ChannelFixture;
using agnocast_test::kChannelNode;
using agnocast_test::kChannelPubPid;
using agnocast_test::kChannelSubPid;
using agnocast_test::kChannelTopic;
using agnocast_test::TestChannel;

namespace
{

constexpr uint64_t kMsgStride = 64;

uint64_t ring_head(const TestChannel & ch)
{
  return __atomic_load_n(&ch.shm->push.head, __ATOMIC_ACQUIRE);
}

bool ring_ipc_pending(const TestChannel & ch)
{
  return __atomic_load_n(&ch.shm->push.ipc_pending, __ATOMIC_ACQUIRE) != 0;
}

// What agnocastlib's take_pushed does to consume one slot.
AgnocastPushSlot consume(TestChannel & ch)
{
  AgnocastPushRing & ring = ch.shm->push;
  const uint64_t tail = __atomic_load_n(&ring.tail, __ATOMIC_RELAXED);
  const AgnocastPushSlot slot = ring.slots[tail % AGNOCAST_PUSH_RING_SLOTS];
  __atomic_store_n(&ring.tail, tail + 1, __ATOMIC_RELEASE);
  return slot;
}

class PushReceiveTest : public ChannelFixture
{
protected:
  void SetUp() override
  {
    ChannelFixture::SetUp();
    ASSERT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), 0);
  }

  // The first request maps the publishers and clears ipc_pending.
  void catch_up()
  {
    ReceiveMsgResponse resp{};
    ASSERT_EQ(h.receive(kChannelTopic, sub_id, &resp, kChannelSubPid), 0);
    ASSERT_FALSE(ring_ipc_pending(ch));
  }

  TestChannel ch;
  uint64_t next_offset = 0;

  int64_t publish_next()
  {
    PublishMsgResponse resp{};
    EXPECT_EQ(publish(next_offset, &resp), 0);
    next_offset += kMsgStride;
    return resp.entry_id;
  }
};

}  // namespace

TEST_F(PushReceiveTest, AttachStartsBehindTheRequest)
{
  EXPECT_TRUE(ring_ipc_pending(ch));
  const int64_t first = publish_next();
  EXPECT_EQ(ring_head(ch), 0U);

  ReceiveMsgResponse resp{};
  ASSERT_EQ(h.receive(kChannelTopic, sub_id, &resp, kChannelSubPid), 0);
  ASSERT_EQ(resp.entry_num, 1U);
  EXPECT_EQ(resp.entry_ids[0], first);
  EXPECT_EQ(resp.pub_shm_num, 1U);
  EXPECT_FALSE(ring_ipc_pending(ch));
}

TEST_F(PushReceiveTest, PublishPushesTheReceiveResult)
{
  catch_up();
  const int64_t first = publish_next();
  ASSERT_EQ(ring_head(ch), 1U);
  const AgnocastPushSlot slot = consume(ch);
  EXPECT_EQ(slot.entry_id, first);
  EXPECT_EQ(slot.entry_addr, pub_addr);
  EXPECT_EQ(h.entry_rc(kChannelTopic, first, sub_id), 1);
  EXPECT_EQ(h.latest_received_entry_id(kChannelTopic, sub_id), first);

  // Already delivered, so the request has nothing more to return.
  ReceiveMsgResponse resp{};
  ASSERT_EQ(h.receive(kChannelTopic, sub_id, &resp, kChannelSubPid), 0);
  EXPECT_EQ(resp.entry_num, 0U);
}

TEST_F(PushReceiveTest, UnreadDepthSendsTheSubscriberBackToTheRequest)
{
  catch_up();
  for (uint32_t i = 0; i < kSubDepth; i++) publish_next();
  EXPECT_EQ(ring_head(ch), kSubDepth);
  EXPECT_FALSE(ring_ipc_pending(ch));

  const int64_t overflow = publish_next();
  EXPECT_EQ(ring_head(ch), kSubDepth);
  EXPECT_TRUE(ring_ipc_pending(ch));
  EXPECT_EQ(h.entry_rc(kChannelTopic, overflow, sub_id), 0);

  // Nothing is pushed while the request is pending, even with room again.
  for (uint32_t i = 0; i < kSubDepth; i++) consume(ch);
  const int64_t behind = publish_next();
  EXPECT_EQ(ring_head(ch), kSubDepth);

  // The publisher keeps only its last message, and nothing referenced the
  // overflow one, so the request returns just the newest.
  EXPECT_EQ(h.entry_rc(kChannelTopic, overflow, sub_id), -1);
  ReceiveMsgResponse resp{};
  ASSERT_EQ(h.receive(kChannelTopic, sub_id, &resp, kChannelSubPid), 0);
  ASSERT_EQ(resp.entry_num, 1U);
  EXPECT_EQ(resp.entry_ids[0], behind);
  EXPECT_FALSE(ring_ipc_pending(ch));

  publish_next();
  EXPECT_EQ(ring_head(ch), kSubDepth + 1);
}

TEST_F(PushReceiveTest, NewPublisherIsMappedThroughTheRequest)
{
  catch_up();
  constexpr pid_t kOtherPubPid = 1002;
  uint64_t other_addr = 0;
  ASSERT_EQ(h.add_process(kOtherPubPid, &other_addr), 0);
  int32_t other_pub = -1;
  ASSERT_EQ(h.add_publisher(kChannelTopic, kChannelNode, kOtherPubPid, 1, &other_pub), 0);

  publish_next();
  EXPECT_EQ(ring_head(ch), 0U);
  EXPECT_TRUE(ring_ipc_pending(ch));

  ReceiveMsgResponse resp{};
  ASSERT_EQ(h.receive(kChannelTopic, sub_id, &resp, kChannelSubPid), 0);
  EXPECT_EQ(resp.entry_num, 1U);
  EXPECT_EQ(resp.pub_shm_num, 1U);
  EXPECT_FALSE(ring_ipc_pending(ch));
}

TEST_F(PushReceiveTest, CallAgainKeepsTheRequestPending)
{
  constexpr uint32_t kDeepDepth = AGNOCAST_PROTO_MAX_RECEIVE_NUM + 5;
  int32_t deep_pub = -1;
  ASSERT_EQ(h.add_publisher(kChannelTopic, kChannelNode, kChannelPubPid, kDeepDepth, &deep_pub), 0);
  int32_t deep_sub = -1;
  ASSERT_EQ(
    h.add_subscriber(kChannelTopic, kChannelNode, kChannelSubPid, kDeepDepth, &deep_sub), 0);
  TestChannel deep;
  ASSERT_EQ(attach_channel(h, kChannelSubPid, deep_sub, deep.dup_fd()), 0);

  for (uint32_t i = 0; i < kDeepDepth; i++) {
    PublishMsgResponse pub_resp{};
    ASSERT_EQ(h.publish(kChannelTopic, deep_pub, pub_addr + i * kMsgStride, &pub_resp), 0);
  }

  ReceiveMsgResponse resp{};
  ASSERT_EQ(h.receive(kChannelTopic, deep_sub, &resp, kChannelSubPid), 0);
  EXPECT_EQ(resp.entry_num, AGNOCAST_PROTO_MAX_RECEIVE_NUM);
  EXPECT_TRUE(resp.call_again);
  EXPECT_TRUE(ring_ipc_pending(deep));

  ASSERT_EQ(h.receive(kChannelTopic, deep_sub, &resp, kChannelSubPid), 0);
  EXPECT_EQ(resp.entry_num, kDeepDepth - AGNOCAST_PROTO_MAX_RECEIVE_NUM);
  EXPECT_FALSE(resp.call_again);
  EXPECT_FALSE(ring_ipc_pending(deep));
}

TEST_F(PushReceiveTest, ImplausibleTailSendsTheSubscriberBackToTheRequest)
{
  catch_up();
  __atomic_store_n(&ch.shm->push.tail, 12345, __ATOMIC_RELEASE);
  const int64_t entry = publish_next();
  EXPECT_EQ(ring_head(ch), 0U);
  EXPECT_TRUE(ring_ipc_pending(ch));
  EXPECT_EQ(h.entry_rc(kChannelTopic, entry, sub_id), 0);
}

TEST_F(PushReceiveTest, SubscriberExitDropsPushedReferences)
{
  catch_up();
  const int64_t entry = publish_next();
  ASSERT_EQ(h.entry_rc(kChannelTopic, entry, sub_id), 1);
  h.disconnect(kChannelSubPid);
  EXPECT_EQ(h.entry_rc(kChannelTopic, entry, sub_id), 0);
}
