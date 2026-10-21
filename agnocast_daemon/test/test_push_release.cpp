// SPDX-License-Identifier: Apache-2.0
#include "channel_test_util.hpp"

#include <gtest/gtest.h>

using agnocast_test::attach_channel;
using agnocast_test::ChannelFixture;
using agnocast_test::kChannelSubPid;
using agnocast_test::kChannelTopic;
using agnocast_test::TestChannel;

using PushReleaseTest = ChannelFixture;

// A message that arrives through the push ring and leaves through the release
// ring costs the subscriber no request at all.
TEST_F(PushReleaseTest, PushedMessageReleasedThroughTheRingIsFreedOnNextPublish)
{
  TestChannel ch;
  ASSERT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), 0);
  ReceiveMsgResponse recv_resp{};
  ASSERT_EQ(h.receive(kChannelTopic, sub_id, &recv_resp, kChannelSubPid), 0);

  PublishMsgResponse first{};
  ASSERT_EQ(publish(0, &first), 0);
  AgnocastPushRing & push = ch.shm->push;
  ASSERT_EQ(__atomic_load_n(&push.head, __ATOMIC_ACQUIRE), 1U);
  ASSERT_EQ(push.slots[0].entry_id, first.entry_id);
  __atomic_store_n(&push.tail, 1, __ATOMIC_RELEASE);

  AgnocastReleaseRing & release = ch.shm->release;
  release.entry_ids[0] = first.entry_id;
  __atomic_store_n(&release.head, 1, __ATOMIC_RELEASE);

  PublishMsgResponse second{};
  ASSERT_EQ(publish(64, &second), 0);
  ASSERT_EQ(second.released_num, 1U);
  EXPECT_EQ(second.released_addrs[0], pub_addr);
  EXPECT_EQ(__atomic_load_n(&push.head, __ATOMIC_ACQUIRE), 2U);
  EXPECT_EQ(push.slots[1].entry_id, second.entry_id);
}
