// SPDX-License-Identifier: Apache-2.0
#include "channel_test_util.hpp"

#include <gtest/gtest.h>

#include <cerrno>

using agnocast_test::attach_channel;
using agnocast_test::ChannelFixture;
using agnocast_test::DaemonHarness;
using agnocast_test::kChannelPubPid;
using agnocast_test::kChannelSubPid;
using agnocast_test::kChannelTopic;
using agnocast_test::TestChannel;

using SubscriberChannelTest = ChannelFixture;

TEST_F(SubscriberChannelTest, AttachSealedChannel)
{
  TestChannel ch;
  EXPECT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), 0);
  EXPECT_TRUE(has_channel());
}

TEST_F(SubscriberChannelTest, AttachTwiceFails)
{
  TestChannel ch;
  ASSERT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), 0);
  TestChannel second;
  EXPECT_EQ(attach_channel(h, kChannelSubPid, sub_id, second.dup_fd()), -EEXIST);
}

TEST_F(SubscriberChannelTest, AttachWithoutDescriptorFails)
{
  EXPECT_EQ(attach_channel(h, kChannelSubPid, sub_id, -1), -EINVAL);
  EXPECT_FALSE(has_channel());
}

TEST_F(SubscriberChannelTest, AttachUnsealedChannelFails)
{
  TestChannel ch(/*seal=*/false);
  EXPECT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), -EPERM);
  EXPECT_FALSE(has_channel());
}

TEST_F(SubscriberChannelTest, AttachWrongSizeFails)
{
  TestChannel ch(true, sizeof(AgnocastSubscriberChannel) + 4096);
  EXPECT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), -EINVAL);
  EXPECT_FALSE(has_channel());
}

TEST_F(SubscriberChannelTest, AttachWrongMagicFails)
{
  TestChannel ch(true, sizeof(AgnocastSubscriberChannel), 0);
  EXPECT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), -EINVAL);
  EXPECT_FALSE(has_channel());
}

TEST_F(SubscriberChannelTest, AttachFromOtherProcessFails)
{
  TestChannel ch;
  EXPECT_EQ(attach_channel(h, kChannelPubPid, sub_id, ch.dup_fd()), -EPERM);
  EXPECT_FALSE(has_channel());
}

TEST_F(SubscriberChannelTest, RemoveSubscriberDropsChannel)
{
  TestChannel ch;
  ASSERT_EQ(attach_channel(h, kChannelSubPid, sub_id, ch.dup_fd()), 0);
  RemoveSubscriberRequest req{};
  DaemonHarness::set_topic(req.topic_name, kChannelTopic);
  req.subscriber_id = sub_id;
  EXPECT_EQ(h.call(AGNOCAST_CMD_REMOVE_SUBSCRIBER, kChannelSubPid, &req, sizeof(req)), 0);
  EXPECT_FALSE(h.subscriber_exists(kChannelTopic, sub_id));
}
