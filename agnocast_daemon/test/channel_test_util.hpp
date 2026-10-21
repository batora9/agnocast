// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "daemon_test_util.hpp"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace agnocast_test
{

inline constexpr const char * kChannelTopic = "/channel_test_topic";
inline constexpr const char * kChannelNode = "/channel_test_node";
inline constexpr pid_t kChannelPubPid = 1000;
inline constexpr pid_t kChannelSubPid = 1001;

// A memfd laid out the way agnocastlib prepares one, mapped here as well so a
// test can play the subscriber's side of the channel.
struct TestChannel
{
  int fd = -1;
  AgnocastSubscriberChannel * shm = nullptr;

  explicit TestChannel(
    bool seal = true, size_t size = sizeof(AgnocastSubscriberChannel),
    uint32_t magic = AGNOCAST_SUB_CHANNEL_MAGIC)
  : mapped_size_(size)
  {
    fd = memfd_create("test_sub_channel", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0 || ftruncate(fd, static_cast<off_t>(size)) != 0) {
      throw std::runtime_error("memfd setup failed");
    }
    void * p = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) throw std::runtime_error("mmap failed");
    shm = static_cast<AgnocastSubscriberChannel *>(p);
    shm->magic = magic;
    if (seal && fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL) != 0) {
      throw std::runtime_error("sealing failed");
    }
  }
  ~TestChannel()
  {
    if (shm != nullptr) munmap(shm, mapped_size_);
    if (fd >= 0) close(fd);
  }
  TestChannel(const TestChannel &) = delete;
  TestChannel & operator=(const TestChannel &) = delete;

  // dispatch takes ownership of the descriptor it is given.
  int dup_fd() const { return dup(fd); }

private:
  size_t mapped_size_ = 0;
};

inline int attach_channel(DaemonHarness & h, pid_t pid, int32_t sub_id, int channel_fd)
{
  AttachSubscriberChannelRequest req{};
  DaemonHarness::set_topic(req.topic_name, kChannelTopic);
  req.subscriber_id = sub_id;
  return h.call(
    AGNOCAST_CMD_ATTACH_SUBSCRIBER_CHANNEL, pid, &req, sizeof(req), nullptr, 0, nullptr,
    channel_fd);
}

// One publisher process (keep-last 1) and one subscriber process (keep-last
// 10) on kChannelTopic, with the subscriber's channel not yet attached.
class ChannelFixture : public ::testing::Test
{
protected:
  static constexpr uint32_t kPubDepth = 1;
  static constexpr uint32_t kSubDepth = 10;

  void SetUp() override
  {
    ASSERT_EQ(h.add_process(kChannelPubPid, &pub_addr), 0);
    ASSERT_EQ(h.add_process(kChannelSubPid), 0);
    ASSERT_EQ(h.add_publisher(kChannelTopic, kChannelNode, kChannelPubPid, kPubDepth, &pub_id), 0);
    ASSERT_EQ(h.add_subscriber(kChannelTopic, kChannelNode, kChannelSubPid, kSubDepth, &sub_id), 0);
  }

  SubscriberInfo * sub_info()
  {
    auto it = h.store_.topic_map_.find(DaemonHarness::topic_key(kChannelTopic));
    if (it == h.store_.topic_map_.end()) return nullptr;
    auto sit = it->second->topic.sub_info_map.find(sub_id);
    return (sit != it->second->topic.sub_info_map.end()) ? &sit->second : nullptr;
  }

  bool has_channel()
  {
    SubscriberInfo * info = sub_info();
    return info != nullptr && info->channel != nullptr;
  }

  int publish(uint64_t offset, PublishMsgResponse * out)
  {
    return h.publish(kChannelTopic, pub_id, pub_addr + offset, out);
  }

  DaemonHarness h;
  uint64_t pub_addr = 0;
  int32_t pub_id = -1;
  int32_t sub_id = -1;
};

}  // namespace agnocast_test
