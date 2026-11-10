// SPDX-License-Identifier: Apache-2.0
#include "daemon_test_util.hpp"

#include <gtest/gtest.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <cstring>
#include <thread>

using agnocast_test::DaemonHarness;

namespace
{
constexpr const char * kTopic = "/topic_shm_test";
constexpr const char * kNode = "/topic_shm_test_node";
constexpr pid_t kPubPid = 1000;
constexpr pid_t kSubPid = 2000;

// What a client does with its segment, under the segment mutex.
int local_publish(
  AgnocastTopicShm * shm, int32_t pub_id, uint64_t addr, int64_t * entry_id,
  uint32_t * released_num = nullptr, uint32_t * subscriber_num = nullptr)
{
  uint64_t released[AGNOCAST_PROTO_MAX_RELEASE_NUM];
  uint32_t released_n = 0;
  static int32_t sub_ids[kTopicShmIdNum];
  uint32_t sub_n = 0;
  TopicShmLock lock(shm);
  const int ret =
    topic_shm_publish(shm, pub_id, addr, entry_id, released, &released_n, sub_ids, &sub_n);
  if (released_num) *released_num = released_n;
  if (subscriber_num) *subscriber_num = sub_n;
  return ret;
}

int local_receive(AgnocastTopicShm * shm, int32_t sub_id, int64_t * ids, uint16_t * num)
{
  uint64_t addrs[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  bool call_again = false;
  TopicShmLock lock(shm);
  return topic_shm_receive(shm, sub_id, TopicShmAlwaysMapped{}, ids, addrs, num, &call_again);
}

int local_release(AgnocastTopicShm * shm, int32_t sub_id, int64_t entry_id)
{
  TopicShmLock lock(shm);
  return topic_shm_release_ref(shm, sub_id, entry_id);
}
}  // namespace

TEST(TopicShm, LocalPublishIsVisibleToDaemonReceive)
{
  DaemonHarness h;
  uint64_t pub_addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &pub_addr), 0);
  ASSERT_EQ(h.add_process(kSubPid), 0);
  int32_t pub_id = -1;
  int32_t sub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 10, &pub_id), 0);
  ASSERT_EQ(h.add_subscriber(kTopic, kNode, kSubPid, 10, &sub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  ASSERT_NE(shm, nullptr);

  int64_t entry_id = -1;
  uint32_t subscriber_num = 0;
  ASSERT_EQ(local_publish(shm, pub_id, pub_addr, &entry_id, nullptr, &subscriber_num), 0);
  EXPECT_EQ(subscriber_num, 1U);
  EXPECT_EQ(h.topic_entries_num(kTopic), 1);

  ReceiveMsgResponse resp{};
  ASSERT_EQ(h.receive(kTopic, sub_id, &resp), 0);
  ASSERT_EQ(resp.entry_num, 1);
  EXPECT_EQ(resp.entry_ids[0], entry_id);
  EXPECT_EQ(resp.entry_addrs[0], pub_addr);
  EXPECT_EQ(resp.pub_shm_num, 1U);
  EXPECT_EQ(h.entry_rc(kTopic, entry_id, sub_id), 1);

  ASSERT_EQ(local_release(shm, sub_id, entry_id), 0);
  EXPECT_EQ(h.entry_rc(kTopic, entry_id, sub_id), 0);
  EXPECT_EQ(local_release(shm, sub_id, entry_id), -EINVAL);
}

TEST(TopicShm, LocalPublishIsVisibleToDaemonTake)
{
  DaemonHarness h;
  uint64_t pub_addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &pub_addr), 0);
  ASSERT_EQ(h.add_process(kSubPid), 0);
  int32_t pub_id = -1;
  int32_t sub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 10, &pub_id), 0);
  ASSERT_EQ(h.add_subscriber(kTopic, kNode, kSubPid, 10, &sub_id, true, true, true), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  ASSERT_NE(shm, nullptr);

  int64_t entry_id = -1;
  ASSERT_EQ(local_publish(shm, pub_id, pub_addr, &entry_id), 0);

  TakeMsgResponse resp{};
  ASSERT_EQ(h.take(kTopic, sub_id, false, &resp), 0);
  EXPECT_EQ(resp.entry_id, entry_id);
  EXPECT_EQ(resp.addr, pub_addr);
}

// Same order as e2e pubfirst + transient local: the message is already in the
// segment when the take subscriber registers.
TEST(TopicShm, TransientLocalTakeSeesEarlierLocalPublish)
{
  DaemonHarness h;
  uint64_t pub_addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &pub_addr), 0);
  ASSERT_EQ(h.add_process(kSubPid), 0);
  int32_t pub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 1, &pub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  ASSERT_NE(shm, nullptr);

  int64_t entry_id = -1;
  ASSERT_EQ(local_publish(shm, pub_id, pub_addr, &entry_id), 0);

  int32_t sub_id = -1;
  ASSERT_EQ(h.add_subscriber(kTopic, kNode, kSubPid, 1, &sub_id, true, true, true), 0);

  TakeMsgResponse resp{};
  ASSERT_EQ(h.take(kTopic, sub_id, false, &resp), 0);
  EXPECT_EQ(resp.entry_id, entry_id);
  EXPECT_EQ(resp.addr, pub_addr);
  EXPECT_EQ(resp.pub_shm_num, 1U);
}

struct NeverMapped
{
  bool operator()(int32_t) const { return false; }
  bool valid_address(int32_t, uint64_t) const { return true; }
};

TEST(TopicShm, LocalReceiveWaitsForThePublisherMemory)
{
  DaemonHarness h;
  uint64_t pub_addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &pub_addr), 0);
  ASSERT_EQ(h.add_process(kSubPid), 0);
  int32_t sub_id = -1;
  ASSERT_EQ(h.add_subscriber(kTopic, kNode, kSubPid, 10, &sub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  {
    TopicShmLock lock(shm);
    EXPECT_TRUE(topic_shm_consume_mmap_update(shm, sub_id));
    EXPECT_FALSE(topic_shm_consume_mmap_update(shm, sub_id));
  }

  // A new publisher asks the subscriber to look up its memory.
  int32_t pub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 10, &pub_id), 0);
  {
    TopicShmLock lock(shm);
    EXPECT_TRUE(topic_shm_consume_mmap_update(shm, sub_id));
  }
  GetPublisherShmInfosRequest req{};
  DaemonHarness::set_topic(req.topic_name, kTopic);
  req.subscriber_id = sub_id;
  GetPublisherShmInfosResponse infos{};
  ASSERT_EQ(
    h.call(AGNOCAST_CMD_GET_PUBLISHER_SHM_INFOS, kSubPid, &req, sizeof(req), &infos, sizeof(infos)),
    0);
  ASSERT_EQ(infos.pub_shm_num, 1U);
  EXPECT_EQ(infos.pub_shm_infos[0].pid, kPubPid);
  EXPECT_EQ(infos.pub_shm_infos[0].shm_addr, pub_addr);
  // Asked again (a lost reply), the daemon lists it again.
  ASSERT_EQ(
    h.call(AGNOCAST_CMD_GET_PUBLISHER_SHM_INFOS, kSubPid, &req, sizeof(req), &infos, sizeof(infos)),
    0);
  EXPECT_EQ(infos.pub_shm_num, 1U);

  int64_t entry_id = -1;
  ASSERT_EQ(local_publish(shm, pub_id, pub_addr, &entry_id), 0);

  // Not mapped yet: the entry is left where it is.
  int64_t ids[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  uint64_t addrs[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  uint16_t num = 0;
  bool call_again = false;
  {
    TopicShmLock lock(shm);
    ASSERT_EQ(topic_shm_receive(shm, sub_id, NeverMapped{}, ids, addrs, &num, &call_again), 0);
  }
  EXPECT_EQ(num, 0);
  ASSERT_EQ(local_receive(shm, sub_id, ids, &num), 0);
  ASSERT_EQ(num, 1);
  EXPECT_EQ(ids[0], entry_id);
}

TEST(TopicShm, LocalPublishReleasesBeyondDepth)
{
  DaemonHarness h;
  uint64_t pub_addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &pub_addr), 0);
  int32_t pub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 2, &pub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);

  int64_t entry_id = -1;
  uint32_t released_num = 0;
  for (int i = 0; i < 2; i++) {
    ASSERT_EQ(local_publish(shm, pub_id, pub_addr + 64 * i, &entry_id, &released_num), 0);
    EXPECT_EQ(released_num, 0U);
  }
  ASSERT_EQ(local_publish(shm, pub_id, pub_addr + 128, &entry_id, &released_num), 0);
  EXPECT_EQ(released_num, 1U);
  EXPECT_EQ(h.topic_entries_num(kTopic), 2);
}

TEST(TopicShm, SubscriberEventfdsFollowRegistration)
{
  DaemonHarness h;
  ASSERT_EQ(h.add_process(kPubPid), 0);
  ASSERT_EQ(h.add_process(kSubPid), 0);
  int32_t pub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 10, &pub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  const uint64_t gen0 = shm->sub_generation;

  const int efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  ASSERT_GE(efd, 0);
  int32_t sub_id = -1;
  ASSERT_EQ(
    h.add_subscriber(kTopic, kNode, kSubPid, 10, &sub_id, false, true, false, false, false, efd),
    0);
  int32_t take_sub_id = -1;
  ASSERT_EQ(h.add_subscriber(kTopic, kNode, kSubPid, 10, &take_sub_id, false, true, true), 0);
  EXPECT_NE(shm->sub_generation, gen0);

  GetSubscriberEventfdsRequest req{};
  DaemonHarness::set_topic(req.topic_name, kTopic);
  GetSubscriberEventfdsResponse resp{};
  ASSERT_EQ(
    h.call(AGNOCAST_CMD_GET_SUBSCRIBER_EVENTFDS, kPubPid, &req, sizeof(req), &resp, sizeof(resp)),
    0);
  EXPECT_EQ(resp.sub_generation, shm->sub_generation);
  EXPECT_EQ(resp.total, 1U);
  ASSERT_EQ(resp.num, 1U);
  EXPECT_EQ(resp.subscriber_ids[0], sub_id);

  RemoveSubscriberRequest rm{};
  DaemonHarness::set_topic(rm.topic_name, kTopic);
  rm.subscriber_id = sub_id;
  const uint64_t gen1 = shm->sub_generation;
  ASSERT_EQ(h.call(AGNOCAST_CMD_REMOVE_SUBSCRIBER, kSubPid, &rm, sizeof(rm)), 0);
  EXPECT_NE(shm->sub_generation, gen1);
}

TEST(TopicShm, RegistrationReplyCarriesTheSegment)
{
  MetadataStore store;
  MemoryAllocator allocator;
  CommandHandlers handlers(store, allocator);
  int sv[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sv), 0);

  auto call = [&](uint32_t cmd, const void * req, uint32_t size, int * fd_out) {
    std::vector<uint8_t> buf(64 * 1024);
    union {
      cmsghdr align;
      char bytes[CMSG_SPACE(sizeof(int))];
    } cmsg_buf{};
    iovec iov{buf.data(), buf.size()};
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.bytes;
    msg.msg_controllen = sizeof(cmsg_buf.bytes);
    ssize_t n = -1;
    std::thread reader([&]() { n = recvmsg(sv[1], &msg, MSG_CMSG_CLOEXEC); });
    handlers.dispatch(sv[0], kPubPid, RequestHeader{cmd, size}, req, -1);
    reader.join();
    *fd_out = -1;
    if (cmsghdr * c = CMSG_FIRSTHDR(&msg); c != nullptr && c->cmsg_type == SCM_RIGHTS) {
      std::memcpy(fd_out, CMSG_DATA(c), sizeof(int));
    }
    return n;
  };

  AddProcessRequest proc_req{};
  int fd = -1;
  ASSERT_GT(call(AGNOCAST_CMD_ADD_PROCESS, &proc_req, sizeof(proc_req), &fd), 0);
  EXPECT_EQ(fd, -1);

  AddPublisherRequest req{};
  DaemonHarness::set_topic(req.topic_name, kTopic);
  DaemonHarness::set_node(req.node_name, kNode);
  req.qos_depth = 1;
  ASSERT_GT(call(AGNOCAST_CMD_ADD_PUBLISHER, &req, sizeof(req), &fd), 0);
  ASSERT_GE(fd, 0);

  struct stat st
  {
  };
  ASSERT_EQ(fstat(fd, &st), 0);
  EXPECT_EQ(st.st_size, static_cast<off_t>(sizeof(AgnocastTopicShm)));
  // Sealed: a client cannot shrink it under the daemon.
  EXPECT_NE(ftruncate(fd, 0), 0);

  void * mapped =
    mmap(nullptr, sizeof(AgnocastTopicShm), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  ASSERT_NE(mapped, MAP_FAILED);
  auto * client_view = static_cast<AgnocastTopicShm *>(mapped);
  EXPECT_EQ(client_view->magic, AGNOCAST_TOPIC_SHM_MAGIC);
  EXPECT_EQ(client_view->ids[0].kind, TOPIC_SHM_ID_PUBLISHER);
  munmap(mapped, sizeof(AgnocastTopicShm));
  close(fd);
  close(sv[0]);
  close(sv[1]);
}

TEST(TopicShm, LockSurvivesAHolderThatDied)
{
  DaemonHarness h;
  ASSERT_EQ(h.add_process(kPubPid), 0);
  int32_t pub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 1, &pub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);

  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    topic_shm_lock(shm);
    _exit(0);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);

  topic_shm_lock(shm);
  topic_shm_unlock(shm);
  topic_shm_lock(shm);
  topic_shm_unlock(shm);
}
