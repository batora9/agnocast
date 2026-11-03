// SPDX-License-Identifier: Apache-2.0
//
// Rollback of a segment operation whose holder died, and re-registration with
// a restarted daemon.

#include "daemon_test_util.hpp"

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstring>
#include <functional>

using agnocast_test::DaemonHarness;

namespace
{
constexpr const char * kTopic = "/rollback_test";
constexpr const char * kNode = "/rollback_test_node";
constexpr pid_t kPubPid = 1000;
constexpr pid_t kSubPid = 2000;

// Runs body in a child that holds the segment mutex and is killed before it
// unlocks, as a holder crashing mid-operation would be.
void die_holding_lock(AgnocastTopicShm * shm, const std::function<void()> & body)
{
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    topic_shm_lock(shm);
    body();
    kill(getpid(), SIGKILL);
    _exit(1);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status));
}

uint32_t check(AgnocastTopicShm * shm)
{
  char msg[256] = {};
  TopicShmLock lock(shm);
  const uint32_t v = topic_shm_check(shm, msg, sizeof(msg));
  EXPECT_EQ(v, 0U) << msg;
  return v;
}

int publish(AgnocastTopicShm * shm, int32_t pub_id, uint64_t addr, int64_t * entry_id)
{
  uint64_t released[AGNOCAST_PROTO_MAX_RELEASE_NUM];
  uint32_t released_num = 0;
  static int32_t sub_ids[kTopicShmIdNum];
  uint32_t sub_num = 0;
  TopicShmLock lock(shm);
  return topic_shm_publish(shm, pub_id, addr, entry_id, released, &released_num, sub_ids, &sub_num);
}

int receive(AgnocastTopicShm * shm, int32_t sub_id, uint16_t * num)
{
  int64_t ids[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  uint64_t addrs[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  bool call_again = false;
  TopicShmLock lock(shm);
  return topic_shm_receive(shm, sub_id, TopicShmAlwaysMapped{}, ids, addrs, num, &call_again);
}
}  // namespace

TEST(Rollback, PublishInterruptedAfterInsertIsUndone)
{
  DaemonHarness h;
  uint64_t addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &addr), 0);
  int32_t pub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 2, &pub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  int64_t entry_id = -1;
  ASSERT_EQ(publish(shm, pub_id, addr, &entry_id), 0);
  const int64_t current = shm->current_entry_id;
  const uint32_t entry_num = shm->entry_num;

  die_holding_lock(shm, [&] {
    int64_t id = -1;
    topic_shm_insert(shm, pub_id, addr + 64, &id);
    topic_shm_insert(shm, pub_id, addr + 128, &id);
  });

  ASSERT_EQ(check(shm), 0U);
  EXPECT_EQ(shm->rollback_count, 1U);
  EXPECT_EQ(shm->current_entry_id, current);
  EXPECT_EQ(shm->entry_num, entry_num);
  EXPECT_EQ(shm->ids[pub_id].entries_num, entry_num);
  // And the topic keeps working.
  ASSERT_EQ(publish(shm, pub_id, addr + 64, &entry_id), 0);
  EXPECT_EQ(entry_id, current);
  check(shm);
}

TEST(Rollback, ReceiveInterruptedMidwayTakesNoReference)
{
  DaemonHarness h;
  uint64_t addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &addr), 0);
  ASSERT_EQ(h.add_process(kSubPid), 0);
  int32_t pub_id = -1;
  int32_t sub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 10, &pub_id), 0);
  ASSERT_EQ(h.add_subscriber(kTopic, kNode, kSubPid, 10, &sub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  int64_t entry_id = -1;
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(publish(shm, pub_id, addr + 64 * i, &entry_id), 0);
  }
  const int64_t latest = shm->ids[sub_id].latest_received_entry_id;

  die_holding_lock(shm, [&] {
    // Three references taken, latest_received not yet advanced.
    for (uint32_t pos = 0; pos < 3; pos++) {
      topic_shm_set_ref(shm, topic_shm_entry_at(shm, pos), sub_id);
    }
  });

  ASSERT_EQ(check(shm), 0U);
  EXPECT_EQ(shm->ids[sub_id].latest_received_entry_id, latest);
  for (uint32_t pos = 0; pos < shm->entry_num; pos++) {
    EXPECT_EQ(topic_shm_entry_at(shm, pos).ref_num, 0U);
  }
  uint16_t num = 0;
  ASSERT_EQ(receive(shm, sub_id, &num), 0);
  EXPECT_EQ(num, 5);
}

TEST(Rollback, SubscriberRemovalInterruptedMidwayIsUndone)
{
  DaemonHarness h;
  uint64_t addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &addr), 0);
  ASSERT_EQ(h.add_process(kSubPid), 0);
  int32_t pub_id = -1;
  int32_t sub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 10, &pub_id), 0);
  ASSERT_EQ(h.add_subscriber(kTopic, kNode, kSubPid, 10, &sub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  int64_t entry_id = -1;
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(publish(shm, pub_id, addr + 64 * i, &entry_id), 0);
  }
  uint16_t num = 0;
  ASSERT_EQ(receive(shm, sub_id, &num), 0);
  ASSERT_EQ(num, 4);
  const uint32_t sub_num = shm->sub_num;

  die_holding_lock(shm, [&] {
    // The id is gone and half the references cleared when the holder dies.
    topic_shm_clear_id(shm, sub_id);
    for (uint32_t pos = 0; pos < 2; pos++) {
      topic_shm_clear_ref(shm, topic_shm_entry_at(shm, pos), sub_id);
    }
  });

  ASSERT_EQ(check(shm), 0U);
  EXPECT_EQ(shm->ids[sub_id].kind, TOPIC_SHM_ID_SUBSCRIBER);
  EXPECT_EQ(shm->sub_num, sub_num);
  for (uint32_t pos = 0; pos < shm->entry_num; pos++) {
    EXPECT_TRUE(topic_shm_test_ref(topic_shm_entry_at(shm, pos), sub_id));
  }
}

TEST(Rollback, DeathDuringRecoveryIsRecoveredAgain)
{
  DaemonHarness h;
  uint64_t addr = 0;
  ASSERT_EQ(h.add_process(kPubPid, &addr), 0);
  int32_t pub_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 10, &pub_id), 0);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  const int64_t current = shm->current_entry_id;

  die_holding_lock(shm, [&] {
    int64_t id = -1;
    topic_shm_insert(shm, pub_id, addr, &id);
  });
  // The next holder dies too, before finishing anything.
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    pthread_mutex_lock(&shm->mutex);  // EOWNERDEAD, recovery not run
    kill(getpid(), SIGKILL);
    _exit(1);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);

  ASSERT_EQ(check(shm), 0U);
  EXPECT_EQ(shm->current_entry_id, current);
  EXPECT_EQ(shm->entry_num, 0U);
}

// The previous daemon dies; a new one takes the segment back from the clients.
TEST(Restore, RestartedDaemonAdoptsSegmentAndDropsOrphans)
{
  // The recovery checks which clients are alive, so these must be real pids:
  // two that live through the test, and one that is already gone.
  const pid_t kPubPid = getppid();
  const pid_t kSubPid = getpid();
  const pid_t kGonePid = fork();
  ASSERT_GE(kGonePid, 0);
  if (kGonePid == 0) _exit(0);
  ASSERT_EQ(waitpid(kGonePid, nullptr, 0), kGonePid);

  auto old_daemon = std::make_unique<DaemonHarness>();
  uint64_t pub_addr = 0;
  ASSERT_EQ(old_daemon->add_process(kPubPid, &pub_addr), 0);
  ASSERT_EQ(old_daemon->add_process(kSubPid), 0);
  ASSERT_EQ(old_daemon->add_process(kGonePid), 0);
  int32_t pub_id = -1;
  int32_t sub_id = -1;
  int32_t gone_sub_id = -1;
  ASSERT_EQ(old_daemon->add_publisher(kTopic, kNode, kPubPid, 10, &pub_id), 0);
  ASSERT_EQ(old_daemon->add_subscriber(kTopic, kNode, kSubPid, 10, &sub_id), 0);
  ASSERT_EQ(old_daemon->add_subscriber(kTopic, kNode, kGonePid, 10, &gone_sub_id), 0);

  // What the clients keep: the segment, mapped and as a descriptor.
  const int seg_fd = dup(old_daemon->topic_shm_fd(kTopic));
  ASSERT_GE(seg_fd, 0);
  void * mapped =
    mmap(nullptr, sizeof(AgnocastTopicShm), PROT_READ | PROT_WRITE, MAP_SHARED, seg_fd, 0);
  ASSERT_NE(mapped, MAP_FAILED);
  auto * shm = static_cast<AgnocastTopicShm *>(mapped);

  int64_t entry_id = -1;
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(publish(shm, pub_id, pub_addr + 64 * i, &entry_id), 0);
  }
  uint16_t num = 0;
  ASSERT_EQ(receive(shm, sub_id, &num), 0);
  ASSERT_EQ(num, 3);
  ASSERT_EQ(receive(shm, gone_sub_id, &num), 0);
  ASSERT_EQ(num, 3);

  // The old daemon dies holding the segment mutex in the middle of adding a
  // subscriber; that registration never reached its client.
  die_holding_lock(shm, [&] {
    int32_t id = -1;
    topic_shm_add_subscriber(shm, kSubPid, 10, false, false, false, &id);
  });
  old_daemon.reset();

  DaemonHarness new_daemon;
  new_daemon.handlers().start_recovery({kPubPid, kSubPid}, 10000);

  RestoreProcessRequest proc{};
  proc.shm_addr = pub_addr;
  ASSERT_EQ(new_daemon.call(AGNOCAST_CMD_RESTORE_PROCESS, kPubPid, &proc, sizeof(proc)), 0);
  ASSERT_EQ(new_daemon.call(AGNOCAST_CMD_RESTORE_PROCESS, kPubPid, &proc, sizeof(proc)), -EEXIST);

  RestoreTopicRequest topic{};
  DaemonHarness::set_topic(topic.topic_name, kTopic);
  ASSERT_EQ(
    new_daemon.call(
      AGNOCAST_CMD_RESTORE_TOPIC, kPubPid, &topic, sizeof(topic), nullptr, 0, nullptr, dup(seg_fd)),
    0);
  EXPECT_EQ(shm->rollback_count, 1U);

  RestorePublisherRequest pub{};
  DaemonHarness::set_topic(pub.reg.topic_name, kTopic);
  DaemonHarness::set_node(pub.reg.node_name, kNode);
  pub.reg.qos_depth = 10;
  pub.publisher_id = pub_id;
  ASSERT_EQ(new_daemon.call(AGNOCAST_CMD_RESTORE_PUBLISHER, kPubPid, &pub, sizeof(pub)), 0);
  // Not the caller's id.
  ASSERT_EQ(new_daemon.call(AGNOCAST_CMD_RESTORE_PUBLISHER, kSubPid, &pub, sizeof(pub)), -EEXIST);
  ASSERT_EQ(new_daemon.call(AGNOCAST_CMD_RESTORE_DONE, kPubPid, nullptr, 0), 0);

  RestoreProcessRequest sub_proc{};
  sub_proc.shm_addr = pub_addr + MemoryAllocator::kDefaultSizeGb * 1024ULL * 1024ULL * 1024ULL;
  ASSERT_EQ(new_daemon.call(AGNOCAST_CMD_RESTORE_PROCESS, kSubPid, &sub_proc, sizeof(sub_proc)), 0);
  ASSERT_EQ(
    new_daemon.call(
      AGNOCAST_CMD_RESTORE_TOPIC, kSubPid, &topic, sizeof(topic), nullptr, 0, nullptr, dup(seg_fd)),
    0);
  RestoreSubscriberRequest sub{};
  DaemonHarness::set_topic(sub.reg.topic_name, kTopic);
  DaemonHarness::set_node(sub.reg.node_name, kNode);
  sub.reg.qos_depth = 10;
  sub.subscriber_id = sub_id;
  const int efd = eventfd(0, EFD_CLOEXEC);
  ASSERT_GE(efd, 0);
  ASSERT_EQ(
    new_daemon.call(
      AGNOCAST_CMD_RESTORE_SUBSCRIBER, kSubPid, &sub, sizeof(sub), nullptr, 0, nullptr, efd),
    0);
  ASSERT_EQ(new_daemon.call(AGNOCAST_CMD_RESTORE_DONE, kSubPid, nullptr, 0), 0);

  new_daemon.handlers().wait_recovery_end();

  GetRecoveryStatusResponse status{};
  ASSERT_EQ(
    new_daemon.call(AGNOCAST_CMD_GET_RECOVERY_STATUS, kSubPid, nullptr, 0, &status, sizeof(status)),
    0);
  EXPECT_TRUE(status.recovered);
  EXPECT_FALSE(status.in_recovery);
  EXPECT_EQ(status.restored_num, 2U);
  EXPECT_EQ(status.adopted_topic_num, 1U);
  // kGonePid's subscriber was never claimed.
  EXPECT_EQ(status.orphan_subscriber_num, 1U);

  EXPECT_TRUE(new_daemon.publisher_exists(kTopic, pub_id));
  EXPECT_TRUE(new_daemon.subscriber_exists(kTopic, sub_id));
  EXPECT_EQ(shm->ids[gone_sub_id].kind, TOPIC_SHM_ID_FREE);
  // The surviving subscriber still holds what it received.
  EXPECT_EQ(shm->entry_num, 3U);
  for (uint32_t pos = 0; pos < shm->entry_num; pos++) {
    const TopicShmEntry & en = topic_shm_entry_at(shm, pos);
    EXPECT_TRUE(topic_shm_test_ref(en, sub_id));
    EXPECT_FALSE(topic_shm_test_ref(en, gone_sub_id));
  }

  DebugCheckTopicRequest req{};
  DaemonHarness::set_topic(req.topic_name, kTopic);
  DebugCheckTopicResponse resp{};
  ASSERT_EQ(
    new_daemon.call(
      AGNOCAST_CMD_DEBUG_CHECK_TOPIC, kSubPid, &req, sizeof(req), &resp, sizeof(resp)),
    0);
  EXPECT_EQ(resp.shm_violations, 0U) << resp.message;
  EXPECT_EQ(resp.registry_violations, 0U) << resp.message;

  // New ids do not reuse the ones handed out before the restart.
  int32_t new_sub_id = -1;
  ASSERT_EQ(new_daemon.add_subscriber(kTopic, kNode, kSubPid, 10, &new_sub_id), 0);
  EXPECT_GT(new_sub_id, gone_sub_id);
  EXPECT_GT(new_sub_id, sub_id + 1);

  munmap(mapped, sizeof(AgnocastTopicShm));
  close(seg_fd);
}
