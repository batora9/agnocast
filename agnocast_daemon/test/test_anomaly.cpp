// SPDX-License-Identifier: Apache-2.0
//
// Clients run the data plane and rely on the daemon when something goes wrong:
// a damaged segment is rebuilt from the registry, a process that wrote garbage
// is quarantined, a stalled holder is killed, and a dead subscriber's
// references are dropped by a peer while the daemon is away.

#include "daemon_test_util.hpp"

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstring>

using agnocast_test::DaemonHarness;

namespace
{

constexpr const char * kTopic = "/anomaly_test";
constexpr const char * kNode = "/anomaly_test_node";
constexpr pid_t kPubPid = 1000;
constexpr pid_t kSubPid = 2000;
constexpr pid_t kRoguePid = 3000;

uint32_t violations(AgnocastTopicShm * shm, char (&msg)[256])
{
  TopicShmLock lock(shm);
  return topic_shm_check(shm, msg, sizeof(msg));
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

int report(
  DaemonHarness & h, pid_t reporter, uint32_t kind, uint32_t epoch, ReportAnomalyResponse * out,
  int64_t entry_id = -1)
{
  ReportAnomalyRequest req{};
  DaemonHarness::set_topic(req.topic_name, kTopic);
  req.kind = kind;
  req.entry_id = entry_id;
  req.epoch = epoch;
  return h.call(AGNOCAST_CMD_REPORT_ANOMALY, reporter, &req, sizeof(req), out, sizeof(*out));
}

int get_segment(DaemonHarness & h, pid_t pid, int32_t id, GetTopicSegmentResponse * out)
{
  GetTopicSegmentRequest req{};
  DaemonHarness::set_topic(req.topic_name, kTopic);
  req.pubsub_id = id;
  return h.call(AGNOCAST_CMD_GET_TOPIC_SEGMENT, pid, &req, sizeof(req), out, sizeof(*out));
}

DebugCheckTopicResponse debug_check(DaemonHarness & h, pid_t pid)
{
  DebugCheckTopicRequest req{};
  DaemonHarness::set_topic(req.topic_name, kTopic);
  DebugCheckTopicResponse resp{};
  EXPECT_EQ(h.call(AGNOCAST_CMD_DEBUG_CHECK_TOPIC, pid, &req, sizeof(req), &resp, sizeof(resp)), 0);
  return resp;
}

// A mapping of the topic's current segment of its own, as a client has: it
// stays valid after the daemon replaces (and unmaps) the segment.
AgnocastTopicShm * client_map(DaemonHarness & h)
{
  void * p = mmap(
    nullptr, sizeof(AgnocastTopicShm), PROT_READ | PROT_WRITE, MAP_SHARED, h.topic_shm_fd(kTopic),
    0);
  return p == MAP_FAILED ? nullptr : static_cast<AgnocastTopicShm *>(p);
}

struct Topology
{
  uint64_t pub_addr = 0;
  int32_t pub_id = -1;
  int32_t sub_id = -1;
};

void setup(DaemonHarness & h, Topology * t)
{
  ASSERT_EQ(h.add_process(kPubPid, &t->pub_addr), 0);
  ASSERT_EQ(h.add_process(kSubPid), 0);
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kPubPid, 10, &t->pub_id), 0);
  ASSERT_EQ(h.add_subscriber(kTopic, kNode, kSubPid, 10, &t->sub_id), 0);
}

}  // namespace

TEST(TopicShmAnomaly, LayoutHashIsStampedAndChecked)
{
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  EXPECT_EQ(shm->layout_hash, topic_shm_layout_hash());
  char msg[256] = {};
  EXPECT_EQ(violations(shm, msg), 0U) << msg;
  shm->layout_hash ^= 1;
  EXPECT_GT(violations(shm, msg), 0U);
  TopicShmLock lock(shm);
  EXPECT_FALSE(topic_shm_quick_check(shm));
}

TEST(TopicShmAnomaly, ReaderSkipsAndReportsAnEntryOutsideThePool)
{
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  int64_t good = -1;
  int64_t bad = -1;
  ASSERT_EQ(publish(shm, t.pub_id, 0x10, &bad), 0);
  ASSERT_EQ(publish(shm, t.pub_id, t.pub_addr, &good), 0);

  struct OnlyGood
  {
    uint64_t addr;
    bool operator()(int32_t) const { return true; }
    bool valid_address(int32_t, uint64_t a) const { return a == addr; }
  };
  int64_t ids[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  uint64_t addrs[AGNOCAST_PROTO_MAX_RECEIVE_NUM];
  uint16_t num = 0;
  bool call_again = false;
  TopicShmBadEntry found;
  {
    TopicShmLock lock(shm);
    ASSERT_EQ(
      topic_shm_receive(shm, t.sub_id, OnlyGood{t.pub_addr}, ids, addrs, &num, &call_again, &found),
      0);
  }
  ASSERT_EQ(num, 1);
  EXPECT_EQ(ids[0], good);
  EXPECT_EQ(found.entry_id, bad);
  EXPECT_EQ(found.publisher_pid, kPubPid);
}

TEST(TopicShmAnomaly, DaemonQuarantinesTheWriterOfABadAddress)
{
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  uint64_t rogue_addr = 0;
  ASSERT_EQ(h.add_process(kRoguePid, &rogue_addr), 0);
  int32_t rogue_id = -1;
  ASSERT_EQ(h.add_publisher(kTopic, kNode, kRoguePid, 10, &rogue_id), 0);

  AgnocastTopicShm * client = client_map(h);
  ASSERT_NE(client, nullptr);
  int64_t good = -1;
  int64_t bad = -1;
  ASSERT_EQ(publish(client, t.pub_id, t.pub_addr, &good), 0);
  ASSERT_EQ(publish(client, rogue_id, 0x10, &bad), 0);

  ReportAnomalyResponse resp{};
  ASSERT_EQ(report(h, kSubPid, AGNOCAST_ANOMALY_BAD_ADDRESS, 0, &resp, bad), 0);
  EXPECT_EQ(resp.action, static_cast<uint32_t>(AGNOCAST_ANOMALY_ACTION_ROTATED));
  EXPECT_EQ(resp.epoch, 1U);

  // The old segment tells its users to move; the new one is consistent,
  // keeps the genuine entry and drops the rogue's.
  EXPECT_TRUE(topic_shm_is_retired(client));
  AgnocastTopicShm * fresh = h.topic_shm(kTopic);
  ASSERT_NE(fresh, client);
  EXPECT_EQ(fresh->mode, static_cast<uint32_t>(TOPIC_SHM_MODE_CLIENT));
  char msg[256] = {};
  EXPECT_EQ(violations(fresh, msg), 0U) << msg;
  EXPECT_EQ(fresh->ids[rogue_id].kind, TOPIC_SHM_ID_FREE);
  uint16_t num = 0;
  ASSERT_EQ(receive(fresh, t.sub_id, &num), 0);
  EXPECT_EQ(num, 1);

  GetTopicSegmentResponse seg{};
  EXPECT_EQ(get_segment(h, kRoguePid, rogue_id, &seg), -EPERM);
  ASSERT_EQ(get_segment(h, kSubPid, t.sub_id, &seg), 0);
  EXPECT_EQ(seg.epoch, 1U);
  int32_t again = -1;
  EXPECT_EQ(h.add_publisher(kTopic, kNode, kRoguePid, 10, &again), -EPERM);

  const DebugCheckTopicResponse check = debug_check(h, kSubPid);
  EXPECT_EQ(check.shm_violations + check.registry_violations, 0U) << check.message;
  EXPECT_EQ(check.quarantined_num, 1U);
  EXPECT_EQ(check.rotation_num, 1U);
  munmap(client, sizeof(AgnocastTopicShm));
}

TEST(TopicShmAnomaly, AGenuineAddressIsNotBlamed)
{
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  int64_t good = -1;
  ASSERT_EQ(publish(h.topic_shm(kTopic), t.pub_id, t.pub_addr, &good), 0);
  ReportAnomalyResponse resp{};
  ASSERT_EQ(report(h, kSubPid, AGNOCAST_ANOMALY_BAD_ADDRESS, 0, &resp, good), 0);
  EXPECT_EQ(resp.action, static_cast<uint32_t>(AGNOCAST_ANOMALY_ACTION_NONE));
  EXPECT_EQ(debug_check(h, kSubPid).quarantined_num, 0U);
}

TEST(TopicShmAnomaly, ScribbledSegmentMovesTheTopicToTheDaemonPath)
{
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  AgnocastTopicShm * client = client_map(h);
  ASSERT_NE(client, nullptr);
  int64_t entry_id = -1;
  ASSERT_EQ(publish(client, t.pub_id, t.pub_addr, &entry_id), 0);
  uint16_t num = 0;
  ASSERT_EQ(receive(client, t.sub_id, &num), 0);  // the subscriber holds it
  ASSERT_EQ(num, 1);

  // A stray write, without the mutex.
  client->entry_num = kTopicShmEntrySlots + 7;
  client->order[0] = 0xffff;

  ReportAnomalyResponse resp{};
  ASSERT_EQ(report(h, kSubPid, AGNOCAST_ANOMALY_CHECK_FAILED, 0, &resp), 0);
  EXPECT_EQ(resp.action, static_cast<uint32_t>(AGNOCAST_ANOMALY_ACTION_DAEMON));
  AgnocastTopicShm * fresh = h.topic_shm(kTopic);
  EXPECT_EQ(fresh->mode, static_cast<uint32_t>(TOPIC_SHM_MODE_DAEMON));
  char msg[256] = {};
  EXPECT_EQ(violations(fresh, msg), 0U) << msg;
  // The held entry survived with its reference, so the subscriber can still
  // release it, now through the daemon.
  EXPECT_EQ(h.entry_rc(kTopic, entry_id, t.sub_id), 1);
  EXPECT_EQ(h.release_sub_ref(kTopic, t.sub_id, entry_id), 0);
  // And the daemon path carries on.
  PublishMsgResponse pub_resp{};
  ASSERT_EQ(h.publish(kTopic, t.pub_id, t.pub_addr + 64, &pub_resp, kPubPid), 0);
  ReceiveMsgResponse recv_resp{};
  ASSERT_EQ(h.receive(kTopic, t.sub_id, &recv_resp, kSubPid), 0);
  EXPECT_EQ(recv_resp.entry_num, 1);
  // On the daemon path only the owner may publish as a publisher id.
  EXPECT_EQ(h.publish(kTopic, t.pub_id, t.pub_addr, &pub_resp, kSubPid), -EPERM);
  munmap(client, sizeof(AgnocastTopicShm));
}

TEST(TopicShmAnomaly, ReportAboutAnOlderEpochIsStale)
{
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  h.topic_shm(kTopic)->entry_num = kTopicShmEntrySlots + 1;
  ReportAnomalyResponse resp{};
  ASSERT_EQ(report(h, kSubPid, AGNOCAST_ANOMALY_CHECK_FAILED, 0, &resp), 0);
  ASSERT_EQ(resp.epoch, 1U);
  ASSERT_EQ(report(h, kPubPid, AGNOCAST_ANOMALY_CHECK_FAILED, 0, &resp), 0);
  EXPECT_EQ(resp.action, static_cast<uint32_t>(AGNOCAST_ANOMALY_ACTION_STALE));
  EXPECT_EQ(debug_check(h, kSubPid).rotation_num, 1U);
}

TEST(TopicShmAnomaly, StalledHolderIsKilledAndRolledBack)
{
  setenv("AGNOCAST_STALL_CONFIRM_MS", "50", 1);
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  (void)topic_shm_cached_pid();

  int pipefd[2];
  ASSERT_EQ(pipe(pipefd), 0);
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    topic_shm_lock(shm);
    int64_t entry_id = -1;
    topic_shm_insert(shm, t.pub_id, t.pub_addr, &entry_id);  // half an operation
    const char c = 'x';
    (void)!write(pipefd[1], &c, 1);
    raise(SIGSTOP);
    _exit(1);
  }
  char c = 0;
  ASSERT_EQ(read(pipefd[0], &c, 1), 1);
  close(pipefd[0]);
  close(pipefd[1]);
  EXPECT_EQ(shm->holder_pid, child);

  ReportAnomalyResponse resp{};
  ASSERT_EQ(report(h, kSubPid, AGNOCAST_ANOMALY_STALL, 0, &resp), 0);
  EXPECT_EQ(resp.action, static_cast<uint32_t>(AGNOCAST_ANOMALY_ACTION_KILLED));
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  EXPECT_TRUE(WIFSIGNALED(status));
  EXPECT_EQ(WTERMSIG(status), SIGKILL);

  const DebugCheckTopicResponse check = debug_check(h, kSubPid);
  EXPECT_EQ(check.shm_violations, 0U) << check.message;
  EXPECT_EQ(check.stall_kill_num, 1U);
  EXPECT_EQ(check.rollback_count, 1U);
  EXPECT_EQ(check.entry_num, 0U);
  unsetenv("AGNOCAST_STALL_CONFIRM_MS");
}

TEST(TopicShmAnomaly, PeerReclaimsADeadSubscriber)
{
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  AgnocastTopicShm * shm = h.topic_shm(kTopic);
  for (int i = 0; i < 5; i++) {
    int64_t entry_id = -1;
    ASSERT_EQ(publish(shm, t.pub_id, t.pub_addr + 64 * static_cast<uint64_t>(i), &entry_id), 0);
  }
  uint16_t num = 0;
  ASSERT_EQ(receive(shm, t.sub_id, &num), 0);
  ASSERT_EQ(num, 5);

  uint32_t n = 0;
  {
    TopicShmLock lock(shm);
    n = topic_shm_reclaim_dead_subscribers(
      shm, [&](int32_t pid, uint64_t) { return pid != kSubPid; });
  }
  EXPECT_EQ(n, 1U);
  EXPECT_EQ(shm->peer_reclaim_count, 1U);
  EXPECT_EQ(shm->ids[t.sub_id].reclaimed, 1);
  char msg[256] = {};
  EXPECT_EQ(violations(shm, msg), 0U) << msg;
  for (uint32_t pos = 0; pos < shm->entry_num; pos++) {
    EXPECT_EQ(topic_shm_entry_at(shm, pos).ref_num, 0U);
  }
  EXPECT_EQ(receive(shm, t.sub_id, &num), -ESRCH);

  // The daemon's own cleanup later finds nothing left to undo.
  h.disconnect(kSubPid);
  EXPECT_EQ(violations(shm, msg), 0U) << msg;
  EXPECT_FALSE(h.subscriber_exists(kTopic, t.sub_id));
}

TEST(TopicShmAnomaly, ProcessStartTimeTellsAReusedPid)
{
  const uint64_t self = topic_shm_proc_starttime(getpid());
  ASSERT_NE(self, 0U);
  EXPECT_TRUE(topic_shm_proc_alive(getpid(), self));
  EXPECT_FALSE(topic_shm_proc_alive(getpid(), self + 1));
  EXPECT_TRUE(topic_shm_proc_alive(getpid(), 0));
}

TEST(TopicShmAnomaly, RetiredSegmentIsNotAdopted)
{
  DaemonHarness h;
  Topology t;
  setup(h, &t);
  const int fd = dup(h.topic_shm_fd(kTopic));
  ASSERT_GE(fd, 0);
  h.topic_shm(kTopic)->entry_num = kTopicShmEntrySlots + 1;
  ReportAnomalyResponse resp{};
  ASSERT_EQ(report(h, kSubPid, AGNOCAST_ANOMALY_CHECK_FAILED, 0, &resp), 0);

  MetadataStore other;
  TopicWrapper * wrapper = nullptr;
  std::unique_lock lock(other.global_mutex_);
  EXPECT_EQ(other.adopt_topic(TopicKey{kTopic, 0}, fd, &wrapper), -EINVAL);
}
