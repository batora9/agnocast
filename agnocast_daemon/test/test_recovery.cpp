// SPDX-License-Identifier: Apache-2.0
//
// Daemon restart recovery (protocol.h, "Daemon restart recovery"): a daemon
// that finds live clients of a predecessor in its state file rebuilds its
// metadata from what they report, and serves nothing else until each has
// restored or is known to be dead.
//
// A "crash" here is dropping one DaemonHarness without any exit cleanup and
// starting another on the same state file. Clients are forked children that
// only sleep, so the daemon's liveness checks see real processes.

#include "daemon_test_util.hpp"

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using agnocast_test::DaemonHarness;

namespace
{

constexpr const char * kTopic = "/recovery";

// A process that exists until it is killed.
struct Child
{
  pid_t pid = -1;
  Child()
  {
    pid = fork();
    if (pid == 0) {
      for (;;) pause();
    }
  }
  ~Child() { kill_now(); }
  void kill_now()
  {
    if (pid <= 0) return;
    kill(pid, SIGKILL);
    waitpid(pid, nullptr, 0);
    pid = -1;
  }
  Child(const Child &) = delete;
  Child & operator=(const Child &) = delete;
};

std::string state_path()
{
  static int n = 0;
  return "/tmp/agnocast_test_state_" + std::to_string(getpid()) + "_" + std::to_string(n++);
}

class Recovery : public ::testing::Test
{
protected:
  void SetUp() override { path = state_path(); }
  void TearDown() override
  {
    std::remove(path.c_str());
    std::remove((path + ".tmp").c_str());
    unsetenv("AGNOCAST_DAEMON_RECOVERY_FENCE_MS");
  }

  static int restore_process(DaemonHarness & h, pid_t pid, uint64_t addr)
  {
    RestoreProcessRequest req{};
    req.shm_addr = addr;
    return h.call(AGNOCAST_CMD_RESTORE_PROCESS, pid, &req, sizeof(req));
  }
  static int restore_mapping(DaemonHarness & h, pid_t pid, pid_t owner, uint64_t addr)
  {
    RestoreMappingsRequest req{};
    req.mapping_num = 1;
    req.mappings[0].pid = owner;
    req.mappings[0].shm_addr = addr;
    return h.call(AGNOCAST_CMD_RESTORE_MAPPINGS, pid, &req, sizeof(req));
  }
  static int restore_publisher(
    DaemonHarness & h, pid_t pid, int32_t id, uint32_t depth, int64_t last_entry_id)
  {
    RestorePublisherRequest req{};
    DaemonHarness::set_topic(req.topic_name, kTopic);
    DaemonHarness::set_node(req.node_name, "pub");
    req.publisher_id = id;
    req.qos_depth = depth;
    req.last_entry_id = last_entry_id;
    return h.call(AGNOCAST_CMD_RESTORE_PUBLISHER, pid, &req, sizeof(req));
  }
  static int restore_subscriber(
    DaemonHarness & h, pid_t pid, int32_t id, uint32_t depth, int64_t latest)
  {
    RestoreSubscriberRequest req{};
    DaemonHarness::set_topic(req.topic_name, kTopic);
    DaemonHarness::set_node(req.node_name, "sub");
    req.subscriber_id = id;
    req.qos_depth = depth;
    req.latest_received_entry_id = latest;
    req.qos_is_reliable = true;
    return h.call(AGNOCAST_CMD_RESTORE_SUBSCRIBER, pid, &req, sizeof(req));
  }
  static int restore_entries(
    DaemonHarness & h, pid_t pid, int32_t id, uint32_t kind,
    const std::vector<std::pair<int64_t, uint64_t>> & entries)
  {
    RestoreEntriesRequest req{};
    DaemonHarness::set_topic(req.topic_name, kTopic);
    req.pubsub_id = id;
    req.kind = kind;
    for (const auto & [eid, addr] : entries) {
      req.entries[req.entry_num].entry_id = eid;
      req.entries[req.entry_num].addr = addr;
      req.entry_num++;
    }
    return h.call(AGNOCAST_CMD_RESTORE_ENTRIES, pid, &req, sizeof(req));
  }
  static int restore_done(DaemonHarness & h, pid_t pid)
  {
    return h.call(AGNOCAST_CMD_RESTORE_DONE, pid, nullptr, 0);
  }
  static GetRecoveryStatusResponse status(DaemonHarness & h)
  {
    GetRecoveryStatusResponse resp{};
    EXPECT_EQ(h.call(AGNOCAST_CMD_GET_RECOVERY_STATUS, 0, nullptr, 0, &resp, sizeof(resp)), 0);
    return resp;
  }
  static bool wait_until_recovered(DaemonHarness & h, int timeout_ms)
  {
    for (int waited = 0; waited < timeout_ms; waited += 10) {
      if (status(h).recovering == 0) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
  }
  static SubscriberInfo & sub_info(DaemonHarness & h, int32_t id)
  {
    return h.store().topic_map_.at(DaemonHarness::topic_key(kTopic))->topic.sub_info_map.at(id);
  }
  static EntryNode * entry(DaemonHarness & h, int64_t id)
  {
    auto & entries = h.store().topic_map_.at(DaemonHarness::topic_key(kTopic))->topic.entries;
    const auto it = entries.find(id);
    return it == entries.end() ? nullptr : &it->second;
  }
  static bool state_file_lists(const std::string & path, pid_t pid)
  {
    std::ifstream ifs(path);
    long long p = 0;
    unsigned long long starttime = 0;
    while (ifs >> p >> starttime) {
      if (p == pid) return true;
    }
    return false;
  }

  std::string path;
};

}  // namespace

// The daemon learns of a client before it answers the client's registration,
// and forgets it once the client is gone.
TEST_F(Recovery, StateFileListsLiveClients)
{
  Child c;
  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(path), 0U);
  ASSERT_EQ(h.add_process(c.pid), 0);
  EXPECT_TRUE(state_file_lists(path, c.pid));
  h.disconnect(c.pid);
  EXPECT_FALSE(state_file_lists(path, c.pid));
}

// The client has to report where its receives start even if it received
// nothing before the crash.
TEST_F(Recovery, AddSubscriberReportsWhereReceivesStart)
{
  Child c;
  DaemonHarness h;
  ASSERT_EQ(h.add_process(c.pid), 0);
  AddSubscriberRequest req{};
  DaemonHarness::set_topic(req.topic_name, kTopic);
  DaemonHarness::set_node(req.node_name, "sub");
  req.qos_depth = 5;
  AddSubscriberResponse resp{};
  ASSERT_EQ(h.call(AGNOCAST_CMD_ADD_SUBSCRIBER, c.pid, &req, sizeof(req), &resp, sizeof(resp)), 0);
  EXPECT_EQ(
    resp.latest_received_entry_id, sub_info(h, resp.subscriber_id).latest_received_entry_id);
}

TEST_F(Recovery, RefusesToRestoreOutsideARecovery)
{
  Child c;
  DaemonHarness h;
  EXPECT_EQ(restore_process(h, c.pid, MemoryAllocator::kDefaultStartAddr), -EBUSY);
}

// The full path: a publisher and a subscriber restore in either order, the
// references come back, other clients wait for the end of the grace period,
// and a publish whose reply was lost (the subscriber got the message, the
// publisher only knows it asked) is answered from the entry instead of being
// published twice.
TEST_F(Recovery, RebuildsFromReportsAndAnswersARepeatedPublishFromTheEntry)
{
  Child p;
  Child s;
  Child late;
  uint64_t pool_p = 0;
  uint64_t pool_s = 0;
  int32_t pub = -1;
  int32_t sub = -1;
  int64_t e0 = -1;
  int64_t e1 = -1;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(path), 0U);
    ASSERT_EQ(old.add_process(p.pid, &pool_p), 0);
    ASSERT_EQ(old.add_process(s.pid, &pool_s), 0);
    ASSERT_EQ(old.add_publisher(kTopic, "pub", p.pid, 2, &pub), 0);
    ASSERT_EQ(old.add_subscriber(kTopic, "sub", s.pid, 5, &sub), 0);
    PublishMsgResponse pr{};
    ReceiveMsgResponse rr{};
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x100, &pr, p.pid), 0);
    e0 = pr.entry_id;
    ASSERT_EQ(old.receive(kTopic, sub, &rr, s.pid), 0);
    ASSERT_EQ(rr.entry_num, 1U);
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x200, &pr, p.pid), 0);
    e1 = pr.entry_id;  // the publisher never sees this reply
    ASSERT_EQ(old.receive(kTopic, sub, &rr, s.pid), 0);
    ASSERT_EQ(rr.entry_num, 1U);
  }  // the daemon crashes

  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(path), 2U);
  EXPECT_EQ(status(h).recovering, 1U);

  // A newcomer waits for the grace period to end.
  std::atomic<bool> late_done{false};
  std::thread newcomer([&] {
    AddProcessRequest req{};
    AddProcessResponse resp{};
    EXPECT_EQ(
      h.call_isolated(AGNOCAST_CMD_ADD_PROCESS, late.pid, &req, sizeof(req), &resp, sizeof(resp)),
      0);
    late_done = true;
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(late_done);

  // The subscriber first: what it holds waits for the publisher's report.
  ASSERT_EQ(restore_process(h, s.pid, pool_s), 0);
  ASSERT_EQ(restore_mapping(h, s.pid, p.pid, pool_p), 0);
  ASSERT_EQ(restore_subscriber(h, s.pid, sub, 5, e1), 0);
  ASSERT_EQ(
    restore_entries(
      h, s.pid, sub, AGNOCAST_RESTORE_ENTRY_HELD, {{e0, pool_p + 0x100}, {e1, pool_p + 0x200}}),
    0);
  ASSERT_EQ(restore_done(h, s.pid), 0);
  EXPECT_EQ(status(h).recovering, 1U);
  EXPECT_FALSE(late_done);

  ASSERT_EQ(restore_process(h, p.pid, pool_p), 0);
  ASSERT_EQ(restore_publisher(h, p.pid, pub, 2, e0), 0);
  ASSERT_EQ(
    restore_entries(h, p.pid, pub, AGNOCAST_RESTORE_ENTRY_PUBLISHED, {{e0, pool_p + 0x100}}), 0);
  ASSERT_EQ(
    restore_entries(h, p.pid, pub, AGNOCAST_RESTORE_ENTRY_PENDING, {{-1, pool_p + 0x200}}), 0);
  ASSERT_EQ(restore_done(h, p.pid), 0);

  const GetRecoveryStatusResponse st = status(h);
  EXPECT_EQ(st.recovering, 0U);
  EXPECT_EQ(st.restored_num, 2U);
  EXPECT_EQ(st.claim_num, 2U);
  EXPECT_EQ(st.rejected_claim_num, 0U);
  EXPECT_EQ(st.orphan_claim_num, 0U);
  newcomer.join();
  EXPECT_TRUE(late_done);

  ASSERT_NE(entry(h, e0), nullptr);
  ASSERT_NE(entry(h, e1), nullptr);
  EXPECT_TRUE(entry(h, e0)->referencing_subscribers.test(static_cast<size_t>(sub)));
  EXPECT_TRUE(entry(h, e1)->referencing_subscribers.test(static_cast<size_t>(sub)));

  // The repeated publish gets the entry the predecessor committed.
  PublishMsgResponse pr{};
  ASSERT_EQ(h.publish(kTopic, pub, pool_p + 0x200, &pr, p.pid), 0);
  EXPECT_EQ(pr.entry_id, e1);
  EXPECT_EQ(status(h).dedup_publish_num, 1U);
  EXPECT_EQ(h.topic_entries_num(kTopic), 2);

  // Nothing old is delivered again, and released entries go back as usual.
  ReceiveMsgResponse rr{};
  ASSERT_EQ(h.receive(kTopic, sub, &rr, s.pid), 0);
  EXPECT_EQ(rr.entry_num, 0U);
  ASSERT_EQ(h.release_sub_ref_isolated(kTopic, sub, e0, s.pid), 0);
  ASSERT_EQ(h.publish(kTopic, pub, pool_p + 0x300, &pr, p.pid), 0);
  EXPECT_GT(pr.entry_id, e1);
  ASSERT_EQ(pr.released_num, 1U);
  EXPECT_EQ(pr.released_addrs[0], pool_p + 0x100);
}

// A subscriber keeps a message of a publisher that died while the daemon was
// down: the daemon keeps the entry for it, does not deliver that publisher's
// messages again, and drops the entry with the subscriber.
TEST_F(Recovery, KeepsAReferenceToAnEntryOfAPublisherThatDied)
{
  Child p;
  Child s;
  uint64_t pool_p = 0;
  uint64_t pool_s = 0;
  int32_t pub = -1;
  int32_t sub = -1;
  int64_t e0 = -1;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(path), 0U);
    ASSERT_EQ(old.add_process(p.pid, &pool_p), 0);
    ASSERT_EQ(old.add_process(s.pid, &pool_s), 0);
    ASSERT_EQ(old.add_publisher(kTopic, "pub", p.pid, 2, &pub), 0);
    ASSERT_EQ(old.add_subscriber(kTopic, "sub", s.pid, 5, &sub), 0);
    PublishMsgResponse pr{};
    ReceiveMsgResponse rr{};
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x100, &pr, p.pid), 0);
    e0 = pr.entry_id;
    ASSERT_EQ(old.receive(kTopic, sub, &rr, s.pid), 0);
  }
  p.kill_now();

  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(path), 1U);
  ASSERT_EQ(restore_process(h, s.pid, pool_s), 0);
  ASSERT_EQ(restore_mapping(h, s.pid, p.pid, pool_p), 0);
  ASSERT_EQ(restore_subscriber(h, s.pid, sub, 5, e0), 0);
  ASSERT_EQ(restore_entries(h, s.pid, sub, AGNOCAST_RESTORE_ENTRY_HELD, {{e0, pool_p + 0x100}}), 0);
  ASSERT_EQ(restore_done(h, s.pid), 0);

  const GetRecoveryStatusResponse st = status(h);
  EXPECT_EQ(st.recovering, 0U);
  EXPECT_EQ(st.orphan_claim_num, 1U);
  ASSERT_NE(entry(h, e0), nullptr);
  EXPECT_TRUE(entry(h, e0)->referencing_subscribers.test(static_cast<size_t>(sub)));

  ReceiveMsgResponse rr{};
  ASSERT_EQ(h.receive(kTopic, sub, &rr, s.pid), 0);
  EXPECT_EQ(rr.entry_num, 0U);
  EXPECT_EQ(h.release_sub_ref_isolated(kTopic, sub, e0, s.pid), 0);
  h.disconnect(s.pid);
  EXPECT_FALSE(h.topic_exists(kTopic));
}

// A reference no live publisher accounts for is not taken; its release later
// still succeeds, so the client is not failed for it.
TEST_F(Recovery, RejectsAReferenceALivePublisherDoesNotReport)
{
  Child p;
  Child s;
  uint64_t pool_p = 0;
  uint64_t pool_s = 0;
  int32_t pub = -1;
  int32_t sub = -1;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(path), 0U);
    ASSERT_EQ(old.add_process(p.pid, &pool_p), 0);
    ASSERT_EQ(old.add_process(s.pid, &pool_s), 0);
    ASSERT_EQ(old.add_publisher(kTopic, "pub", p.pid, 2, &pub), 0);
    ASSERT_EQ(old.add_subscriber(kTopic, "sub", s.pid, 5, &sub), 0);
  }

  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(path), 2U);
  ASSERT_EQ(restore_process(h, p.pid, pool_p), 0);
  ASSERT_EQ(restore_publisher(h, p.pid, pub, 2, -1), 0);
  ASSERT_EQ(restore_done(h, p.pid), 0);
  ASSERT_EQ(restore_process(h, s.pid, pool_s), 0);
  ASSERT_EQ(restore_mapping(h, s.pid, p.pid, pool_p), 0);
  ASSERT_EQ(restore_subscriber(h, s.pid, sub, 5, -1), 0);
  ASSERT_EQ(restore_entries(h, s.pid, sub, AGNOCAST_RESTORE_ENTRY_HELD, {{7, pool_p + 0x40}}), 0);
  ASSERT_EQ(restore_done(h, s.pid), 0);

  EXPECT_EQ(status(h).rejected_claim_num, 1U);
  EXPECT_EQ(entry(h, 7), nullptr);
  EXPECT_EQ(h.release_sub_ref_isolated(kTopic, sub, 7, s.pid), 0);
}

// A client that dies before restoring settles itself.
TEST_F(Recovery, SettlesAClientThatDiesDuringTheGracePeriod)
{
  Child c;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(path), 0U);
    ASSERT_EQ(old.add_process(c.pid), 0);
  }
  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(path), 1U);
  EXPECT_EQ(status(h).recovering, 1U);
  c.kill_now();
  ASSERT_TRUE(wait_until_recovered(h, 2000));
  const GetRecoveryStatusResponse st = status(h);
  EXPECT_EQ(st.dead_num, 1U);
  EXPECT_EQ(st.fenced_num, 0U);
  EXPECT_FALSE(state_file_lists(path, c.pid));
}

// A client that is alive but never restores (stopped, hung) is killed after
// AGNOCAST_DAEMON_RECOVERY_FENCE_MS, which ends the grace period.
TEST_F(Recovery, KillsAClientThatDoesNotRestoreInTime)
{
  setenv("AGNOCAST_DAEMON_RECOVERY_FENCE_MS", "150", 1);
  Child c;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(path), 0U);
    ASSERT_EQ(old.add_process(c.pid), 0);
  }
  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(path), 1U);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(status(h).recovering, 1U);
  ASSERT_TRUE(wait_until_recovered(h, 3000));
  const GetRecoveryStatusResponse st = status(h);
  EXPECT_EQ(st.fenced_num, 1U);
  EXPECT_EQ(st.dead_num, 1U);
  int wstatus = 0;
  ASSERT_EQ(waitpid(c.pid, &wstatus, 0), c.pid);
  EXPECT_TRUE(WIFSIGNALED(wstatus) && WTERMSIG(wstatus) == SIGKILL);
  c.pid = -1;
}

// Clients of a predecessor that are already gone are not waited for.
TEST_F(Recovery, DoesNotWaitForClientsThatAreGone)
{
  {
    Child c;
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(path), 0U);
    ASSERT_EQ(old.add_process(c.pid), 0);
  }  // the child is killed and reaped
  DaemonHarness h;
  EXPECT_EQ(h.begin_recovery(path), 0U);
  EXPECT_EQ(status(h).recovering, 0U);
}

// The predecessor committed and delivered a publish whose reply was lost, and
// the subscriber has released it since: only the subscriber's list of recently
// delivered entries says so. The repeated publish is answered from that entry
// instead of being delivered a second time.
TEST_F(Recovery, RecognizesAPendingPublishASubscriberAlreadyReleased)
{
  Child p;
  Child s;
  uint64_t pool_p = 0;
  uint64_t pool_s = 0;
  int32_t pub = -1;
  int32_t sub = -1;
  int64_t e0 = -1;
  int64_t e1 = -1;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(path), 0U);
    ASSERT_EQ(old.add_process(p.pid, &pool_p), 0);
    ASSERT_EQ(old.add_process(s.pid, &pool_s), 0);
    ASSERT_EQ(old.add_publisher(kTopic, "pub", p.pid, 5, &pub), 0);
    ASSERT_EQ(old.add_subscriber(kTopic, "sub", s.pid, 5, &sub), 0);
    PublishMsgResponse pr{};
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x100, &pr, p.pid), 0);
    e0 = pr.entry_id;
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x200, &pr, p.pid), 0);
    e1 = pr.entry_id;  // never answered
  }

  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(path), 2U);
  ASSERT_EQ(restore_process(h, p.pid, pool_p), 0);
  ASSERT_EQ(restore_publisher(h, p.pid, pub, 5, e0), 0);
  ASSERT_EQ(
    restore_entries(h, p.pid, pub, AGNOCAST_RESTORE_ENTRY_PUBLISHED, {{e0, pool_p + 0x100}}), 0);
  ASSERT_EQ(
    restore_entries(h, p.pid, pub, AGNOCAST_RESTORE_ENTRY_PENDING, {{-1, pool_p + 0x200}}), 0);
  ASSERT_EQ(restore_done(h, p.pid), 0);
  ASSERT_EQ(restore_process(h, s.pid, pool_s), 0);
  ASSERT_EQ(restore_mapping(h, s.pid, p.pid, pool_p), 0);
  ASSERT_EQ(restore_subscriber(h, s.pid, sub, 5, e1), 0);
  ASSERT_EQ(
    restore_entries(
      h, s.pid, sub, AGNOCAST_RESTORE_ENTRY_DELIVERED,
      {{e0, pool_p + 0x100}, {e1, pool_p + 0x200}}),
    0);
  ASSERT_EQ(restore_done(h, s.pid), 0);
  ASSERT_EQ(status(h).recovering, 0U);

  ASSERT_NE(entry(h, e1), nullptr);
  PublishMsgResponse pr{};
  ASSERT_EQ(h.publish(kTopic, pub, pool_p + 0x200, &pr, p.pid), 0);
  EXPECT_EQ(pr.entry_id, e1);
  ReceiveMsgResponse rr{};
  ASSERT_EQ(h.receive(kTopic, sub, &rr, s.pid), 0);
  EXPECT_EQ(rr.entry_num, 0U);  // not delivered again
}

// A publisher reuses an address once told to free it, so a recently delivered
// entry at the pending address may be an older message. Only an id newer than
// the publisher's last answered one is taken for the pending publish.
TEST_F(Recovery, DoesNotMistakeAnOlderMessageAtTheSameAddressForAPendingPublish)
{
  Child p;
  Child s;
  uint64_t pool_p = 0;
  uint64_t pool_s = 0;
  int32_t pub = -1;
  int32_t sub = -1;
  int64_t e0 = -1;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(path), 0U);
    ASSERT_EQ(old.add_process(p.pid, &pool_p), 0);
    ASSERT_EQ(old.add_process(s.pid, &pool_s), 0);
    ASSERT_EQ(old.add_publisher(kTopic, "pub", p.pid, 5, &pub), 0);
    ASSERT_EQ(old.add_subscriber(kTopic, "sub", s.pid, 5, &sub), 0);
    PublishMsgResponse pr{};
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x100, &pr, p.pid), 0);
    e0 = pr.entry_id;
  }

  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(path), 2U);
  // e0 was freed (not reported), its address reused for a publish the
  // predecessor never committed.
  ASSERT_EQ(restore_process(h, p.pid, pool_p), 0);
  ASSERT_EQ(restore_publisher(h, p.pid, pub, 5, e0), 0);
  ASSERT_EQ(
    restore_entries(h, p.pid, pub, AGNOCAST_RESTORE_ENTRY_PENDING, {{-1, pool_p + 0x100}}), 0);
  ASSERT_EQ(restore_done(h, p.pid), 0);
  ASSERT_EQ(restore_process(h, s.pid, pool_s), 0);
  ASSERT_EQ(restore_mapping(h, s.pid, p.pid, pool_p), 0);
  ASSERT_EQ(restore_subscriber(h, s.pid, sub, 5, e0), 0);
  ASSERT_EQ(
    restore_entries(h, s.pid, sub, AGNOCAST_RESTORE_ENTRY_DELIVERED, {{e0, pool_p + 0x100}}), 0);
  ASSERT_EQ(restore_done(h, s.pid), 0);

  EXPECT_EQ(entry(h, e0), nullptr);
  PublishMsgResponse pr{};
  ASSERT_EQ(h.publish(kTopic, pub, pool_p + 0x100, &pr, p.pid), 0);
  EXPECT_GT(pr.entry_id, e0);  // a new message, delivered
  ReceiveMsgResponse rr{};
  ASSERT_EQ(h.receive(kTopic, sub, &rr, s.pid), 0);
  ASSERT_EQ(rr.entry_num, 1U);
  EXPECT_EQ(rr.entry_ids[0], pr.entry_id);
}
