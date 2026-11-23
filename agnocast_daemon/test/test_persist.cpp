// SPDX-License-Identifier: Apache-2.0
//
// Recovery from persisted metadata (persist.hpp): with a sink linked in (A, B),
// a restarted daemon loads what its predecessor persisted, and the restoring
// clients only reattach and reconcile what they own. Without a sink (C) these
// tests are skipped.

#include "daemon_test_util.hpp"
#include "persist.hpp"

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <string>
#include <vector>

using agnocast_test::DaemonHarness;

namespace
{

constexpr const char * kTopic = "/persisted";

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
  ~Child()
  {
    if (pid > 0) {
      kill(pid, SIGKILL);
      waitpid(pid, nullptr, 0);
    }
  }
};

class Persist : public ::testing::Test
{
protected:
  void SetUp() override
  {
    unsetenv("AGNOCAST_DAEMON_PERSIST");
    const std::string base = "/tmp/agnocast_test_persist_" + std::to_string(getpid());
    state = base + ".state";
    medium = base + ".medium";
    setenv("AGNOCAST_DAEMON_REGION_PATH", medium.c_str(), 1);
    setenv("AGNOCAST_DAEMON_LOG_PATH", medium.c_str(), 1);
    const bool has_sink = persist::make_sink() != nullptr;
    std::remove(medium.c_str());
    if (!has_sink) GTEST_SKIP() << "no persistence sink in this build (C)";
  }
  void TearDown() override
  {
    std::remove(state.c_str());
    std::remove((state + ".tmp").c_str());
    std::remove(medium.c_str());
  }
  static int call(DaemonHarness & h, uint32_t cmd, pid_t pid, const void * req, uint32_t size)
  {
    return h.call(cmd, pid, req, size);
  }
  static EntryNode * entry(DaemonHarness & h, int64_t id)
  {
    auto & topics = h.store().topic_map_;
    const auto t = topics.find(DaemonHarness::topic_key(kTopic));
    if (t == topics.end()) return nullptr;
    const auto it = t->second->topic.entries.find(id);
    return it == t->second->topic.entries.end() ? nullptr : &it->second;
  }

  std::string state;
  std::string medium;
};

}  // namespace

// The predecessor's committed state comes back; the subscriber's own report
// overrides its receive position and references (pushes it never took), and
// a publish whose reply was lost is answered from the persisted entry.
TEST_F(Persist, ReloadsCommittedStateAndReconcilesTheClients)
{
  Child p;
  Child s;
  uint64_t pool_p = 0;
  uint64_t pool_s = 0;
  int32_t pub = -1;
  int32_t sub = -1;
  int64_t e0 = -1;
  int64_t e1 = -1;
  int64_t e2 = -1;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(state), 0U);
    ASSERT_EQ(old.add_process(p.pid, &pool_p), 0);
    ASSERT_EQ(old.add_process(s.pid, &pool_s), 0);
    ASSERT_EQ(old.add_publisher(kTopic, "pub", p.pid, 5, &pub), 0);
    ASSERT_EQ(old.add_subscriber(kTopic, "sub", s.pid, 5, &sub), 0);
    PublishMsgResponse pr{};
    ReceiveMsgResponse rr{};
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x100, &pr, p.pid), 0);
    e0 = pr.entry_id;
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x200, &pr, p.pid), 0);
    e1 = pr.entry_id;
    ASSERT_EQ(old.receive(kTopic, sub, &rr, s.pid), 0);  // takes e0 and e1
    ASSERT_EQ(rr.entry_num, 2U);
    ASSERT_EQ(old.publish(kTopic, pub, pool_p + 0x300, &pr, p.pid), 0);
    e2 = pr.entry_id;  // the publisher never sees this reply
  }

  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(state), 2U);
  // Loaded before anyone restored.
  ASSERT_NE(entry(h, e0), nullptr);
  ASSERT_NE(entry(h, e2), nullptr);
  EXPECT_TRUE(entry(h, e1)->referencing_subscribers.test(static_cast<size_t>(sub)));

  RestoreProcessRequest rp{};
  rp.shm_addr = pool_p;
  ASSERT_EQ(call(h, AGNOCAST_CMD_RESTORE_PROCESS, p.pid, &rp, sizeof(rp)), 0);
  RestorePublisherRequest rpub{};
  DaemonHarness::set_topic(rpub.topic_name, kTopic);
  rpub.publisher_id = pub;
  rpub.qos_depth = 5;
  rpub.last_entry_id = e1;
  ASSERT_EQ(call(h, AGNOCAST_CMD_RESTORE_PUBLISHER, p.pid, &rpub, sizeof(rpub)), 0);
  RestoreEntriesRequest pend{};
  DaemonHarness::set_topic(pend.topic_name, kTopic);
  pend.pubsub_id = pub;
  pend.kind = AGNOCAST_RESTORE_ENTRY_PENDING;
  pend.entry_num = 1;
  pend.entries[0] = AgnocastRestoreEntry{-1, pool_p + 0x300};
  ASSERT_EQ(call(h, AGNOCAST_CMD_RESTORE_ENTRIES, p.pid, &pend, sizeof(pend)), 0);
  ASSERT_EQ(call(h, AGNOCAST_CMD_RESTORE_DONE, p.pid, nullptr, 0), 0);

  // The subscriber had released e0 into a ring the daemon never drained: it
  // reports only e1 as held.
  rp.shm_addr = pool_s;
  ASSERT_EQ(call(h, AGNOCAST_CMD_RESTORE_PROCESS, s.pid, &rp, sizeof(rp)), 0);
  RestoreSubscriberRequest rsub{};
  DaemonHarness::set_topic(rsub.topic_name, kTopic);
  rsub.subscriber_id = sub;
  rsub.qos_depth = 5;
  rsub.latest_received_entry_id = e1;
  ASSERT_EQ(call(h, AGNOCAST_CMD_RESTORE_SUBSCRIBER, s.pid, &rsub, sizeof(rsub)), 0);
  RestoreEntriesRequest held{};
  DaemonHarness::set_topic(held.topic_name, kTopic);
  held.pubsub_id = sub;
  held.kind = AGNOCAST_RESTORE_ENTRY_HELD;
  held.entry_num = 1;
  held.entries[0] = AgnocastRestoreEntry{e1, pool_p + 0x200};
  ASSERT_EQ(call(h, AGNOCAST_CMD_RESTORE_ENTRIES, s.pid, &held, sizeof(held)), 0);
  ASSERT_EQ(call(h, AGNOCAST_CMD_RESTORE_DONE, s.pid, nullptr, 0), 0);

  EXPECT_FALSE(entry(h, e0)->referencing_subscribers.test(static_cast<size_t>(sub)));
  EXPECT_TRUE(entry(h, e1)->referencing_subscribers.test(static_cast<size_t>(sub)));

  PublishMsgResponse pr{};
  ASSERT_EQ(h.publish(kTopic, pub, pool_p + 0x300, &pr, p.pid), 0);
  EXPECT_EQ(pr.entry_id, e2);  // answered from the persisted entry
  ReceiveMsgResponse rr{};
  ASSERT_EQ(h.receive(kTopic, sub, &rr, s.pid), 0);
  ASSERT_EQ(rr.entry_num, 1U);  // e2, once
  EXPECT_EQ(rr.entry_ids[0], e2);
}

// Changes nobody committed are not loaded.
TEST_F(Persist, DropsUncommittedChanges)
{
  Child p;
  uint64_t pool_p = 0;
  int32_t pub = -1;
  {
    DaemonHarness old;
    ASSERT_EQ(old.begin_recovery(state), 0U);
    ASSERT_EQ(old.add_process(p.pid, &pool_p), 0);
    ASSERT_EQ(old.add_publisher(kTopic, "pub", p.pid, 5, &pub), 0);
    // A request that dies before it answers: changes without a commit.
    TopicWrapper * w = old.store().find_topic(DaemonHarness::topic_key(kTopic));
    ASSERT_NE(w, nullptr);
    auto [it, inserted] = w->topic.entries.try_emplace(42);
    ASSERT_TRUE(inserted);
    it->second.entry_id = 42;
    it->second.publisher_id = pub;
    it->second.msg_virtual_address = pool_p + 0x40;
    persist::g_sink->entry_put(*w, it->second);
    persist::g_sink->ref(*w, it->second, 3, true);
  }
  DaemonHarness h;
  ASSERT_EQ(h.begin_recovery(state), 1U);
  EXPECT_EQ(entry(h, 42), nullptr);
  EXPECT_TRUE(h.publisher_exists(kTopic, pub));
}
