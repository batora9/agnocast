// SPDX-License-Identifier: Apache-2.0
//
// One-shot query of the public membership APIs. Used to wait until the
// 1-publisher / 4-subscriber topology is up, to wait until a crashed
// subscriber leaves membership, and to snapshot post-cleanup state.

#include "agnocast/agnocast.hpp"
#include "agnocast/agnocast_ipc.hpp"
#include "consistency_common.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{

struct Endpoint
{
  std::string node_name;
  uint32_t qos_depth = 0;
  bool qos_is_transient_local = false;
  bool qos_is_reliable = false;
};

struct Snapshot
{
  std::vector<std::string> topics;
  std::vector<Endpoint> publishers;
  std::vector<Endpoint> subscribers;
  uint32_t publisher_num = 0;
  uint32_t subscriber_num = 0;
};

std::string json_escape(const std::string & s)
{
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if (c == '"' || c == '\\') {
      out.push_back('\\');
    }
    out.push_back(c);
  }
  return out;
}

std::vector<Endpoint> query_endpoints(const std::string & topic, bool publishers)
{
  std::vector<agnocast::topic_info_ret> buf(MAX_TOPIC_INFO_RET_NUM);
  union agnocast::ioctl_topic_info_args args = {};
  args.topic_name = {topic.c_str(), topic.size()};
  args.topic_info_ret_buffer_addr = reinterpret_cast<uint64_t>(buf.data());
  args.topic_info_ret_buffer_size = MAX_TOPIC_INFO_RET_NUM;
  args.domain_id = 0;

  const int rc = publishers ? agnocast::agnocast_ipc_get_topic_publisher_info(&args)
                            : agnocast::agnocast_ipc_get_topic_subscriber_info(&args);
  if (rc < 0) {
    std::fprintf(
      stderr, "consistency_observer: %s info failed: %s\n", publishers ? "publisher" : "subscriber",
      std::strerror(errno));
    return {};
  }

  std::vector<Endpoint> out;
  for (uint32_t i = 0; i < args.ret_topic_info_ret_num; i++) {
    if (buf[i].is_bridge) {
      continue;
    }
    Endpoint e;
    e.node_name = buf[i].node_name;
    e.qos_depth = buf[i].qos_depth;
    e.qos_is_transient_local = buf[i].qos_is_transient_local;
    e.qos_is_reliable = buf[i].qos_is_reliable;
    out.push_back(std::move(e));
  }
  std::sort(out.begin(), out.end(), [](const Endpoint & a, const Endpoint & b) {
    return a.node_name < b.node_name;
  });
  return out;
}

Snapshot query_snapshot(const std::string & topic)
{
  Snapshot snap;

  std::vector<char> name_buf(static_cast<size_t>(MAX_TOPIC_NUM) * TOPIC_NAME_BUFFER_SIZE);
  union agnocast::ioctl_topic_list_args list_args = {};
  list_args.topic_name_buffer_addr = reinterpret_cast<uint64_t>(name_buf.data());
  list_args.domain_id_buffer_addr = 0;
  list_args.topic_name_buffer_size = MAX_TOPIC_NUM;
  if (agnocast::agnocast_ipc_get_topic_list(&list_args) == 0) {
    for (uint32_t i = 0; i < list_args.ret_topic_num; i++) {
      snap.topics.emplace_back(name_buf.data() + static_cast<size_t>(i) * TOPIC_NAME_BUFFER_SIZE);
    }
  }
  std::sort(snap.topics.begin(), snap.topics.end());

  snap.publishers = query_endpoints(topic, true);
  snap.subscribers = query_endpoints(topic, false);

  union agnocast::ioctl_get_publisher_num_args pub_num = {};
  pub_num.topic_name = {topic.c_str(), topic.size()};
  if (agnocast::agnocast_ipc_get_publisher_num(&pub_num) == 0) {
    snap.publisher_num = pub_num.ret_publisher_num;
  }

  union agnocast::ioctl_get_subscriber_num_args sub_num = {};
  sub_num.topic_name = {topic.c_str(), topic.size()};
  if (agnocast::agnocast_ipc_get_subscriber_num(&sub_num) == 0) {
    snap.subscriber_num = sub_num.ret_other_process_subscriber_num;
  }

  return snap;
}

bool same_node_name(const std::string & a, const std::string & b)
{
  const std::string_view aa = (!a.empty() && a.front() == '/') ? std::string_view(a).substr(1) : a;
  const std::string_view bb = (!b.empty() && b.front() == '/') ? std::string_view(b).substr(1) : b;
  return aa == bb;
}

bool contains_node(const std::vector<Endpoint> & eps, const std::string & name)
{
  return std::any_of(
    eps.begin(), eps.end(), [&](const Endpoint & e) { return same_node_name(e.node_name, name); });
}

void write_endpoints(std::ostream & os, const char * key, const std::vector<Endpoint> & eps)
{
  os << "  \"" << key << "\": [\n";
  for (size_t i = 0; i < eps.size(); i++) {
    const Endpoint & e = eps[i];
    os << "    {\"node_name\": \"" << json_escape(e.node_name)
       << "\", \"qos_depth\": " << e.qos_depth
       << ", \"qos_is_transient_local\": " << (e.qos_is_transient_local ? "true" : "false")
       << ", \"qos_is_reliable\": " << (e.qos_is_reliable ? "true" : "false") << "}";
    if (i + 1 < eps.size()) {
      os << ",";
    }
    os << "\n";
  }
  os << "  ]";
}

void write_snapshot(std::ostream & os, const Snapshot & snap)
{
  os << "{\n";
  os << "  \"topics\": [";
  for (size_t i = 0; i < snap.topics.size(); i++) {
    if (i > 0) {
      os << ", ";
    }
    os << "\"" << json_escape(snap.topics[i]) << "\"";
  }
  os << "],\n";
  write_endpoints(os, "publishers", snap.publishers);
  os << ",\n";
  write_endpoints(os, "subscribers", snap.subscribers);
  os << ",\n";
  os << "  \"publisher_num\": " << snap.publisher_num << ",\n";
  os << "  \"subscriber_num\": " << snap.subscriber_num << "\n";
  os << "}\n";
}

}  // namespace

class ConsistencyObserver : public agnocast::Node
{
public:
  ConsistencyObserver() : agnocast::Node("consistency_observer", consistency_node_options()) {}
};

int main(int argc, char ** argv)
{
  agnocast::init(argc, argv);
  auto node = std::make_shared<ConsistencyObserver>();

  const std::string topic = node->declare_parameter<std::string>("topic", "/consistency");
  const std::string output = node->declare_parameter<std::string>("output", "");
  const std::string wait_absent_subscriber =
    node->declare_parameter<std::string>("wait_absent_subscriber", "");
  const int wait_publishers = node->declare_parameter<int>("wait_publishers", 0);
  const int wait_subscribers = node->declare_parameter<int>("wait_subscribers", 0);
  const int wait_timeout_sec = node->declare_parameter<int>("wait_timeout_sec", 15);

  Snapshot snap;
  if (wait_publishers > 0 || wait_subscribers > 0 || !wait_absent_subscriber.empty()) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                            std::chrono::seconds(wait_timeout_sec));
    bool matched = false;
    while (agnocast::ok() && std::chrono::steady_clock::now() < deadline) {
      snap = query_snapshot(topic);
      const int pub_n = static_cast<int>(snap.publishers.size());
      const int sub_n = static_cast<int>(snap.subscribers.size());
      const bool absent_ok =
        wait_absent_subscriber.empty() || !contains_node(snap.subscribers, wait_absent_subscriber);
      if (
        (wait_publishers <= 0 || pub_n == wait_publishers) &&
        (wait_subscribers <= 0 || sub_n == wait_subscribers) && absent_ok) {
        matched = true;
        break;
      }
      std::this_thread::sleep_for(20ms);
    }
    if (!matched) {
      std::fprintf(
        stderr,
        "consistency_observer: timed out waiting for %d pub / %d sub"
        " (absent '%s'; last saw %zu / %zu)\n",
        wait_publishers, wait_subscribers, wait_absent_subscriber.c_str(), snap.publishers.size(),
        snap.subscribers.size());
      return 1;
    }
  } else {
    snap = query_snapshot(topic);
  }

  if (output.empty()) {
    write_snapshot(std::cout, snap);
  } else {
    std::ofstream ofs(output);
    write_snapshot(ofs, snap);
  }

  node.reset();
  agnocast::shutdown();
  return 0;
}
