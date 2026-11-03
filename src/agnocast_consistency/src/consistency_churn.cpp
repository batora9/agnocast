// SPDX-License-Identifier: Apache-2.0
//
// Keeps the daemon busy with registration changes on the experiment topic, so
// that a daemon crash is likely to land inside one of them.
//
// Each round it registers a subscriber, receives a few messages through the
// raw receive call and keeps their references, then removes the subscriber
// without releasing them (the daemon clears them as part of the removal), and
// registers and removes a publisher. SIGINT ends it after the current round.

#include "agnocast/agnocast.hpp"
#include "agnocast/agnocast_ipc.hpp"
#include "agnocast_sample_interfaces/msg/static_size_array.hpp"
#include "consistency_common.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

using MessageT = agnocast_sample_interfaces::msg::StaticSizeArray;

class ConsistencyChurn : public agnocast::Node
{
  std::string topic_;
  int qos_depth_;
  int interval_ms_;
  int64_t rounds_{0};

  void receive_and_keep(const agnocast::Subscription<MessageT>::SharedPtr & sub)
  {
    std::array<agnocast::publisher_shm_info, MAX_PUBLISHER_NUM> pub_shm_infos{};
    union agnocast::ioctl_receive_msg_args args = {};
    const std::string topic_name = sub->get_topic_name();
    args.topic_name = {topic_name.c_str(), topic_name.size()};
    args.subscriber_id = sub->get_id();
    args.pub_shm_info_addr = reinterpret_cast<uint64_t>(pub_shm_infos.data());
    args.pub_shm_info_size = MAX_PUBLISHER_NUM;

    std::lock_guard<std::mutex> lock(agnocast::mmap_mtx);
    if (agnocast::agnocast_ipc_receive_msg(&args) < 0) {
      std::fprintf(stderr, "consistency_churn: receive_msg failed: %s\n", std::strerror(errno));
      return;
    }
    for (uint32_t i = 0; i < args.ret_pub_shm_num; i++) {
      agnocast::map_read_only_area(
        pub_shm_infos[i].pid, pub_shm_infos[i].shm_addr, pub_shm_infos[i].shm_size);
    }
    // The references are deliberately not released.
  }

public:
  ConsistencyChurn() : agnocast::Node("consistency_churn", consistency_node_options())
  {
    topic_ = this->declare_parameter<std::string>("topic", "/consistency");
    qos_depth_ = this->declare_parameter<int>("qos_depth", 10);
    interval_ms_ = this->declare_parameter<int>("interval_ms", 2);
  }

  void run()
  {
    const rclcpp::QoS qos(rclcpp::KeepLast(static_cast<size_t>(qos_depth_)));
    while (agnocast::ok()) {
      {
        auto sub = this->create_subscription<MessageT>(
          topic_, qos, [](const agnocast::ipc_shared_ptr<const MessageT> &) {});
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
        receive_and_keep(sub);
      }
      {
        auto pub = this->create_publisher<MessageT>(topic_, qos);
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
      }
      rounds_++;
    }
    std::fprintf(stderr, "consistency_churn: %lld rounds\n", static_cast<long long>(rounds_));
  }
};

int main(int argc, char ** argv)
{
  agnocast::init(argc, argv);
  {
    auto node = std::make_shared<ConsistencyChurn>();
    node->run();
  }
  agnocast::shutdown();
  return 0;
}
