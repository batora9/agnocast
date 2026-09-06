// SPDX-License-Identifier: Apache-2.0
//
// role:=victim — tight receive_msg + release_sub_ref loop (no executor).
// role:=holder — keeps the latest message and periodically touches it.
// After SNAP, a DROP marker makes holders reset last_ so publisher-crash
// leak can see whether the dead publisher record then disappears.

#include "agnocast/agnocast.hpp"
#include "agnocast/agnocast_ipc.hpp"
#include "agnocast/node/agnocast_only_single_threaded_executor.hpp"
#include "agnocast_sample_interfaces/msg/static_size_array.hpp"
#include "consistency_common.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

using MessageT = agnocast_sample_interfaces::msg::StaticSizeArray;
using namespace std::chrono_literals;

class ConsistencySubscriber : public agnocast::Node
{
  agnocast::Subscription<MessageT>::SharedPtr sub_;
  agnocast::TimerBase::SharedPtr touch_timer_;
  agnocast::ipc_shared_ptr<const MessageT> last_;
  agnocast::AgnocastOnlyExecutor * executor_{nullptr};
  std::string topic_;
  std::string role_;
  std::string sync_dir_;
  int qos_depth_;
  int index_{0};

  void receive_and_release_once()
  {
    std::array<agnocast::publisher_shm_info, MAX_PUBLISHER_NUM> pub_shm_infos{};
    union agnocast::ioctl_receive_msg_args receive_args = {};
    receive_args.topic_name = {sub_->get_topic_name().c_str(), sub_->get_topic_name().size()};
    receive_args.subscriber_id = sub_->get_id();
    receive_args.pub_shm_info_addr = reinterpret_cast<uint64_t>(pub_shm_infos.data());
    receive_args.pub_shm_info_size = MAX_PUBLISHER_NUM;

    {
      std::lock_guard<std::mutex> lock(agnocast::mmap_mtx);
      while (agnocast::agnocast_ipc_receive_msg(&receive_args) < 0) {
        if (errno == EINTR) {
          if (!agnocast::ok()) {
            return;
          }
          continue;
        }
        std::fprintf(stderr, "receive_msg failed: %s\n", std::strerror(errno));
        return;
      }
      for (uint32_t i = 0; i < receive_args.ret_pub_shm_num; i++) {
        agnocast::map_read_only_area(
          pub_shm_infos[i].pid, pub_shm_infos[i].shm_addr, pub_shm_infos[i].shm_size);
      }
    }

    for (uint16_t i = 0; i < receive_args.ret_entry_num; i++) {
      MessageT * ptr = reinterpret_cast<MessageT *>(receive_args.ret_entry_addrs[i]);
      agnocast::ipc_shared_ptr<const MessageT> msg(
        ptr, sub_->get_topic_name(), sub_->get_id(), receive_args.ret_entry_ids[i]);
      (void)msg;
    }

    if (receive_args.ret_call_again && agnocast::ok()) {
      receive_and_release_once();
    }
  }

  void maybe_pause_executor()
  {
    if (executor_ == nullptr || !consistency_sync_exists(sync_dir_, "SNAP")) {
      return;
    }
    write_sync_marker(sync_dir_, "sub_" + std::to_string(index_) + ".paused");
    executor_->cancel();
  }

public:
  static std::string node_name_from_env()
  {
    const char * idx = std::getenv("CONSISTENCY_SUB_INDEX");
    const int i = idx != nullptr ? std::atoi(idx) : 0;
    return "consistency_sub_" + std::to_string(i);
  }

  ConsistencySubscriber() : agnocast::Node(node_name_from_env(), consistency_node_options())
  {
    topic_ = this->declare_parameter<std::string>("topic", "/consistency");
    role_ = this->declare_parameter<std::string>("role", "holder");
    qos_depth_ = this->declare_parameter<int>("qos_depth", 10);
    sync_dir_ = this->declare_parameter<std::string>("sync_dir", "");
    index_ = this->declare_parameter<int>("subscriber_index", 0);

    const rclcpp::QoS qos(rclcpp::KeepLast(static_cast<size_t>(qos_depth_)));

    if (role_ == "victim") {
      sub_ = this->create_subscription<MessageT>(
        topic_, qos, [](const agnocast::ipc_shared_ptr<const MessageT> &) {});
    } else {
      sub_ = this->create_subscription<MessageT>(
        topic_, qos, [this](const agnocast::ipc_shared_ptr<const MessageT> & msg) {
          last_ = msg;
          if (last_) {
            (void)last_->id;
          }
          maybe_pause_executor();
        });
      touch_timer_ = this->create_wall_timer(20ms, [this]() {
        if (last_) {
          (void)last_->id;
        }
        maybe_pause_executor();
      });
    }
  }

  bool is_victim() const { return role_ == "victim"; }

  void set_executor(agnocast::AgnocastOnlyExecutor * executor) { executor_ = executor; }

  void wait_for_start() const
  {
    write_ready_and_wait_start(sync_dir_, "sub_" + std::to_string(index_));
  }

  void run_victim_loop()
  {
    while (agnocast::ok()) {
      receive_and_release_once();
    }
  }

  void maybe_release_held()
  {
    if (!consistency_sync_exists(sync_dir_, "RELEASE_HELD")) {
      return;
    }
    const std::string marker = "sub_" + std::to_string(index_) + ".released";
    if (consistency_sync_exists(sync_dir_, marker)) {
      return;
    }
    last_.reset();
    write_sync_marker(sync_dir_, marker);
  }

  void maybe_drop_held()
  {
    if (!consistency_sync_exists(sync_dir_, "DROP")) {
      return;
    }
    const std::string marker = "sub_" + std::to_string(index_) + ".dropped";
    if (consistency_sync_exists(sync_dir_, marker)) {
      return;
    }
    // release_sub_ref only clears the bit. Orphaned publisher records are
    // reclaimed when the subscriber is removed (same path as SIGINT destructor).
    touch_timer_.reset();
    last_.reset();
    sub_.reset();
    write_sync_marker(sync_dir_, marker);
  }

  void idle_after_pause()
  {
    while (agnocast::ok()) {
      maybe_release_held();
      maybe_drop_held();
      if (last_) {
        (void)last_->id;
      }
      std::this_thread::sleep_for(50ms);
    }
  }
};

int main(int argc, char ** argv)
{
  agnocast::init(argc, argv);
  auto node = std::make_shared<ConsistencySubscriber>();
  node->wait_for_start();
  if (node->is_victim()) {
    node->run_victim_loop();
    node.reset();
    agnocast::shutdown();
    return 0;
  }

  auto executor = std::make_shared<agnocast::AgnocastOnlySingleThreadedExecutor>(20);
  node->set_executor(executor.get());
  executor->add_node(node);
  executor->spin();
  node->idle_after_pause();
  node.reset();
  agnocast::shutdown();
  return 0;
}
