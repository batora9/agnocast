// SPDX-License-Identifier: Apache-2.0
//
// A second publisher on the topic that misbehaves, for the recovery-flow
// evaluation (daemon backend only).
//
//   mode:=publish      publishes in a tight loop; combined with
//                      AGNOCAST_CLIENT_FAULT_POINT it dies (or stops) holding
//                      the segment's mutex in the middle of a publish
//   mode:=bad_address  after fault_delay_ms, publishes entries whose address
//                      lies outside its pool (a corrupted pointer), every 1 ms
//                      until the daemon bars it (at most inject_ms), then idles
//   mode:=scribble     after fault_delay_ms, writes garbage over the segment's
//                      bookkeeping without the mutex (a stray write), then idles
//
// The two debug modes need AGNOCAST_ALLOW_DEBUG_FAULTS=1. Markers in sync_dir:
// rogue.ready, rogue.injected (or rogue.inject_failed).

#include "agnocast/agnocast.hpp"
#include "agnocast_sample_interfaces/msg/static_size_array.hpp"
#include "consistency_common.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

using MessageT = agnocast_sample_interfaces::msg::StaticSizeArray;

class ConsistencyRogue : public agnocast::Node
{
  agnocast::Publisher<MessageT>::SharedPtr pub_;
  std::string topic_;
  std::string mode_;
  std::string sync_dir_;
  std::string stats_file_;
  int fault_delay_ms_;
  int inject_ms_;
  int64_t seq_ = 0;

public:
  ConsistencyRogue() : agnocast::Node("consistency_rogue", consistency_node_options())
  {
    topic_ = this->declare_parameter<std::string>("topic", "/consistency");
    mode_ = this->declare_parameter<std::string>("mode", "publish");
    sync_dir_ = this->declare_parameter<std::string>("sync_dir", "");
    stats_file_ = this->declare_parameter<std::string>("stats_file", "");
    fault_delay_ms_ = this->declare_parameter<int>("fault_delay_ms", 200);
    inject_ms_ = this->declare_parameter<int>("inject_ms", 2000);
    const int qos_depth = this->declare_parameter<int>("qos_depth", 10);
    pub_ = this->create_publisher<MessageT>(
      topic_, rclcpp::QoS(rclcpp::KeepLast(static_cast<size_t>(qos_depth))));
  }

  void run()
  {
    write_ready_and_wait_start(sync_dir_, "rogue");
    if (mode_ == "publish") {
      while (agnocast::ok()) {
        auto message = pub_->borrow_loaned_message();
        message->id = seq_++;
        pub_->publish(std::move(message));
        if ((seq_ & 1023) == 0) {
          write_client_stats(stats_file_);
        }
      }
      return;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(fault_delay_ms_));
#ifdef AGNOCAST_USE_DAEMON
    const int kind = mode_ == "bad_address" ? agnocast::AGNOCAST_DEBUG_FAULT_BAD_ADDRESS
                                            : agnocast::AGNOCAST_DEBUG_FAULT_SCRIBBLE;
    // Written first: with a protection key the scribble faults and kills us.
    write_sync_marker(sync_dir_, "rogue.injecting");
    if (agnocast::agnocast_ipc_debug_inject_fault(topic_, pub_->get_id(), kind) == 0) {
      write_sync_marker(sync_dir_, "rogue.injected");
    } else {
      std::fprintf(stderr, "inject %s failed: %s\n", mode_.c_str(), std::strerror(errno));
      write_sync_marker(sync_dir_, "rogue.inject_failed");
    }
    // A broken publisher keeps publishing broken pointers. A single one may
    // fall outside every reader's depth window before anyone looks (and is
    // then harmless), so keep at it until the daemon stops us (the segment is
    // replaced and this process is barred), at most inject_ms.
    if (kind == agnocast::AGNOCAST_DEBUG_FAULT_BAD_ADDRESS) {
      const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(inject_ms_);
      int injected = 1;
      while (agnocast::ok() && std::chrono::steady_clock::now() < until &&
             agnocast::agnocast_ipc_debug_inject_fault(topic_, pub_->get_id(), kind) == 0) {
        injected++;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      std::fprintf(stderr, "injected %d bad entries\n", injected);
    }
#else
    write_sync_marker(sync_dir_, "rogue.inject_failed");
#endif
    while (agnocast::ok()) {
      write_client_stats(stats_file_);
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
};

int main(int argc, char ** argv)
{
  agnocast::init(argc, argv);
  {
    auto node = std::make_shared<ConsistencyRogue>();
    node->run();
  }
  agnocast::shutdown();
  return 0;
}
