// SPDX-License-Identifier: Apache-2.0
//
// Tight-loop publisher for the metadata-consistency harness.
// Counts publish responses' released entries and atomically writes
// published/released/outstanding to a status file.

#include "agnocast/agnocast.hpp"
#include "agnocast_sample_interfaces/msg/static_size_array.hpp"
#include "consistency_common.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

using MessageT = agnocast_sample_interfaces::msg::StaticSizeArray;

namespace
{

void write_status_file(
  const std::string & path, int64_t published, int64_t released, int64_t outstanding)
{
  if (path.empty()) {
    return;
  }
  const std::string tmp = path + ".tmp";
  {
    std::ofstream ofs(tmp);
    ofs << published << ' ' << released << ' ' << outstanding << '\n';
  }
  std::filesystem::rename(tmp, path);
}

}  // namespace

class ConsistencyPublisher : public agnocast::Node
{
  agnocast::Publisher<MessageT>::SharedPtr pub_;
  std::string topic_;
  std::string status_file_;
  std::string sync_dir_;
  int qos_depth_;
  int64_t published_ = 0;
  int64_t released_ = 0;
  bool flushed_ = false;
  int64_t seq_ = 0;

public:
  ConsistencyPublisher() : agnocast::Node("consistency_pub", consistency_node_options())
  {
    topic_ = this->declare_parameter<std::string>("topic", "/consistency");
    qos_depth_ = this->declare_parameter<int>("qos_depth", 10);
    status_file_ = this->declare_parameter<std::string>("status_file", "");
    sync_dir_ = this->declare_parameter<std::string>("sync_dir", "");

    pub_ = this->create_publisher<MessageT>(
      topic_, rclcpp::QoS(rclcpp::KeepLast(static_cast<size_t>(qos_depth_))));
  }

  void write_counts()
  {
    write_status_file(status_file_, published_, released_, published_ - released_);
  }

  void maybe_flush()
  {
    if (flushed_ || !consistency_sync_exists(sync_dir_, "FLUSH")) {
      return;
    }
    write_counts();
    write_sync_marker(sync_dir_, "pub.flushed");
    flushed_ = true;
  }

  bool pause_if_requested()
  {
    if (!consistency_sync_exists(sync_dir_, "PAUSE")) {
      return false;
    }
    write_counts();
    write_sync_marker(sync_dir_, "pub.paused");
    idle_until_shutdown();
    return true;
  }

  void run_loop()
  {
    write_ready_and_wait_start(sync_dir_, "pub");
    while (agnocast::ok()) {
      if (pause_if_requested()) {
        break;
      }
      auto message = pub_->borrow_loaned_message();
      message->id = seq_++;
      pub_->publish(std::move(message));
      published_++;
      released_ += static_cast<int64_t>(agnocast::get_last_publish_released_num());
      if ((published_ & 31) == 0) {
        write_counts();
      }
      // After a publish so KeepLast GC (max 3/call) is reflected in the sample.
      maybe_flush();
    }
    write_counts();
  }
};

int main(int argc, char ** argv)
{
  agnocast::init(argc, argv);
  {
    auto node = std::make_shared<ConsistencyPublisher>();
    node->run_loop();
  }
  agnocast::shutdown();
  return 0;
}
