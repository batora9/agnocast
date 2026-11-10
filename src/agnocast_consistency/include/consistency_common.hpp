// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "agnocast/node/agnocast_context.hpp"
#include "rclcpp/rclcpp.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <thread>

inline rclcpp::NodeOptions consistency_node_options()
{
  rclcpp::NodeOptions options;
  options.start_parameter_services(false);
  options.start_parameter_event_publisher(false);
  return options;
}

inline bool consistency_sync_exists(const std::string & sync_dir, const std::string & name)
{
  if (sync_dir.empty()) {
    return false;
  }
  std::error_code ec;
  return std::filesystem::exists(std::filesystem::path(sync_dir) / name, ec);
}

inline void write_sync_marker(const std::string & sync_dir, const std::string & name)
{
  if (sync_dir.empty()) {
    return;
  }
  std::ofstream ofs(std::filesystem::path(sync_dir) / name);
}

inline void write_ready_and_wait_start(const std::string & sync_dir, const std::string & name)
{
  if (sync_dir.empty()) {
    return;
  }
  write_sync_marker(sync_dir, name + ".ready");
  while (agnocast::ok() && !consistency_sync_exists(sync_dir, "START")) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

inline void idle_until_shutdown()
{
  while (agnocast::ok()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

#ifdef AGNOCAST_USE_DAEMON
#include "agnocast/agnocast_ipc.hpp"
#endif

// Writes what this process's data plane ran into (daemon backend) as one JSON
// line, atomically. No-op with the kernel module or an empty path.
inline void write_client_stats(const std::string & path)
{
#ifdef AGNOCAST_USE_DAEMON
  if (path.empty()) {
    return;
  }
  const agnocast::DaemonClientStats st = agnocast::agnocast_ipc_client_stats();
  const std::string tmp = path + ".tmp";
  {
    std::ofstream ofs(tmp);
    ofs << "{\"anomaly_reports\": " << st.anomaly_reports
        << ", \"segment_refetches\": " << st.segment_refetches
        << ", \"rollbacks\": " << st.rollbacks << ", \"stall_reports\": " << st.stall_reports
        << ", \"bad_entries\": " << st.bad_entries << ", \"peer_reclaims\": " << st.peer_reclaims
        << ", \"daemon_path_calls\": " << st.daemon_path_calls
        << ", \"shm_protected\": " << (st.shm_protected ? "true" : "false") << "}\n";
  }
  std::error_code ec;
  std::filesystem::rename(tmp, path, ec);
#else
  (void)path;
#endif
}
