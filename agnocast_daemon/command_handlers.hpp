// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "memory_allocator.hpp"
#include "metadata_store.hpp"
#include "protocol.h"

#include <sys/types.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Implements all Agnocast daemon commands, mirroring agnocast_kmod/agnocast_ioctl.c.
class CommandHandlers
{
public:
  CommandHandlers(MetadataStore & store, MemoryAllocator & allocator);
  ~CommandHandlers();
  CommandHandlers(const CommandHandlers &) = delete;
  CommandHandlers & operator=(const CommandHandlers &) = delete;

  // Keeps the pids of the live clients in state_path (protocol.h, "Daemon
  // restart recovery"). When the file names clients of a predecessor that are
  // still alive, the grace period starts: only those clients are served,
  // restoring, until each has restored or is known to be dead. Returns how
  // many it waits for. Without a call, nothing is persisted (the unit tests).
  size_t begin_recovery(const std::string & state_path);

  // recv_fd is the descriptor the client attached with SCM_RIGHTS, or -1. Only
  // AGNOCAST_CMD_ADD_SUBSCRIBER (the subscriber's notification eventfd) and
  // AGNOCAST_CMD_ATTACH_SUBSCRIBER_CHANNEL (the channel memfd) send one;
  // dispatch closes it for every other command so a client cannot leak
  // descriptors into the daemon.
  void dispatch(
    int client_fd, pid_t client_pid, const RequestHeader & hdr, const void * payload, int recv_fd);

  // Called when a client disconnects its Unix socket.
  void on_client_disconnect(pid_t client_pid);

private:
  MetadataStore & store_;
  MemoryAllocator & allocator_;

  static void send_response(int fd, int32_t error_code);
  static void send_response(
    int fd, int32_t error_code, const void * payload, uint32_t payload_size);

  void handle_get_version(int fd);
  void handle_add_process(int fd, pid_t pid, const void * payload);
  void handle_add_subscriber(int fd, pid_t pid, const void * payload, int notify_eventfd);
  void handle_add_publisher(int fd, pid_t pid, const void * payload);
  void handle_release_sub_ref(int fd, pid_t pid, const void * payload);
  void handle_publish_msg(int fd, pid_t pid, const void * payload);
  void handle_receive_msg(int fd, pid_t pid, const void * payload);
  void handle_take_msg(int fd, pid_t pid, const void * payload);
  void handle_get_subscriber_num(int fd, pid_t pid, const void * payload);
  void handle_get_exit_process(int fd);
  void handle_get_subscriber_qos(int fd, pid_t pid, const void * payload);
  void handle_get_publisher_qos(int fd, pid_t pid, const void * payload);
  void handle_add_bridge(int fd, pid_t pid, const void * payload);
  void handle_remove_bridge(int fd, pid_t pid, const void * payload);
  void handle_get_publisher_num(int fd, pid_t pid, const void * payload);
  void handle_remove_subscriber(int fd, pid_t pid, const void * payload);
  void handle_remove_publisher(int fd, pid_t pid, const void * payload);
  void handle_check_and_request_bridge_shutdown(int fd, pid_t pid);
  void handle_get_topic_list(int fd);
  void handle_get_topic_subscriber_info(int fd, pid_t pid, const void * payload);
  void handle_get_topic_publisher_info(int fd, pid_t pid, const void * payload);
  void handle_get_node_subscriber_topics(int fd, pid_t pid, const void * payload);
  void handle_get_node_publisher_topics(int fd, pid_t pid, const void * payload);
  void handle_set_ros2_subscriber_num(int fd, pid_t pid, const void * payload);
  void handle_set_ros2_publisher_num(int fd, pid_t pid, const void * payload);
  void handle_notify_bridge_shutdown(int fd, pid_t pid);
  void handle_attach_subscriber_channel(int fd, pid_t pid, const void * payload, int channel_fd);

  void process_exit_cleanup(pid_t pid);

  // ---- Daemon restart recovery ----
  struct Claim
  {
    TopicKey key;
    int32_t subscriber_id;
    int64_t entry_id;
    uint64_t addr;
  };
  struct Recovery
  {
    // Clients of the predecessor not yet settled (restored or dead): pid →
    // start time.
    std::unordered_map<pid_t, uint64_t> expected;
    std::unordered_set<pid_t> restoring;  // between RESTORE_PROCESS and RESTORE_DONE
    std::unordered_set<pid_t> restored;
    std::unordered_set<pid_t> dead;  // expected ones known to be dead
    std::vector<Claim> claims;       // references reported, resolved at the end
    // Entries subscribers were recently handed: topic → entry id → address.
    std::unordered_map<TopicKey, std::unordered_map<int64_t, uint64_t>, TopicKeyHash> delivered;
    std::unordered_map<uint64_t, pid_t> pool_owner;  // pool address → owner, as reported
    int64_t begin_ns = 0;
    GetRecoveryStatusResponse status{};
  };

  // Waits out the grace period unless the command may run during it. Called
  // first in dispatch; one atomic load outside a recovery.
  void gate_on_recovery(pid_t pid, uint32_t command);
  // Caller holds global_mutex_ (unique). Writes the live clients' pids.
  void persist_state_locked();
  // Ends the grace period once every expected client is settled.
  void try_finish_recovery();
  void finish_recovery_locked();  // global_mutex_ (unique) and recovery_mtx_ held
  void recovery_watchdog_main();
  void forget_restoring_client(pid_t pid);

  void handle_restore_process(int fd, pid_t pid, const void * payload);
  void handle_restore_mappings(int fd, pid_t pid, const void * payload);
  void handle_restore_publisher(int fd, pid_t pid, const void * payload);
  void handle_restore_subscriber(int fd, pid_t pid, const void * payload, int notify_eventfd);
  void handle_restore_entries(int fd, pid_t pid, const void * payload);
  void handle_restore_done(int fd, pid_t pid);
  void handle_get_recovery_status(int fd);

  std::string state_path_;  // empty: nothing persisted
  std::atomic<bool> recovering_{false};
  std::mutex recovery_mtx_;  // after global_mutex_ when both are held
  std::condition_variable recovery_cv_;
  Recovery recovery_;
  bool stop_watchdog_ = false;
  std::thread watchdog_;
};
