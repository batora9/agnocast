// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "memory_allocator.hpp"
#include "metadata_store.hpp"
#include "protocol.h"

#include <sys/types.h>

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Implements all Agnocast daemon commands, mirroring agnocast_kmod/agnocast_ioctl.c.
class CommandHandlers
{
public:
  CommandHandlers(MetadataStore & store, MemoryAllocator & allocator);
  ~CommandHandlers();

  CommandHandlers(const CommandHandlers &) = delete;
  CommandHandlers & operator=(const CommandHandlers &) = delete;

  // recv_fd is the descriptor the client attached with SCM_RIGHTS, or -1. Only
  // ADD_SUBSCRIBER and RESTORE_SUBSCRIBER (the subscriber's eventfd) and
  // RESTORE_TOPIC (the topic segment) send one; dispatch closes it for every
  // other command so a client cannot leak descriptors into the daemon.
  void dispatch(
    int client_fd, pid_t client_pid, const RequestHeader & hdr, const void * payload, int recv_fd);

  // Called when a client disconnects its Unix socket.
  void on_client_disconnect(pid_t client_pid);

  // Called when the client process itself has exited (its pidfd became
  // readable), which a socket a forked child still holds would never tell.
  void on_client_exit(pid_t client_pid);

  // Where the pids of the live clients are kept for a successor daemon. Empty
  // (the default, used by the unit tests) keeps nothing.
  void set_state_path(std::string path) { state_path_ = std::move(path); }

  // Daemon restart: the predecessor's clients that are still alive. New
  // registrations wait until each has restored or died, or until timeout_ms.
  // starttimes (pid → /proc start time, from the state file) tells a reused pid
  // from the client itself; a pid missing there counts as alive while it exists.
  void start_recovery(
    const std::vector<pid_t> & expected_pids, int64_t timeout_ms,
    const std::unordered_map<pid_t, uint64_t> & starttimes = {});

  // Blocks until a recovery started by start_recovery() has finished.
  void wait_recovery_end();

private:
  enum class RestoreState { Pending, Restored, Dead };

  // Bookkeeping of a daemon that took over a predecessor's clients.
  struct Recovery
  {
    bool recovered = false;
    bool in_recovery = false;
    bool finishing = false;
    std::unordered_map<pid_t, RestoreState> expected;
    std::unordered_map<pid_t, uint64_t> expected_starttime;
    uint32_t timed_out_num = 0;
    uint32_t adopted_topic_num = 0;
    uint32_t orphan_publisher_num = 0;
    uint32_t orphan_subscriber_num = 0;
    int64_t start_ns = 0;
    int64_t end_ns = 0;
    int64_t deadline_ns = 0;
  };

  MetadataStore & store_;
  MemoryAllocator & allocator_;
  std::string state_path_;

  // Lock ordering: store_.global_mutex_ → recovery_mtx_. Waiters for the end
  // of the recovery take recovery_mtx_ alone.
  std::mutex recovery_mtx_;
  std::condition_variable recovery_cv_;
  Recovery recovery_;
  std::thread recovery_thread_;
  bool stop_recovery_thread_ = false;

  static void send_response(int fd, int32_t error_code);
  // send_fds/send_fd_num are attached with SCM_RIGHTS; the daemon keeps its own copies.
  static void send_response(
    int fd, int32_t error_code, const void * payload, uint32_t payload_size,
    const int * send_fds = nullptr, uint32_t send_fd_num = 0);

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
  void handle_get_subscriber_eventfds(int fd, pid_t pid, const void * payload);
  void handle_get_publisher_shm_infos(int fd, pid_t pid, const void * payload);

  void handle_restore_process(int fd, pid_t pid, const void * payload);
  void handle_restore_mappings(int fd, pid_t pid, const void * payload);
  void handle_restore_topic(int fd, pid_t pid, const void * payload, int shm_fd);
  void handle_restore_publisher(int fd, pid_t pid, const void * payload);
  void handle_restore_subscriber(int fd, pid_t pid, const void * payload, int notify_eventfd);
  void handle_restore_done(int fd, pid_t pid);
  void handle_debug_check_topic(int fd, pid_t pid, const void * payload);
  void handle_get_recovery_status(int fd);
  void handle_report_anomaly(int fd, pid_t pid, const void * payload);
  void handle_get_topic_segment(int fd, pid_t pid, const void * payload);

  // Replaces the topic's segment with one rebuilt from the registry (and what
  // can be trusted of the old one, held in old_copy), in the given mode. The
  // caller holds global_mutex_ (shared) and topic_rwsem (unique), and the old
  // segment's mutex when it could take it. Returns false when no new segment
  // could be created.
  bool rotate_topic_locked(TopicWrapper * wrapper, AgnocastTopicShm * old_copy, uint32_t mode);

  // Removes a process's publishers and subscribers from the topic's registry
  // and bars it from the topic. Caller holds topic_rwsem (unique).
  void quarantine_locked(TopicWrapper * wrapper, pid_t pid);

  // Whether the client the state file named is still the same process.
  bool expected_alive_locked(pid_t pid) const;

  void process_exit_cleanup(pid_t pid);

  // Writes the pids of the live clients to state_path_. Caller holds
  // global_mutex_.
  void persist_state_locked();

  // Holds a new registration back while the predecessor's clients restore.
  // Returns at once for a client that is itself restoring.
  void wait_for_recovery_before_registration(pid_t pid);

  // Removes the ids in every segment that no client claimed and whose process
  // is_settled() says will not claim them any more. Caller holds global_mutex_
  // (unique).
  void reconcile_topics_locked(const std::function<bool(pid_t)> & is_settled);

  void recovery_thread_main();
  // Ends the recovery: reconciles, cleans up after the dead, and lets the
  // waiting registrations through.
  void finish_recovery();
};
