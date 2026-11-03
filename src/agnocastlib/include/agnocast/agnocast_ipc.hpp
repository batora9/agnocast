// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "agnocast/agnocast_ioctl.hpp"

#ifndef AGNOCAST_USE_DAEMON
#include <sys/ioctl.h>
#else
#include <sys/types.h>

#include <cstdint>
#include <string>
#endif

namespace agnocast
{

extern int agnocast_fd;

#ifndef AGNOCAST_USE_DAEMON
// ============================================================
// Kernel module mode: thin inline wrappers around ioctl()
// ============================================================

inline int agnocast_ipc_get_version(struct ioctl_get_version_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_VERSION_CMD, args);
}
inline int agnocast_ipc_add_process(union ioctl_add_process_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_ADD_PROCESS_CMD, args);
}
inline int agnocast_ipc_add_publisher(union ioctl_add_publisher_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_ADD_PUBLISHER_CMD, args);
}
inline int agnocast_ipc_add_subscriber(union ioctl_add_subscriber_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_ADD_SUBSCRIBER_CMD, args);
}
inline int agnocast_ipc_publish_msg(union ioctl_publish_msg_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_PUBLISH_MSG_CMD, args);
}
inline int agnocast_ipc_receive_msg(union ioctl_receive_msg_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_RECEIVE_MSG_CMD, args);
}
inline int agnocast_ipc_take_msg(union ioctl_take_msg_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_TAKE_MSG_CMD, args);
}
inline int agnocast_ipc_release_sub_ref(struct ioctl_update_entry_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_RELEASE_SUB_REF_CMD, args);
}
inline int agnocast_ipc_remove_publisher(struct ioctl_remove_publisher_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_REMOVE_PUBLISHER_CMD, args);
}
inline int agnocast_ipc_remove_subscriber(struct ioctl_remove_subscriber_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_REMOVE_SUBSCRIBER_CMD, args);
}
inline int agnocast_ipc_get_subscriber_num(union ioctl_get_subscriber_num_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_SUBSCRIBER_NUM_CMD, args);
}
inline int agnocast_ipc_get_publisher_num(union ioctl_get_publisher_num_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_PUBLISHER_NUM_CMD, args);
}
inline int agnocast_ipc_get_exit_process(struct ioctl_get_exit_process_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_EXIT_PROCESS_CMD, args);
}
inline int agnocast_ipc_get_subscriber_qos(struct ioctl_get_subscriber_qos_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_SUBSCRIBER_QOS_CMD, args);
}
inline int agnocast_ipc_get_publisher_qos(struct ioctl_get_publisher_qos_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_PUBLISHER_QOS_CMD, args);
}
inline int agnocast_ipc_add_bridge(struct ioctl_add_bridge_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_ADD_BRIDGE_CMD, args);
}
inline int agnocast_ipc_remove_bridge(struct ioctl_remove_bridge_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_REMOVE_BRIDGE_CMD, args);
}
inline int agnocast_ipc_check_and_request_bridge_shutdown(
  struct ioctl_check_and_request_bridge_shutdown_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_CHECK_AND_REQUEST_BRIDGE_SHUTDOWN_CMD, args);
}
inline int agnocast_ipc_notify_bridge_shutdown()
{
  return ioctl(agnocast_fd, AGNOCAST_NOTIFY_BRIDGE_SHUTDOWN_CMD);
}
inline int agnocast_ipc_set_ros2_subscriber_num(struct ioctl_set_ros2_subscriber_num_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_SET_ROS2_SUBSCRIBER_NUM_CMD, args);
}
inline int agnocast_ipc_set_ros2_publisher_num(struct ioctl_set_ros2_publisher_num_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_SET_ROS2_PUBLISHER_NUM_CMD, args);
}
inline int agnocast_ipc_get_topic_list(union ioctl_topic_list_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_TOPIC_LIST_CMD, args);
}
inline int agnocast_ipc_get_topic_subscriber_info(union ioctl_topic_info_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_TOPIC_SUBSCRIBER_INFO_CMD, args);
}
inline int agnocast_ipc_get_topic_publisher_info(union ioctl_topic_info_args * args)
{
  return ioctl(agnocast_fd, AGNOCAST_GET_TOPIC_PUBLISHER_INFO_CMD, args);
}

#else  // AGNOCAST_USE_DAEMON
// ============================================================
// User daemon mode: declarations — implemented in agnocast_socket_client.cpp
// ============================================================

int agnocast_ipc_get_version(struct ioctl_get_version_args * args);
int agnocast_ipc_add_process(union ioctl_add_process_args * args);
int agnocast_ipc_add_publisher(union ioctl_add_publisher_args * args);
int agnocast_ipc_add_subscriber(union ioctl_add_subscriber_args * args);
int agnocast_ipc_publish_msg(union ioctl_publish_msg_args * args);
int agnocast_ipc_receive_msg(union ioctl_receive_msg_args * args);
int agnocast_ipc_take_msg(union ioctl_take_msg_args * args);
int agnocast_ipc_release_sub_ref(struct ioctl_update_entry_args * args);
int agnocast_ipc_remove_publisher(struct ioctl_remove_publisher_args * args);
int agnocast_ipc_remove_subscriber(struct ioctl_remove_subscriber_args * args);
int agnocast_ipc_get_subscriber_num(union ioctl_get_subscriber_num_args * args);
int agnocast_ipc_get_publisher_num(union ioctl_get_publisher_num_args * args);
int agnocast_ipc_get_exit_process(struct ioctl_get_exit_process_args * args);
int agnocast_ipc_get_subscriber_qos(struct ioctl_get_subscriber_qos_args * args);
int agnocast_ipc_get_publisher_qos(struct ioctl_get_publisher_qos_args * args);
int agnocast_ipc_add_bridge(struct ioctl_add_bridge_args * args);
int agnocast_ipc_remove_bridge(struct ioctl_remove_bridge_args * args);
int agnocast_ipc_check_and_request_bridge_shutdown(
  struct ioctl_check_and_request_bridge_shutdown_args * args);
int agnocast_ipc_notify_bridge_shutdown();
int agnocast_ipc_set_ros2_subscriber_num(struct ioctl_set_ros2_subscriber_num_args * args);
int agnocast_ipc_set_ros2_publisher_num(struct ioctl_set_ros2_publisher_num_args * args);
int agnocast_ipc_get_topic_list(union ioctl_topic_list_args * args);
int agnocast_ipc_get_topic_subscriber_info(union ioctl_topic_info_args * args);
int agnocast_ipc_get_topic_publisher_info(union ioctl_topic_info_args * args);

// Records that this process mapped pid's memory pool (its own included), so a
// receive knows which publishers it can read and a restarted daemon can be told.
void daemon_note_mapped_region(pid_t pid, uint64_t addr, uint64_t size);

// How often this process reconnected to a restarted daemon and restored its
// registrations.
uint64_t daemon_reconnect_count();

// Evaluation of the rollback after a daemon crash.
struct DaemonTopicCheck
{
  uint32_t shm_violations;
  uint32_t registry_violations;
  uint64_t rollback_count;
  uint64_t rollback_record_num;
  uint32_t entry_num;
  uint32_t sub_num;
  char message[256];
};
int agnocast_ipc_debug_check_topic(const std::string & topic_name, DaemonTopicCheck * out);

struct DaemonRecoveryStatus
{
  bool recovered;
  bool in_recovery;
  uint32_t expected_num;
  uint32_t restored_num;
  uint32_t dead_num;
  uint32_t timed_out_num;
  uint32_t adopted_topic_num;
  uint32_t orphan_publisher_num;
  uint32_t orphan_subscriber_num;
  int64_t start_ns;
  int64_t end_ns;
};
int agnocast_ipc_get_recovery_status(DaemonRecoveryStatus * out);

#endif  // AGNOCAST_USE_DAEMON

}  // namespace agnocast
