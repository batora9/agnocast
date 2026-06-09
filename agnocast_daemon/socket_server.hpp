// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>

#include <sys/types.h>

#include "metadata_store.hpp"
#include "memory_allocator.hpp"
#include "protocol.h"

// Unix Domain Socket server for the Agnocast daemon.
//
// Architecture:
//   Main thread  — epoll-based accept loop (non-blocking).
//   Client threads — one detached std::thread per connected client; each
//                    performs blocking recv/send on the client socket.
//
// This separates the concerns cleanly: the event loop is never blocked by
// request processing, while per-client serialisation is handled naturally
// by the thread owning that socket.
//
// TODO: replace per-client threads with a thread pool if connection count
//       becomes a scalability concern.
class SocketServer
{
public:
  SocketServer(MetadataStore & store, MemoryAllocator & allocator);
  ~SocketServer();

  SocketServer(const SocketServer &) = delete;
  SocketServer & operator=(const SocketServer &) = delete;

  // Blocks until request_shutdown() is called (or a fatal error occurs).
  void run();

  // Thread-safe; may be called from a signal handler.
  void request_shutdown() noexcept;

private:
  // Accept one client from server_fd_ and spawn a handler thread.
  void accept_client();

  // Runs in a per-client thread.  Loops reading requests until the client
  // disconnects or an error occurs.
  void handle_client(int client_fd, pid_t client_pid);

  // Dispatch a single decoded command.  Called from handle_client().
  void dispatch_command(
    int client_fd, pid_t client_pid, const RequestHeader & hdr, const void * payload);

  // Send a response with no payload (error-only reply).
  static void send_response(int fd, int32_t error_code);

  // Send a response with payload.  Header and payload are delivered as a
  // single SOCK_SEQPACKET message via sendmsg() with scatter-gather I/O.
  static void send_response(
    int fd, int32_t error_code, const void * payload, uint32_t payload_size);

  int server_fd_ = -1;
  int epoll_fd_ = -1;

  // Set by request_shutdown(); the epoll loop checks this flag once per
  // kEpollTimeoutMs milliseconds.
  // TODO: replace with an eventfd for immediate wakeup.
  std::atomic<bool> shutdown_requested_{false};

  MetadataStore & store_;
  MemoryAllocator & allocator_;
};
