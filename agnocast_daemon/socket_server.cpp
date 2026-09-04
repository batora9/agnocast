// SPDX-License-Identifier: Apache-2.0
#include "socket_server.hpp"

#include "bench_timing.hpp"
#include "hot_channel.hpp"

#include <linux/memfd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <new>
#include <stdexcept>
#include <thread>

static constexpr int kMaxEpollEvents = 64;
static constexpr int kEpollTimeoutMs = 1000;

// ============================================================
// Construction / destruction
// ============================================================

SocketServer::SocketServer(MetadataStore & store, MemoryAllocator & allocator)
: store_(store), allocator_(allocator), handlers_(store, allocator)
{
  server_fd_ = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (server_fd_ < 0) {
    throw std::runtime_error(std::string("socket() failed: ") + strerror(errno));
  }

  // Remove stale socket file so bind() does not fail with EADDRINUSE.
  // A legitimate previous run cleans up on exit, but do it here defensively.
  unlink(AGNOCAST_DAEMON_SOCKET_PATH);

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, AGNOCAST_DAEMON_SOCKET_PATH, sizeof(addr.sun_path) - 1);

  if (bind(server_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(server_fd_);
    throw std::runtime_error(std::string("bind() failed: ") + strerror(errno));
  }

  // Allow owner + group access only (0660).
  chmod(AGNOCAST_DAEMON_SOCKET_PATH, 0660);

  if (listen(server_fd_, SOMAXCONN) < 0) {
    close(server_fd_);
    unlink(AGNOCAST_DAEMON_SOCKET_PATH);
    throw std::runtime_error(std::string("listen() failed: ") + strerror(errno));
  }
}

SocketServer::~SocketServer()
{
  if (server_fd_ >= 0) close(server_fd_);
  if (epoll_fd_ >= 0) close(epoll_fd_);
  // Remove socket file so a subsequent daemon start does not see a stale path.
  unlink(AGNOCAST_DAEMON_SOCKET_PATH);
}

// ============================================================
// Main event loop
// ============================================================

void SocketServer::run()
{
  epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd_ < 0) {
    throw std::runtime_error(std::string("epoll_create1() failed: ") + strerror(errno));
  }

  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = server_fd_;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) < 0) {
    throw std::runtime_error(std::string("epoll_ctl() failed: ") + strerror(errno));
  }

  epoll_event events[kMaxEpollEvents];
  while (!shutdown_requested_) {
    const int nfds = epoll_wait(epoll_fd_, events, kMaxEpollEvents, kEpollTimeoutMs);
    if (nfds < 0) {
      if (errno == EINTR) continue;
      fprintf(stderr, "agnocast_daemon: epoll_wait() failed: %s\n", strerror(errno));
      break;
    }
    for (int i = 0; i < nfds; ++i) {
      if (events[i].data.fd == server_fd_) {
        accept_client();
      }
    }
  }
}

void SocketServer::request_shutdown() noexcept
{
  shutdown_requested_.store(true, std::memory_order_relaxed);
}

// ============================================================
// Client connection
// ============================================================

void SocketServer::accept_client()
{
  const int client_fd = accept4(server_fd_, nullptr, nullptr, SOCK_CLOEXEC);
  if (client_fd < 0) {
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      fprintf(stderr, "agnocast_daemon: accept4() failed: %s\n", strerror(errno));
    }
    return;
  }

  // Obtain the connecting process's PID from the kernel.
  // SO_PEERCRED is kernel-verified and cannot be spoofed by the client.
  ucred cred{};
  socklen_t cred_len = sizeof(cred);
  if (getsockopt(client_fd, SOL_SOCKET, SO_PEERCRED, &cred, &cred_len) < 0) {
    fprintf(stderr, "agnocast_daemon: getsockopt(SO_PEERCRED) failed: %s\n", strerror(errno));
    close(client_fd);
    return;
  }

  const int buf = AGNOCAST_DAEMON_SOCKET_BUF_SIZE;
  if (setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf)) < 0) {
    fprintf(stderr, "agnocast_daemon: setsockopt(SO_SNDBUF) failed: %s\n", strerror(errno));
    close(client_fd);
    return;
  }
  if (setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf)) < 0) {
    fprintf(stderr, "agnocast_daemon: setsockopt(SO_RCVBUF) failed: %s\n", strerror(errno));
    close(client_fd);
    return;
  }

  std::thread([this, client_fd, pid = cred.pid]() {
    handle_client(client_fd, pid);
    close(client_fd);
  }).detach();
}

// ============================================================
// Per-client request loop
// ============================================================

static int create_hot_memfd()
{
  const int fd = static_cast<int>(syscall(SYS_memfd_create, "agnocast-hot", MFD_CLOEXEC));
  if (fd < 0) return -1;
  if (ftruncate(fd, static_cast<off_t>(sizeof(HotChannel))) < 0) {
    close(fd);
    return -1;
  }
  return fd;
}

static bool send_memfd(int sock, int memfd)
{
  char dummy = 0;
  iovec iov{};
  iov.iov_base = &dummy;
  iov.iov_len = 1;
  char cbuf[CMSG_SPACE(sizeof(int))];
  msghdr msg{};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = cbuf;
  msg.msg_controllen = sizeof(cbuf);
  cmsghdr * cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  std::memcpy(CMSG_DATA(cmsg), &memfd, sizeof(int));
  return sendmsg(sock, &msg, MSG_NOSIGNAL) >= 0;
}

static bool socket_hung_up(int client_fd)
{
  pollfd pfd{};
  pfd.fd = client_fd;
  pfd.events = POLLHUP | POLLERR | POLLIN;
  const int n = poll(&pfd, 1, 0);
  if (n < 0) {
    return errno != EINTR;
  }
  if (n == 0) return false;
  if (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) return true;
  if (pfd.revents & POLLIN) {
    char discard;
    const ssize_t r = recv(client_fd, &discard, 1, MSG_DONTWAIT);
    return r == 0;
  }
  return false;
}

void SocketServer::handle_client(int client_fd, pid_t client_pid)
{
  const int memfd = create_hot_memfd();
  if (memfd < 0) {
    fprintf(
      stderr, "agnocast_daemon: memfd_create failed (pid=%d): %s\n", client_pid, strerror(errno));
    return;
  }
  void * map = mmap(nullptr, sizeof(HotChannel), PROT_READ | PROT_WRITE, MAP_SHARED, memfd, 0);
  if (map == MAP_FAILED) {
    fprintf(
      stderr, "agnocast_daemon: mmap hot channel failed (pid=%d): %s\n", client_pid,
      strerror(errno));
    close(memfd);
    return;
  }
  auto * channel = new (map) HotChannel{};
  if (!send_memfd(client_fd, memfd)) {
    fprintf(
      stderr, "agnocast_daemon: send memfd failed (pid=%d): %s\n", client_pid, strerror(errno));
    munmap(map, sizeof(HotChannel));
    close(memfd);
    return;
  }
  close(memfd);

  CommandHandlers::bind_hot_channel(channel);
  uint32_t served = 0;

  while (!shutdown_requested_) {
    if (channel->request_seq.load(std::memory_order_acquire) != served) {
      // fall through to handle
    } else if (hot_spin_until(&channel->request_seq, served + 1, AGNOCAST_HOT_DAEMON_SPIN_NS)) {
      // next seq arrived during the spin; served+1 may skip if client ever
      // posts by more than 1, so re-read below
    } else {
      if (socket_hung_up(client_fd)) break;
      const uint32_t expected = channel->request_seq.load(std::memory_order_relaxed);
      if (expected != served) {
        // posted while we checked hangup
      } else {
        timespec ts{};
        ts.tv_nsec = 50000000;  // 50 ms, so shutdown and hangup are noticed
        hot_futex_wait(&channel->request_seq, expected, &ts);
        continue;
      }
    }

    const uint32_t posted = channel->request_seq.load(std::memory_order_acquire);
    if (posted == served) continue;

    AGNOCAST_DAEMON_BENCH_STAMP_RECV();
    AGNOCAST_DAEMON_BENCH_CLEAR_WORK();

    RequestHeader hdr{};
    hdr.command = channel->command;
    hdr.payload_size = channel->request_size;
    if (hdr.payload_size > AGNOCAST_HOT_PAYLOAD_SIZE) {
      fprintf(
        stderr, "agnocast_daemon: oversized hot payload from pid=%d (%u bytes)\n", client_pid,
        hdr.payload_size);
      break;
    }
    const void * payload = (hdr.payload_size > 0) ? channel->payload : nullptr;
    handlers_.dispatch(client_fd, client_pid, hdr, payload);
    served = posted;
  }

  CommandHandlers::bind_hot_channel(nullptr);
  munmap(map, sizeof(HotChannel));
  handlers_.on_client_disconnect(client_pid);
}
