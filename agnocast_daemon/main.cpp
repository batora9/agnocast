// SPDX-License-Identifier: Apache-2.0
#include "memory_allocator.hpp"
#include "metadata_store.hpp"
#include "protocol.h"
#include "socket_server.hpp"

#include <signal.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// Global pointer used by the signal handler to request a graceful shutdown.
// Written once (before run()) and read from signal context — safe in practice
// because the signal handler only stores true to an atomic<bool> inside server.
static SocketServer * g_server = nullptr;

static void signal_handler(int /*signo*/) noexcept
{
  if (g_server) {
    g_server->request_shutdown();
  }
}

static std::string state_path()
{
  const char * v = std::getenv("AGNOCAST_DAEMON_STATE_PATH");
  return (v != nullptr && v[0] != '\0') ? v : AGNOCAST_DAEMON_STATE_PATH;
}

static int64_t recovery_timeout_ms()
{
  const char * v = std::getenv("AGNOCAST_DAEMON_RECOVERY_TIMEOUT_MS");
  if (v == nullptr || v[0] == '\0') return 5000;
  const long long n = std::strtoll(v, nullptr, 10);
  return n > 0 ? n : 5000;
}

// The clients a previous daemon left behind that are still running. They will
// reconnect and restore what they registered. A line is "<pid> <starttime>";
// the start time tells a reused pid from the client (0 or missing: unknown).
static std::vector<pid_t> previous_clients(
  const std::string & path, std::unordered_map<pid_t, uint64_t> * starttimes)
{
  std::vector<pid_t> pids;
  std::ifstream ifs(path);
  if (!ifs) return pids;
  std::string line;
  while (std::getline(ifs, line)) {
    if (line.empty() || line.rfind("agnocast_daemon", 0) == 0) continue;
    char * end = nullptr;
    const long pid = std::strtol(line.c_str(), &end, 10);
    const uint64_t starttime = std::strtoull(end, nullptr, 10);
    if (pid <= 0 || pid == getpid()) continue;
    if (topic_shm_proc_alive(static_cast<pid_t>(pid), starttime)) {
      pids.push_back(static_cast<pid_t>(pid));
      (*starttimes)[static_cast<pid_t>(pid)] = starttime;
    }
  }
  return pids;
}

int main()
{
  // Ignore SIGPIPE so that write() to a disconnected client returns EPIPE
  // instead of killing the daemon.
  signal(SIGPIPE, SIG_IGN);

  // Register graceful-shutdown handlers before creating any resources.
  struct sigaction sa
  {
  };
  sa.sa_handler = signal_handler;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGTERM, &sa, nullptr);
  sigaction(SIGINT, &sa, nullptr);

  try {
    MemoryAllocator allocator;
    MetadataStore store;
    SocketServer server(store, allocator);

    g_server = &server;

    const std::string path = state_path();
    std::unordered_map<pid_t, uint64_t> starttimes;
    const std::vector<pid_t> clients = previous_clients(path, &starttimes);
    server.handlers().set_state_path(path);
    if (!clients.empty()) {
      server.handlers().start_recovery(clients, recovery_timeout_ms(), starttimes);
    }

    fprintf(stderr, "agnocast_daemon: started (socket: %s)\n", AGNOCAST_DAEMON_SOCKET_PATH);

    server.run();

    g_server = nullptr;
    fprintf(stderr, "agnocast_daemon: shutdown complete\n");
  } catch (const std::exception & e) {
    fprintf(stderr, "agnocast_daemon: fatal error: %s\n", e.what());
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
