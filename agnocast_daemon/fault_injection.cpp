// SPDX-License-Identifier: Apache-2.0
//
// Crash points for evaluating the rollback after a daemon crash.
//
//   AGNOCAST_DAEMON_FAULT_POINT=<name>   e.g. shm.remove_subscriber
//   AGNOCAST_DAEMON_FAULT_HIT=<n>        kill on the n-th time it is reached (default 1)
//
// When the named point is reached for the n-th time the daemon sends itself
// SIGKILL, so it dies exactly there, holding whatever it holds: no destructor,
// no unlock, no reply. "shm.*" points sit inside segment operations with the
// segment mutex held; "reply.*" points sit after a change was committed but
// before the client was told.

#include "topic_shm.hpp"

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifdef AGNOCAST_FAULT_INJECTION

namespace
{

struct FaultConfig
{
  const char * point = nullptr;
  long hit = 1;
};

const FaultConfig & fault_config()
{
  static const FaultConfig cfg = [] {
    FaultConfig c;
    const char * point = std::getenv("AGNOCAST_DAEMON_FAULT_POINT");
    if (point != nullptr && point[0] != '\0') c.point = point;
    if (const char * hit = std::getenv("AGNOCAST_DAEMON_FAULT_HIT"); hit != nullptr) {
      const long n = std::strtol(hit, nullptr, 10);
      if (n > 0) c.hit = n;
    }
    return c;
  }();
  return cfg;
}

std::atomic<long> g_fault_hits{0};

}  // namespace

void agnocast_fault_point(const char * name)
{
  const FaultConfig & cfg = fault_config();
  if (cfg.point == nullptr || std::strcmp(cfg.point, name) != 0) return;
  if (g_fault_hits.fetch_add(1) + 1 != cfg.hit) return;
  // stderr is unbuffered, so the line is out before the signal lands.
  std::fprintf(stderr, "agnocast_daemon: fault injection: killed at %s (hit %ld)\n", name, cfg.hit);
  kill(getpid(), SIGKILL);
}

#endif  // AGNOCAST_FAULT_INJECTION
