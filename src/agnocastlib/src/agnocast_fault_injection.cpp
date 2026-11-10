// SPDX-License-Identifier: Apache-2.0
//
// Client-side fault points for evaluating the recovery flows (daemon backend).
//
//   AGNOCAST_CLIENT_FAULT_POINT=<name>    shm.publish, shm.receive, shm.take, shm.release
//   AGNOCAST_CLIENT_FAULT_HIT=<n>         act on the n-th time it is reached (default 1)
//   AGNOCAST_CLIENT_FAULT_ACTION=<what>   kill (default): SIGKILL, dies holding the segment
//                                         stop: SIGSTOP, keeps holding it (a stall)
//
// The points sit inside topic_shm_* with the segment's mutex held and part of
// the operation done, the worst place for a client to die or stop.

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace
{

struct FaultConfig
{
  const char * point = nullptr;
  long hit = 1;
  bool stop = false;
};

const FaultConfig & fault_config()
{
  static const FaultConfig cfg = [] {
    FaultConfig c;
    const char * point = std::getenv("AGNOCAST_CLIENT_FAULT_POINT");
    if (point != nullptr && point[0] != '\0') c.point = point;
    if (const char * hit = std::getenv("AGNOCAST_CLIENT_FAULT_HIT"); hit != nullptr) {
      const long n = std::strtol(hit, nullptr, 10);
      if (n > 0) c.hit = n;
    }
    const char * action = std::getenv("AGNOCAST_CLIENT_FAULT_ACTION");
    c.stop = action != nullptr && std::strcmp(action, "stop") == 0;
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
  std::fprintf(
    stderr, "agnocast: fault injection: %s at %s (hit %ld, pid %d)\n",
    cfg.stop ? "stopped" : "killed", name, cfg.hit, static_cast<int>(getpid()));
  kill(getpid(), cfg.stop ? SIGSTOP : SIGKILL);
}
