#ifndef ZP_UTIL_H
#define ZP_UTIL_H

#include <string>

// TODO use Tasktype instead of macro
enum TaskType {
  kTaskKill = 0,
  kTaskKillAll = 1
};

#define TASK_KILL 0
#define TASK_KILLALL 1

struct WorkerCronTask {
  int task;
  std::string ip_port;
};

#endif
