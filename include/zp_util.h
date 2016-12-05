#ifndef INCLUDE_ZP_UTIL_H_
#define INCLUDE_ZP_UTIL_H_

#include <string>

#include "zp_conf.h"
// @TODO use Tasktype instead of macro
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

void daemonize();
void close_std();
void create_pid_file();
class FileLocker {
 public:
  explicit FileLocker(const std::string& file);
  ~FileLocker();
  slash::Status Lock();

 private:
  slash::FileLock* file_lock_;
  const std::string file_;
};

#endif
