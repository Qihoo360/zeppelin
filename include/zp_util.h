#ifndef ZP_UTIL_H
#define ZP_UTIL_H

#include <string>

#include "zp_options.h"
class ZPOptions;
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

void daemonize();
void close_std();
void create_pid_file(ZPOptions& options);
class FileLocker {
  public:

    FileLocker(const std::string& file);
    ~FileLocker();
    slash::Status Lock();

  private:

    slash::FileLock* file_lock_;
    const std::string file_;
};

#endif
