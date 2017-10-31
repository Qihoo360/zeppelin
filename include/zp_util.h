#ifndef INCLUDE_ZP_UTIL_H_
#define INCLUDE_ZP_UTIL_H_

#include <string>
#include <glog/logging.h>

#include "include/zp_conf.h"

#include "slash/include/env.h"

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

struct Statistic {
  std::string table_name;
  uint64_t last_querys;
  uint64_t querys;
  uint64_t last_qps;
  uint64_t used_disk;
  uint64_t free_disk;  // not used for now

  Statistic();
  Statistic(const Statistic& stat);

  void Reset();
  void Add(const Statistic& stat);
  void Dump();
};

#endif
