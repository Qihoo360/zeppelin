#ifndef ZP_METACMD_WORKER_THREAD_H
#define ZP_METACMD_WORKER_THREAD_H

#include <queue>

#include "zp_command.h"
#include "zp_admin.h"
#include "zp_metacmd_conn.h"
#include "zp_util.h"

#include "holy_thread.h"

#include "slash_mutex.h"

#include "env.h"

class ZPMetacmdWorkerThread : public pink::HolyThread<ZPMetacmdConn> {
 public:

  ZPMetacmdWorkerThread(int port, int cron_interval = 0);
  virtual ~ZPMetacmdWorkerThread();

  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip_port);

  Cmd* GetCmd(const int op) {
    return GetCmdFromTable(op, cmds_);
  }
  void KillMetacmdConn();

  uint64_t query_num() {
    slash::RWLock(&rwlock_, false);
    return query_num_;
  }

  uint64_t last_sec_query_num() {
    slash::RWLock(&rwlock_, false);
    return last_sec_query_num_;
  }

  void PlusQueryNum() {
    slash::RWLock(&rwlock_, true);
    query_num_++;
  }

  void ResetLastSecQuerynum() {
    uint64_t cur_time_us = slash::NowMicros();
    slash::RWLock l(&rwlock_, true);
    last_sec_query_num_ = ((query_num_ - last_query_num_) * 1000000 / (cur_time_us - last_time_us_ + 1));
    last_query_num_ = query_num_;
    last_time_us_ = cur_time_us;
  }

 private:
  slash::Mutex mutex_; // protect cron_task_
  void AddCronTask(WorkerCronTask task);
  void KillAll();
  std::queue<WorkerCronTask> cron_tasks_;

  std::unordered_map<int, Cmd*> cmds_;
  uint64_t query_num_;
  uint64_t last_query_num_;
  uint64_t last_sec_query_num_;
  uint64_t last_time_us_;

};
#endif
