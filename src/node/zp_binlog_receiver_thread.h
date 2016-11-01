#ifndef ZP_BINLOG_RECEIVER_THREAD_H
#define ZP_BINLOG_RECEIVER_THREAD_H

#include <queue>

#include "zp_util.h"
#include "zp_sync_conn.h"
#include "zp_command.h"

#include "holy_thread.h"
#include "slash_mutex.h"
#include "env.h"

class ZPBinlogReceiverThread : public pink::HolyThread<ZPSyncConn> {
 public:

  ZPBinlogReceiverThread(int port, int cron_interval = 0);
  virtual ~ZPBinlogReceiverThread();
  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip);
  void KillBinlogSender();

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
    uint64_t cur_time_ms = slash::NowMicros();
    slash::RWLock(&rwlock_, true);
    last_sec_query_num_ = (query_num_ - last_query_num_) * 1000000 / (cur_time_ms - last_time_us_ + 1);
    last_time_us_ = cur_time_ms;
    last_query_num_ = query_num_;
  }

  int32_t ThreadClientNum() {
    slash::RWLock(&rwlock_, false);
    int32_t num = conns_.size();
    return num;
  }


 private:
  slash::Mutex mutex_; // protect cron_task_
  void AddCronTask(WorkerCronTask task);
  void KillAll();
  std::queue<WorkerCronTask> cron_tasks_;

  uint64_t query_num_;
  uint64_t last_query_num_;
  uint64_t last_time_us_;
  uint64_t last_sec_query_num_;
  uint64_t serial_;
};
#endif
