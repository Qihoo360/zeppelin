#ifndef ZP_BINLOG_RECEIVER_THREAD_H
#define ZP_BINLOG_RECEIVER_THREAD_H

#include <queue>

#include "holy_thread.h"
#include "slash_mutex.h"
#include "env.h"

#include "zp_util.h"
#include "zp_sync_conn.h"
#include "zp_command.h"


class ZPBinlogReceiverThread : public pink::HolyThread<ZPSyncConn> {
 public:

  ZPBinlogReceiverThread(int port, int cron_interval = 0);
  virtual ~ZPBinlogReceiverThread();
  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip);
  void KillBinlogSender();

  int32_t ThreadClientNum() {
    slash::RWLock(&rwlock_, false);
    int32_t num = conns_.size();
    return num;
  }


  void PlusStat(const std::string &table);
  void UpdateLastStat();
  bool GetStat(const std::string &table, Statistic& stat);
  bool GetTotalStat(Statistic& stat);


 private:
  slash::Mutex mutex_; // protect cron_task_
  void AddCronTask(WorkerCronTask task);
  void KillAll();
  std::queue<WorkerCronTask> cron_tasks_;

  // statistic related
  slash::Mutex stat_mu_;
  uint64_t last_time_us_;
  Statistic other_stat_;
  std::unordered_map<std::string, Statistic*> table_stats_;

  uint64_t serial_;
};
#endif
