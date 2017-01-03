#ifndef ZP_DATA_WORKER_THREAD_H
#define ZP_DATA_WORKER_THREAD_H

#include <queue>
#include <unordered_map>

#include "worker_thread.h"
#include "slash_mutex.h"
#include "env.h"

#include "zp_util.h"
#include "zp_command.h"
#include "zp_data_client_conn.h"


class ZPDataWorkerThread : public pink::WorkerThread<ZPDataClientConn> {
 public:
  ZPDataWorkerThread(int cron_interval = 0);
  virtual ~ZPDataWorkerThread();
  virtual void CronHandle();

  //int64_t ThreadClientList(std::vector<ClientInfo> *clients = NULL);
  bool ThreadClientKill(std::string ip_port = "");
  int ThreadClientNum();

  void PlusStat(const std::string &table);
  void UpdateLastStat();
  bool GetStat(const std::string &table, Statistic& stat);
  bool GetTotalStat(Statistic& stat);

 private:
  slash::Mutex mutex_; // protect cron_task_
  std::queue<WorkerCronTask> cron_tasks_;

  // statistic related
  slash::Mutex stat_mu_;
  uint64_t last_time_us_;
  Statistic other_stat_;
  std::unordered_map<std::string, Statistic*> table_stats_;

  void AddCronTask(WorkerCronTask task);
  bool FindClient(std::string ip_port);
  void ClientKill(std::string ip_port);
  void ClientKillAll();
};


#endif
