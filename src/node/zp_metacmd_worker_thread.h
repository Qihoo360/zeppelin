#ifndef ZP_METACMD_WORKER_THREAD_H
#define ZP_METACMD_WORKER_THREAD_H

#include <queue>

#include "zp_command.h"
#include "zp_admin.h"
#include "zp_metacmd_conn.h"
#include "holy_thread.h"
#include "slash_mutex.h"
#include "zp_util.h"

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

 private:
  slash::Mutex mutex_; // protect cron_task_
  void AddCronTask(WorkerCronTask task);
  void KillAll();
  std::queue<WorkerCronTask> cron_tasks_;

  std::unordered_map<int, Cmd*> cmds_;

};
#endif
