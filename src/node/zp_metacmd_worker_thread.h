#ifndef ZP_METACMD_WORKER_THREAD_H
#define ZP_METACMD_WORKER_THREAD_H

#include "zp_command.h"
#include "zp_admin.h"
#include "zp_metacmd_conn.h"
#include "holy_thread.h"

class ZPMetacmdWorkerThread : public pink::HolyThread<ZPMetacmdConn> {
 public:

  ZPMetacmdWorkerThread(int port, int cron_interval = 0);
  virtual ~ZPMetacmdWorkerThread();

  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip_port);

  bool FindSlave(int fd);

  Cmd* GetCmd(const int op) {
    return GetCmdFromTable(op, cmds_);
  }

 private:

  std::unordered_map<int, Cmd*> cmds_;

};
#endif
