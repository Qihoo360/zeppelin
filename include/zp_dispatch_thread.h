#ifndef ZP_DISPATCH_THREAD_H
#define ZP_DISPATCH_THREAD_H

#include "zp_worker_thread.h"
#include "dispatch_thread.h"
#include "zp_client_conn.h"

class ZPDispatchThread : public pink::DispatchThread<ZPClientConn> {
 public:
  ZPDispatchThread(int port, int work_num, ZPWorkerThread** ZP_worker_thread, int cron_interval);
  virtual ~ZPDispatchThread();
  virtual void CronHandle();

  int ClientNum();

 private:
};

#endif
