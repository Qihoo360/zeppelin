#ifndef ZP_DATA_DISPATCH_THREAD_H
#define ZP_DATA_DISPATCH_THREAD_H

#include "zp_data_worker_thread.h"
#include "dispatch_thread.h"
#include "zp_data_client_conn.h"

class ZPDataDispatchThread : public pink::DispatchThread<ZPDataClientConn> {
 public:
  ZPDataDispatchThread(int port, int work_num, ZPDataWorkerThread** worker_thread, int cron_interval);
  virtual ~ZPDataDispatchThread();
  virtual void CronHandle();

  int ClientNum();

 private:
};

#endif
