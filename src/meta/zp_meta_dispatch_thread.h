#ifndef ZP_META_DISPATCH_THREAD_H
#define ZP_META_DISPATCH_THREAD_H

#include "dispatch_thread.h"
#include "zp_meta_client_conn.h"

class ZPMetaDispatchThread : public pink::DispatchThread<ZPMetaClientConn> {
 public:
  ZPMetaDispatchThread(int port, int work_num, ZPMetaWorkerThread** worker_thread, int cron_interval);
  virtual ~ZPMetaDispatchThread();
  virtual void CronHandle();

  int ClientNum();

 private:

};

#endif
