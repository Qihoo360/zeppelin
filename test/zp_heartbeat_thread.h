#ifndef ZP_HEARTBEAT_THREAD_H
#define ZP_HEARTBEAT_THREAD_H

#include "zp_heartbeat_conn.h"

#include "logger.h"

#include "holy_thread.h"

class ZPHeartbeatThread : public pink::HolyThread<ZPHeartbeatConn> {
 public:

  ZPHeartbeatThread(int port, int cron_interval = 0);
  virtual ~ZPHeartbeatThread();

  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip_port);


 private:
};
#endif
