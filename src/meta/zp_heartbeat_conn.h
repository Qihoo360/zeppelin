#ifndef ZP_HEARTBEAT_CONN_H
#define ZP_HEARTBEAT_CONN_H

#include <glog/logging.h>

#include "pb_conn.h"
#include "pink_thread.h"


class ZPHeartbeatThread;

class ZPHeartbeatConn: public pink::PbConn {
 public:
  ZPHeartbeatConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~ZPHeartbeatConn();
  virtual int DealMessage();

 private:
  ZPHeartbeatThread* self_thread_;
};

#endif
