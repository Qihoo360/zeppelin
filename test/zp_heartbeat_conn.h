#ifndef ZP_HEARTBEAT_CONN_H
#define ZP_HEARTBEAT_CONN_H

#include "logger.h"
#include "zp_meta.pb.h"
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

  ZPMeta::MetaCmd request_;
  ZPMeta::MetaCmdResponse response_;
};

#endif
