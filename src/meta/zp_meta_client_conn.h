#ifndef ZP_META_CLIENT_CONN_H
#define ZP_META_CLIENT_CONN_H

#include <string>

#include "client.pb.h"

#include "pb_conn.h"
#include "pink_thread.h"


class ZPMetaWorkerThread;

class ZPMetaClientConn : public pink::PbConn {
 public:
  ZPMetaClientConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~ZPMetaClientConn();

  virtual int DealMessage();
  ZPMetaWorkerThread* self_thread() {
    return self_thread_;
  }

 private:

  client::CmdRequest request_;
  client::CmdResponse response_;

  ZPMetaWorkerThread* self_thread_;
};

#endif
