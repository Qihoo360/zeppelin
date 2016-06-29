#ifndef ZP_CLIENT_CONN_H
#define ZP_CLIENT_CONN_H

#include <string>

#include "client.pb.h"

#include "pb_conn.h"
#include "pink_thread.h"


class ZPWorkerThread;

class ZPClientConn : public pink::PbConn {
 public:
  ZPClientConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~ZPClientConn();

  virtual int DealMessage();
  ZPWorkerThread* self_thread() {
    return self_thread_;
  }

 private:

  ZPWorkerThread* self_thread_;
};

#endif
