#ifndef ZP_METACMD_CONN_H
#define ZP_METACMD_CONN_H

#include <glog/logging.h>
#include "zp_data_control.pb.h"

#include "pb_conn.h"
#include "pink_thread.h"


class ZPMetacmdWorkerThread;

class ZPMetacmdConn: public pink::PbConn {
 public:
  ZPMetacmdConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~ZPMetacmdConn();
  virtual int DealMessage();

 private:
  ZPMetacmdWorkerThread* self_thread_;

  ZPDataControl::DataCmdRequest request_;
  ZPDataControl::DataCmdResponse response_;
};

#endif
