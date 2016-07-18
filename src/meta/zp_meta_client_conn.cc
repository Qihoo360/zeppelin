#include "zp_meta_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "zp_meta_worker_thread.h"
#include "zp_meta_server.h"

extern ZPMetaServer* g_zp_meta_server;

////// ZPMetaClientConn //////
ZPMetaClientConn::ZPMetaClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = dynamic_cast<ZPMetaWorkerThread*>(thread);
}

ZPMetaClientConn::~ZPMetaClientConn() {
}

// Msg is  [ length (int32) | pb_msg (length bytes) ]
int ZPMetaClientConn::DealMessage() {
  return 0;
}

