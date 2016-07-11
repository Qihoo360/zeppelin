#include "zp_metacmd_conn.h"

#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_admin.h"

#include "slash_string.h"

extern ZPDataServer *zp_data_server;

ZPMetacmdConn::ZPMetacmdConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = reinterpret_cast<ZPMetacmdWorkerThread*>(thread);
}

ZPMetacmdConn::~ZPMetacmdConn() {
}

int ZPMetacmdConn::DealMessage() {
  request_.ParseFromArray(rbuf_ + 4, header_len_);
  LOG(INFO) << "ZPMetacmdConn DealMessage type:" << (int)request_.type();

  Cmd* cmd = self_thread_->GetCmd(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported type: " << (int)request_.type();
    return -1;
  }
  

  // for now, only one cmd SYNC
  switch (request_.type()) {
    case ZPDataControl::DataCmdRequest_TYPE::DataCmdRequest_TYPE_SYNC: {
      cmd->Do(&request_, &response_);
      DLOG(INFO) << "Receive Sync cmd";
      set_is_reply(true);
      break;
    }
  }

  if (cmd->is_write()) {
    if (cmd->result().ok()) {
      // Restore Message
    }
    // TODO add RecordLock for write cmd
  }

  res_ = cmd->Response();

  return 0;
  
//  set_is_reply(true);
//  if (argv_[0] == "ping") {
//    memcpy(wbuf_ + wbuf_len_, "+PONG\r\n", 7);
//    wbuf_len_ += 7;
//  } else if (argv_[0] == "spci") {
//    int64_t sid = -1;
//    slash::string2l(argv_[1].data(), argv_[1].size(), &sid);
//    zp_data_server->MayUpdateSlavesMap(sid, fd());
//    memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
//    wbuf_len_ += 5;
//  } else {
//    memcpy(wbuf_ + wbuf_len_, "-ERR What the fuck are u sending\r\n", 34);
//    wbuf_len_ += 34;
//  }
  return 0;
}
