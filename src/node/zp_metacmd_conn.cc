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

  cmd->Do(&request_, &response_);
  set_is_reply(true);

  switch (request_.type()) {
  //  case ZPMeta::MetaCmd_Type::MetaCmd_Type_SYNC: {
  //    DLOG(INFO) << "Receive Sync cmd";
  //    break;
  //  }
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_UPDATE: {
      DLOG(INFO) << "Receive Update cmd";
      break;
    }
    default:
      DLOG(INFO) << "unsupported cmd type:" << (int)request_.type();
  }

  if (cmd->is_write()) {
    if (cmd->result().ok()) {
      // Restore Message
    }
    // TODO add RecordLock for write cmd
  }

  res_ = &response_;

  return 0;
}
