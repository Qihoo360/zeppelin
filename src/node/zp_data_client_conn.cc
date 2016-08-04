#include "zp_data_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "zp_data_worker_thread.h"
#include "zp_data_server.h"

extern ZPDataServer* zp_data_server;

////// ZPDataClientConn //////
ZPDataClientConn::ZPDataClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = dynamic_cast<ZPDataWorkerThread*>(thread);
}

ZPDataClientConn::~ZPDataClientConn() {
}

// Msg is  [ length (int32) | pb_msg (length bytes) ]
int ZPDataClientConn::DealMessage() {
  request_.ParseFromArray(rbuf_ + 4, header_len_);
 // if (!ret) {
 //     DLOG(INFO) << "DealMessage  ParseFromArray failed";
 // }

  // TODO test only
  switch (request_.type()) {
    case client::Type::SET: {
      DLOG(INFO) << "Receive Set cmd";
      break;
    }
    case client::Type::GET: {
      DLOG(INFO) << "Receive Get cmd";
      break;
    }
    case client::Type::SYNC: {
      DLOG(INFO) << "Receive Sync cmd";
      break;
    }
  }

  Cmd* cmd = self_thread_->GetCmd(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported type: " << (int)request_.type();
    return -1;
  }

  Status s = cmd->Init(&request_);
  if (!s.ok()) {
    LOG(ERROR) << "command Init failed, " << s.ToString();
  }

  if (cmd->is_write()) {
    // TODO add RecordLock for write cmd
    zp_data_server->mutex_record_.Lock(cmd->key());
  }

  cmd->Do(&request_, &response_);
  set_is_reply(true);

  if (cmd->is_write()) {
    if (cmd->result().ok()) {
      // Restore Message
      std::string raw_msg(rbuf_, header_len_ + 4);
      zp_data_server->logger_->Lock();
      zp_data_server->logger_->Put(raw_msg);
      zp_data_server->logger_->Unlock();
    }
    // TODO add RecordLock for write cmd
    zp_data_server->mutex_record_.Unlock(cmd->key());
  }

  res_ = &response_;

  return 0;
}

