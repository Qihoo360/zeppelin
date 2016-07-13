#include "zp_sync_conn.h"

#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_binlog_receiver_thread.h"

extern ZPDataServer* zp_data_server;

ZPSyncConn::ZPSyncConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
//  self_thread_ = reinterpret_cast<ZPBinlogReceiverThread*>(thread);
  self_thread_ = dynamic_cast<ZPBinlogReceiverThread*>(thread);
}

ZPSyncConn::~ZPSyncConn() {
}

int ZPSyncConn::DealMessage() {
  request_.ParseFromArray(rbuf_ + 4, header_len_);
  // TODO test only
  switch (request_.type()) {
    case client::Type::SET: {
      DLOG(INFO) << "SyncConn Receive Set cmd";
      break;
    }
    case client::Type::GET: {
      DLOG(INFO) << "SyncConn Receive Get cmd";
      break;
    }
  }

  self_thread_->PlusThreadQuerynum();

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

  // do not reply
  set_is_reply(false);
  cmd->Do(&request_, &response_);

  if (cmd->is_write()) {
    if (cmd->result().ok()) {
      // Restore Message
      std::string raw_msg(rbuf_, header_len_);
      zp_data_server->logger_->Lock();
      zp_data_server->logger_->Put(raw_msg);
      zp_data_server->logger_->Unlock();
    }
    // TODO add RecordLock for write cmd
    zp_data_server->mutex_record_.Unlock(cmd->key());
  }

  res_ = &response_;
  //res_ = cmd->Response();

  return 0;
}
