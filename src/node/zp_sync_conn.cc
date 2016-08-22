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
  self_thread_->PlusQueryNum();

  request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);

  //int result = request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  //DLOG(INFO) << "SyncConn ParseFromArray return " << result << ", cur_pos_=" << cur_pos_ << ", header_len_=" << header_len_ << ", rbuf_len=" << rbuf_len_;

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
    default:
      DLOG(INFO) << "SyncConn Receive unsupported cmd";
      break;
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

  // do not reply
  set_is_reply(false);

  // Add read lock for no suspend command
  if (!cmd->is_suspend()) {
    pthread_rwlock_rdlock(&(zp_data_server->server_rw_));
  }

  zp_data_server->mutex_record_.Lock(cmd->key());

  cmd->Do(&request_, &response_);

  if (cmd->result().ok()) {
    // Restore Message
    std::string raw_msg(rbuf_ + cur_pos_ - header_len_ - 4, header_len_ + 4);
    zp_data_server->logger_->Lock();
    zp_data_server->logger_->Put(raw_msg);
    zp_data_server->logger_->Unlock();
  }
  
  zp_data_server->mutex_record_.Unlock(cmd->key());

  if (!cmd->is_suspend()) {
    pthread_rwlock_unlock(&(zp_data_server->server_rw_));
  }

  res_ = &response_;
  //res_ = cmd->Response();

  return 0;
}
