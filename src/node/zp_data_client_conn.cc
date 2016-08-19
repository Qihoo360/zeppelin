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
  self_thread_->PlusThreadQuerynum();
  request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);

  //int ret = request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  //DLOG(INFO) << "DataClientConn ParseFromArray return " << ret << ", cur_pos_=" << cur_pos_ << ", header_len_=" << header_len_ << ", rbuf_len=" << rbuf_len_;
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

  set_is_reply(true);

  if (cmd->is_write()) {
    // TODO  very ugly implementation for readonly
    if (zp_data_server->readonly()) {
      cmd->Do(&request_, &response_, true);
      res_ = &response_;
      return 0;
    }
    // TODO add RecordLock for write cmd
    zp_data_server->mutex_record_.Lock(cmd->key());
  }

  // Add read lock for no suspend command
  if (!cmd->is_suspend()) {
    pthread_rwlock_rdlock(&(zp_data_server->server_rw_));
  }

  cmd->Do(&request_, &response_);

  if (cmd->is_write()) {
    if (cmd->result().ok() && request_.type() != client::Type::SYNC) {
      // Restore Message
      std::string raw_msg(rbuf_ + cur_pos_ - header_len_ - 4, header_len_ + 4);
      zp_data_server->logger_->Lock();
      zp_data_server->logger_->Put(raw_msg);
      zp_data_server->logger_->Unlock();
    }
    // TODO add RecordLock for write cmd
    zp_data_server->mutex_record_.Unlock(cmd->key());
  }

  if (!cmd->is_suspend()) {
    pthread_rwlock_unlock(&(zp_data_server->server_rw_));
  }

  res_ = &response_;

  return 0;
}

