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
  uint32_t buf;
  memcpy((char *)(&buf), rbuf_ + 4, sizeof(uint32_t));
  uint32_t msg_code = ntohl(buf);
  DLOG(INFO) << "Receiver DealMessage msg_code:" << msg_code;

  self_thread_->PlusThreadQuerynum();

  Cmd* cmd = self_thread_->GetCmd(static_cast<client::OPCODE>(msg_code));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported msg_code: " << msg_code;
    return -1;
  }

  Status s = cmd->Init(rbuf_ + 8, header_len_ - 4);
  if (!s.ok()) {
    LOG(ERROR) << "command Init failed, " << s.ToString();
  }

  DLOG(INFO) << "s1. cmd->Init then Do";
  if (cmd->is_write()) {
    // TODO add RecordLock for write cmd
    zp_data_server->mutex_record_.Lock(cmd->key());
  }

  // do not reply
  set_is_reply(false);
  cmd->Do();

  DLOG(INFO) << "s2. cmd->Do end";

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

  //res_ = cmd->Response();

  return 0;
}
