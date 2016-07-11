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

// Msg is  [ length (int32) | msg_code (int32) | pb_msg (length bytes) ]
int ZPDataClientConn::DealMessage() {
  uint32_t buf;
  memcpy((char *)(&buf), rbuf_ + 4, sizeof(uint32_t));
  uint32_t msg_code = ntohl(buf);
  LOG(INFO) << "DealMessage msg_code:" << msg_code;


  Cmd* cmd = self_thread_->GetCmd(static_cast<client::OPCODE>(msg_code));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported msg_code: " << msg_code;
    return -1;
  }

  Status s = cmd->Init(rbuf_ + 8, header_len_ - 4);
  if (!s.ok()) {
    LOG(ERROR) << "command Init failed, " << s.ToString();
  }

  if (cmd->is_write()) {
    // TODO add RecordLock for write cmd
    zp_data_server->mutex_record_.Lock(cmd->key());
  }

  set_is_reply(true);

  // TODO  change the protocol
  cmd->Do(NULL, NULL);

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

  res_ = cmd->Response();

  return 0;
}

