#include "zp_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "zp_worker_thread.h"
#include "zp_server.h"

extern ZPServer* zp_server;

////// ZPClientConn //////
ZPClientConn::ZPClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = dynamic_cast<ZPWorkerThread*>(thread);
}

ZPClientConn::~ZPClientConn() {
}

// Msg is  [ length (int32) | msg_code (int32) | pb_msg (length bytes) ]
int ZPClientConn::DealMessage() {
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
  }

  set_is_reply(true);
  cmd->Do();

  if (cmd->is_write()) {
    if (cmd->result().ok()) {
      // Restore Message
      std::string raw_msg(rbuf_, header_len_);
      zp_server->logger_->Lock();
      zp_server->logger_->Put(raw_msg);
      zp_server->logger_->Unlock();
    }
    // TODO add RecordLock for write cmd
  }

  res_ = cmd->Response();

  return 0;
}

