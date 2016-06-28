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

  set_is_reply(true);
  cmd->Do();

  res_ = cmd->Response();

//  switch (msg_code) {
//    case client::OPCODE::Set: {
//      break;
//    }
//
//    case client::OPCODE::Get: {
//      client::Get_Request request;
//      client::Get_Response* response = new client::Get_Response;
//      request.ParseFromArray(rbuf_ + 8, header_len_ - 4);
//
//      std::string value;
//      Status result = server_thGet_->server_->floyd_->Get(request.key(), value);
//      if (!result.ok()) {
//        response->set_status(1);
//        LOG_ERROR("Get failed %s", result.ToString().c_str());
//      } else if (result.ok()) {
//        response->set_status(0);
//        response->set_value(value);
//        LOG_INFO ("Get key(%s) ok!", request.key().c_str());
//      }
//      res_ = response;
//      break;
//    }
//
//    default:
//      LOG_INFO ("invalid msg_code %d\n", msg_code);
//      break;
//  }

  return 0;
}

