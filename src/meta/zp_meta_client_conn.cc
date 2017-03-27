#include "zp_meta_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "zp_meta_worker_thread.h"
#include "zp_meta_server.h"

extern ZPMetaServer* g_meta_server;

////// ZPDataClientConn //////
ZPMetaClientConn::ZPMetaClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = dynamic_cast<ZPMetaWorkerThread*>(thread);
}

ZPMetaClientConn::~ZPMetaClientConn() {
}

// Msg is  [ length (int32) | pb_msg (length bytes) ]
int ZPMetaClientConn::DealMessage() {
  response_.Clear();
  bool need_redirect = true;
  bool ret = request_.ParseFromArray(rbuf_ + 4, header_len_);
  if (!ret) {
    LOG(INFO) << "DealMessage, Invalid pb message";
  }
  // TODO test only
  ZPMeta::Type response_type;
  switch (request_.type()) {
    case ZPMeta::Type::PING:
      response_type = ZPMeta::Type::PING;
      DLOG(INFO) << "Receive ping cmd";
      break;
    case ZPMeta::Type::PULL:
      response_type = ZPMeta::Type::PULL;
      need_redirect = false;
      DLOG(INFO) << "Receive pull cmd";
      break;
    case ZPMeta::Type::INIT:
      response_type = ZPMeta::Type::INIT;
      DLOG(INFO) << "Receive init cmd";
      break;
    default:
      DLOG(INFO) << "Receive unknow meta cmd";
  }
  
  // Redirect to leader if needed
  set_is_reply(true);
  if (!g_meta_server->IsLeader() && need_redirect) {
    Status s = g_meta_server->RedirectToLeader(request_, &response_);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to redirect to leader : " << s.ToString();
      response_.set_type(response_type);
      response_.set_code(ZPMeta::StatusCode::ERROR);
      response_.set_msg(s.ToString());
      res_ = &response_;
      return -1;
    }
    DLOG(INFO) << "Success redirect to leader";
    res_ = &response_;
    return 0;
  }

  Cmd* cmd = g_meta_server->GetCmd(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    response_.set_type(response_type);
    response_.set_code(ZPMeta::StatusCode::ERROR);
    response_.set_msg("Unknown command");
    res_ = &response_;
    return -1;
  }

  cmd->Do(&request_, &response_);
  res_ = &response_;

  return 0;
}
