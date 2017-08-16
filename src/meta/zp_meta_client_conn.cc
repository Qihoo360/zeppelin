#include "src/meta/zp_meta_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "src/meta/zp_meta_server.h"

extern ZPMetaServer* g_meta_server;

////// ZPDataClientConn //////
ZPMetaClientConn::ZPMetaClientConn(int fd, const std::string& ip_port,
    pink::ServerThread* server_thread)
  : PbConn(fd, ip_port, server_thread) {
}

ZPMetaClientConn::~ZPMetaClientConn() {
}

// Msg is  [ length (int32) | pb_msg (length bytes) ]
int ZPMetaClientConn::DealMessage() {
  response_.Clear();
  if (!request_.ParseFromArray(rbuf_ + 4, header_len_)) {
    LOG(INFO) << "DealMessage, Invalid pb message";
    return -1;
  }

  Cmd* cmd = g_meta_server->GetCmd(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    response_.set_type(request_.type());
    response_.set_code(ZPMeta::StatusCode::ERROR);
    response_.set_msg("Unknown command");
    res_ = &response_;
    return -1;
  }

  // Redirect to leader if needed
  set_is_reply(true);
  if (cmd->is_redirect()
      && !g_meta_server->IsLeader()) {
    Status s = g_meta_server->RedirectToLeader(request_, &response_);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to redirect to leader : " << s.ToString();
      response_.set_type(request_.type());
      response_.set_code(ZPMeta::StatusCode::ERROR);
      response_.set_msg(s.ToString());
      res_ = &response_;
      return -1;
    }
    res_ = &response_;
    return 0;
  }

  cmd->Do(&request_, &response_);
  res_ = &response_;
  return 0;
}
