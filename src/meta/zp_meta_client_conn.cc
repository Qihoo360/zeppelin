#include "zp_meta_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "zp_meta_worker_thread.h"
#include "zp_meta_server.h"

extern ZPMetaServer* zp_meta_server;

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
  request_.ParseFromArray(rbuf_ + 4, header_len_);
  // TODO test only
  ZPMeta::MetaCmdResponse_Type response_type;
  switch (request_.type()) {
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_JOIN:
      response_type = ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_JOIN;
      DLOG(INFO) << "Receive join cmd";
      break;
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_PING:
      response_type = ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PING;
      DLOG(INFO) << "Receive ping cmd";
      break;
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_PULL:
      response_type = ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PULL;
      need_redirect = false;
      DLOG(INFO) << "Receive pull cmd";
      break;
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_INIT:
      response_type = ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_INIT;
      DLOG(INFO) << "Receive init cmd";
      break;
    default:
      DLOG(INFO) << "Receive unknow meta cmd";
  }
  
  // Redirect to leader if needed
  set_is_reply(true);
  if (!zp_meta_server->IsLeader() && need_redirect) {
    Status s = zp_meta_server->RedirectToLeader(request_, response_);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to redirect to leader : " << s.ToString();
      response_.set_type(response_type);
      ZPMeta::MetaCmdResponse_Status* status_res = response_.mutable_status();
      status_res->set_code(ZPMeta::StatusCode::kError);
      status_res->set_msg(s.ToString());
      return -1;
    }
    DLOG(INFO) << "Success redirect to leader";
    res_ = &response_;
    return 0;
  }

  Cmd* cmd = self_thread_->GetCmd(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    response_.set_type(response_type);
    ZPMeta::MetaCmdResponse_Status* status_res = response_.mutable_status();
    status_res->set_code(ZPMeta::StatusCode::kError);
    status_res->set_msg("Unknown command");
    return -1;
  }

  cmd->Do(&request_, &response_);
  res_ = &response_;

  return 0;
}
