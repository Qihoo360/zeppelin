#include "src/meta/zp_meta_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "src/meta/zp_meta_server.h"

extern ZPMetaServer* g_meta_server;

////// ZPMetaServerHandle //////
void ZPMetaServerHandle::CronHandle() const {
  g_meta_server->ResetLastSecQueryNum();
  LOG(INFO) << "ServerQueryNum: " << g_meta_server->query_num()
      << " ServerCurrentQps: " << g_meta_server->last_qps();

  // Check alive
  g_meta_server->CheckNodeAlive();
  //g_meta_server->DebugOffset();
  g_meta_server->ScheduleUpdate();
}

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
  bool need_redirect = true;
  if (!request_.ParseFromArray(rbuf_ + 4, header_len_)) {
    LOG(INFO) << "DealMessage, Invalid pb message";
    return -1;
  }

  ZPMeta::Type response_type = request_.type();
  switch (request_.type()) {
    case ZPMeta::Type::PING:
      DLOG(INFO) << "Receive ping cmd";
      break;
    case ZPMeta::Type::INIT:
      DLOG(INFO) << "Receive init cmd";
      break;
    // Cmds do not need redirect
    case ZPMeta::Type::PULL:
      need_redirect = false;
      DLOG(INFO) << "Receive pull cmd";
      break;
    case ZPMeta::Type::LISTTABLE:
    case ZPMeta::Type::LISTNODE:
    case ZPMeta::Type::LISTMETA:
    case ZPMeta::Type::METASTATUS:
      need_redirect = false;
      DLOG(INFO) << "Receive LIST (Table/Node/Meta/MetaStatus) cmd";
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
