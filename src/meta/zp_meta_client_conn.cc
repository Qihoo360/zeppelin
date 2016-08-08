#include "zp_meta_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "zp_meta_worker_thread.h"
#include "zp_meta_server.h"

extern ZPMetaServer* zp_meta_server;

////// ZPDataClientConn //////
ZPMetaClientConn::ZPMetaClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port), leader_cli_(NULL), leader_cmd_port_(0) {
  self_thread_ = dynamic_cast<ZPMetaWorkerThread*>(thread);
}

ZPMetaClientConn::~ZPMetaClientConn() {
  LeaderClean();
}

bool ZPMetaClientConn::IsLeader() {
  std::string leader_ip;
  int leader_port = 0, leader_cmd_port = 0;
  while (!zp_meta_server->GetLeader(leader_ip, leader_port)) {
    DLOG(INFO) << "Wait leader ... ";
    // Wait leader election
    sleep(1);
  }
  LOG(INFO) << "Leader: " << leader_ip << ":" << leader_port;
  leader_cmd_port = leader_port + kMetaPortShiftCmd;
  if (leader_ip == leader_ip_ && leader_cmd_port == leader_cmd_port_) {
    // has connected to leader
    return false;
  }
  
  // Leader changed
  LeaderClean();
  if (leader_ip == zp_meta_server->local_ip() && 
      leader_port == zp_meta_server->local_port()) {
    // I am Leader
    return true;
  }
  
  // Connect to new leader
  leader_cli_ = new pink::PbCli();
  leader_ip_ = leader_ip;
  leader_cmd_port_ = leader_cmd_port;
  pink::Status s = leader_cli_->Connect(leader_ip_, leader_cmd_port_);
  if (!s.ok()) {
    LOG(ERROR) << "connect to leader: " << leader_ip_ << ":" << leader_cmd_port_ << " failed";
  }
  leader_cli_->set_send_timeout(1000);
  leader_cli_->set_recv_timeout(1000);
  return false;
}


// Msg is  [ length (int32) | pb_msg (length bytes) ]
int ZPMetaClientConn::DealMessage() {
  request_.ParseFromArray(rbuf_ + 4, header_len_);
  // TODO test only
  switch (request_.type()) {
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_JOIN:
      DLOG(INFO) << "Receive join cmd";
      break;
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_PING:
      DLOG(INFO) << "Receive ping cmd";
      break;
    default:
      DLOG(INFO) << "Receive unknow meta cmd";
  }
  
  // Redirect to leader if needed
  if (!IsLeader()) {
    pink::Status s = leader_cli_->Send(&request_);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to redirect message to leader, " << s.ToString();
      return -1;
    }
    s = leader_cli_->Recv(&response_); 
    if (!s.ok()) {
      LOG(ERROR) << "Failed to get redirect message response from leader" << s.ToString();
      return -1;
    }
    LOG(INFO) << "Receive redirect message response from leader";
    return 0;
  }

  Cmd* cmd = self_thread_->GetCmd(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported type: " << (int)request_.type();
    return -1;
  }

  cmd->Do(&request_, &response_);
  set_is_reply(true);

  res_ = &response_;

  return 0;
}

