#include "zp_admin.h"

#include <glog/logging.h>
#include "zp_meta_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPMetaServer *zp_meta_server;

void InitServerControlCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
  // Join
  Cmd* joinptr = new JoinCmd(kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(ServerControl::OPCODE::JOIN), joinptr));

  // Ping
  Cmd* pingptr = new PingCmd(kCmdFlagsRead);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(ServerControl::OPCODE::PING), pingptr));
}

Status JoinCmd::Init(const void *buf, size_t count) {
  ServerControl::Join_Request* request = new ServerControl::Join_Request;
  request->ParseFromArray(buf, count);
  
  request_ = request;
  assert(request_ != NULL);

  return Status::OK();
}

void JoinCmd::Do() {
  ServerControl::Join_Request* request = dynamic_cast<ServerControl::Join_Request*>(request_);
  ServerControl::Join_Response* response = new ServerControl::Join_Response;

  slash::Status s;
  Node node(request->node().ip(), request->node().port());

 // slash::MutexLock l(&(zp_meta_server->slave_mutex_));
 // if (!zp_meta_server->FindSlave(node)) {
 //   SlaveItem si;
 //   si.node = node;
 //   si.hb_fd = fd_;
 //   gettimeofday(&si.create_time, NULL);
 //   si.sender = NULL;

 //   LOG(INFO) << "Join a new node(" << node.ip << ", " << node.port << ")";
 //   s = zp_meta_server->AddBinlogSender(si, request->filenum(), request->offset());

 //   if (!s.ok()) {
 //     response->set_status(1);
 //     response->set_msg(result_.ToString());
 //     result_ = slash::Status::Corruption(s.ToString());
 //     LOG(ERROR) << "command failed: Join, caz " << s.ToString();
 //   } else {
 //     response->set_status(0);
 //     DLOG(INFO) << "Join node ok";
 //     result_ = slash::Status::OK();
 //   }
 // }

  response_ = response;
}

Status PingCmd::Init(const void *buf, size_t count) {
  ServerControl::Ping_Request* request = new ServerControl::Ping_Request;
  request->ParseFromArray(buf, count);
  
  request_ = request;
  assert(request_ != NULL);

  return Status::OK();
}

void PingCmd::Do() {
  ServerControl::Ping_Request* request = dynamic_cast<ServerControl::Ping_Request*>(request_);
  ServerControl::Ping_Response* response = new ServerControl::Ping_Response;

  Node node(request->node().ip(), request->node().port());

//  slash::MutexLock l(&(zp_meta_server->slave_mutex_));
//  if (!zp_meta_server->FindSlave(node)) {
//    LOG(WARNING) << "receive Ping from unknown node(" << node.ip << ", " << node.port << ")";
//    response->set_status(1);
//    result_ = slash::Status::Corruption("unrecognized");
//  } else {
//    response->set_status(0);
//    DLOG(INFO) << "receive Ping from node(" << node.ip << ":" << node.port << ")";
//    result_ = slash::Status::OK();
//  }

  response_ = response;
}
