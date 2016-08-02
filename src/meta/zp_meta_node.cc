#include <glog/logging.h>
#include "slash_string.h"

#include "zp_meta.pb.h"
#include "zp_meta_node.h"
#include "zp_meta_server.h"

extern ZPMetaServer *zp_meta_server;

void JoinCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  ZPMeta::MetaCmd* request = static_cast<ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_JOIN);

  zp_meta_server->AddNodeAlive(slash::IpPortString(request->join().node().ip(), request->join().node().port()));
  status_res->set_code(ZPMeta::StatusCode::kOk);
  status_res->set_msg("Join OK!");
  result_ = slash::Status::OK();
  DLOG(INFO) << "Join node: " << request->join().node().ip() << ":"
             << request->join().node().port();
}

void PingCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  ZPMeta::MetaCmd* request = static_cast<ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PING);
  
  // Update Ping time
  std::string node = slash::IpPortString(request->ping().node().ip(),
                                         request->ping().node().port());
  
  zp_meta_server->UpdateNodeAlive(node);
  
  status_res->set_code(ZPMeta::StatusCode::kOk);
  status_res->set_msg("Ping OK!");
  result_ = slash::Status::OK();
  DLOG(INFO) << "Receive ping from node: " << request->ping().node().ip()
             << ":" << request->ping().node().port();
}

