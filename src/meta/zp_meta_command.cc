#include <glog/logging.h>
#include "slash_string.h"

#include "zp_meta.pb.h"
#include "zp_meta_command.h"
#include "zp_meta_server.h"

extern ZPMetaServer *zp_meta_server;

void JoinCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_JOIN);

  Status s = zp_meta_server->AddNodeAlive(slash::IpPortString(request->join().node().ip(), request->join().node().port()));
  if (s.ok()) {
    status_res->set_code(ZPMeta::StatusCode::kOk);
    status_res->set_msg("Join OK!");
  } else {
    status_res->set_code(ZPMeta::StatusCode::kError);
    status_res->set_msg(s.ToString());
  }
  DLOG(INFO) << "Join node: " << request->join().node().ip() << ":"
             << request->join().node().port();
}

void PingCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PING);
  
  // Update Ping time
  std::string node = slash::IpPortString(request->ping().node().ip(),
                                         request->ping().node().port());
  
  bool up_ret = zp_meta_server->UpdateNodeAlive(node);
  
  if (up_ret) {
    status_res->set_code(ZPMeta::StatusCode::kOk);
    status_res->set_msg("Ping OK!");
  } else {
    status_res->set_code(ZPMeta::StatusCode::kNotFound);
    status_res->set_msg("you are not in alive_nodes!");
  }

  ZPMeta::MetaCmdResponse_Ping* ping = response->mutable_ping();
  ping->set_version(zp_meta_server->version());

  DLOG(INFO) << "Receive ping from node: " << request->ping().node().ip()
             << ":" << request->ping().node().port() << ", version=" << request->ping().version()
             << ", response version=" << zp_meta_server->version();
}

void PullCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  assert(request->type() == ZPMeta::MetaCmd_Type_PULL);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PULL);

  ZPMeta::MetaCmdResponse_Pull ms_info;
  Status s = zp_meta_server->GetMSInfo(ms_info);
  LOG(INFO) << "Pull size: " << ms_info.info().size();
  if (!s.ok()) {
    ms_info.set_version(-1);
    status_res->set_code(ZPMeta::StatusCode::kError);
    status_res->set_msg(s.ToString());
  } else {
    status_res->set_code(ZPMeta::StatusCode::kOk);
    status_res->set_msg("Pull Ok!");
  }

  ZPMeta::MetaCmdResponse_Pull* pull = response->mutable_pull();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PULL);
  pull->CopyFrom(ms_info);
  
  DLOG(INFO) << "Receive Pull from client";
}

void InitCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_INIT);

  Status s = zp_meta_server->Distribute(request->init().num());

  if (s.ok()) {
    status_res->set_code(ZPMeta::StatusCode::kOk);
    status_res->set_msg("Init OK!");
  } else {
    status_res->set_code(ZPMeta::StatusCode::kError);
    status_res->set_msg(s.ToString());
  }
  DLOG(INFO) << "Init, partition num: " << request->init().num();
}
