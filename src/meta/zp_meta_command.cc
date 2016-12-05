#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/repeated_field.h>
#include "slash_string.h"

#include "zp_meta.pb.h"
#include "zp_meta_command.h"
#include "zp_meta_server.h"

extern ZPMetaServer *zp_meta_server;

void PingCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PING);
  
  // Update Ping time
  std::string node = slash::IpPortString(request->ping().node().ip(),
                                         request->ping().node().port());
  
  Status s  = zp_meta_server->AddNodeAlive(node);
  
  if (s.ok()) {
    status_res->set_code(ZPMeta::StatusCode::kOk);
    status_res->set_msg("Ping OK!");
  } else {
    status_res->set_code(ZPMeta::StatusCode::kError);
    status_res->set_msg(s.ToString());
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
  std::set<std::string> tables;
  zp_meta_server->InitVersionIfNeeded();
  if (request->pull().has_name()) {
    std::string name = request->pull().name();
    tables.insert(slash::StringToLower(name));
  } else if (request->pull().has_node()) {
    std::string ip_port = request->pull().node().ip() + ":" + std::to_string(request->pull().node().port());
    zp_meta_server->GetTableListForNode(ip_port, tables);
  }

  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PULL);

  ZPMeta::MetaCmdResponse_Pull ms_info;
  Status s = zp_meta_server->GetMSInfo(tables, ms_info);
  LOG(INFO) << "Pull table num: " << ms_info.info_size();
  if (!s.ok()) {
    ms_info.set_version(-1);
    status_res->set_code(ZPMeta::StatusCode::kError);
    status_res->set_msg(s.ToString());
  } else {
    status_res->set_code(ZPMeta::StatusCode::kOk);
    status_res->set_msg("Pull Ok!");
  }

  ZPMeta::MetaCmdResponse_Pull* pull = response->mutable_pull();
  pull->CopyFrom(ms_info);

  // TODO rm
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(*pull, &text_format);
  DLOG(INFO) << "pull : [" << text_format << "]";

  
  DLOG(INFO) << "Receive Pull from client";
}

void InitCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->init().name();
  std::string table = slash::StringToLower(name);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_INIT);

  Status s = zp_meta_server->Distribute(table, request->init().num());

  if (s.ok()) {
    status_res->set_code(ZPMeta::StatusCode::kOk);
    status_res->set_msg("Init OK!");
    DLOG(INFO) << "Init, table: " << table << " partition num: " << request->init().num();
  } else {
    status_res->set_code(ZPMeta::StatusCode::kError);
    status_res->set_msg(s.ToString());
  }
}
