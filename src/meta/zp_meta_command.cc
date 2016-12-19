#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/repeated_field.h>
#include "slash_string.h"

#include "zp_meta.pb.h"
#include "zp_meta_command.h"
#include "zp_meta_server.h"

extern ZPMetaServer *g_meta_server;

void PingCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::PING);
  
  // Update Ping time
  std::string node = slash::IpPortString(request->ping().node().ip(),
                                         request->ping().node().port());
  
  Status s  = g_meta_server->AddNodeAlive(node);
  
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("Ping OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }

  ZPMeta::MetaCmdResponse_Ping* ping = response->mutable_ping();
  ping->set_version(g_meta_server->version());

  DLOG(INFO) << "Receive ping from node: " << request->ping().node().ip()
             << ":" << request->ping().node().port() << ", version=" << request->ping().version()
             << ", response version=" << g_meta_server->version();
}

void PullCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::set<std::string> tables;
  g_meta_server->InitVersionIfNeeded();
  if (request->pull().has_name()) {
    std::string name = request->pull().name();
    tables.insert(slash::StringToLower(name));
  } else if (request->pull().has_node()) {
    std::string ip_port = request->pull().node().ip() + ":" + std::to_string(request->pull().node().port());
    g_meta_server->GetTableListForNode(ip_port, &tables);
  }

  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::PULL);

  ZPMeta::MetaCmdResponse_Pull ms_info;
  Status s = g_meta_server->GetMSInfo(tables, &ms_info);
  LOG(INFO) << "Pull table num: " << ms_info.info_size();
  if (!s.ok()) {
    ms_info.set_version(-1);
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  } else {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("Pull Ok!");
  }

  ZPMeta::MetaCmdResponse_Pull* pull = response->mutable_pull();
  pull->CopyFrom(ms_info);

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

  response->set_type(ZPMeta::Type::INIT);

  Status s = g_meta_server->Distribute(table, request->init().num());

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("Init OK!");
    DLOG(INFO) << "Init, table: " << table << " partition num: " << request->init().num();
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void SetMasterCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->set_master().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->set_master().basic().partition();
  ZPMeta::Node node = request->set_master().basic().node();
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::SETMASTER);

  Status s = g_meta_server->SetMaster(table, p, node);

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("SetMaster OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void AddSlaveCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->add_slave().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->add_slave().basic().partition();
  ZPMeta::Node node = request->add_slave().basic().node();
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::ADDSLAVE);

  Status s = g_meta_server->AddSlave(table, p, node);

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("AddSlave OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void RemoveSlaveCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->remove_slave().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->remove_slave().basic().partition();
  ZPMeta::Node node = request->remove_slave().basic().node();
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::REMOVESLAVE);

  Status s = g_meta_server->RemoveSlave(table, p, node);

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("RemoveSlave OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void ListTableCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmdResponse_ListTable *table_name = response->mutable_list_table();

  response->set_type(ZPMeta::Type::LISTTABLE);

  Status s = g_meta_server->GetTableList(table_name);

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("ListTable OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void ListNodeCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmdResponse_ListNode *nodes = response->mutable_list_node();

  response->set_type(ZPMeta::Type::LISTNODE);

  Status s = g_meta_server->GetAllNodes(nodes);

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("ListNode OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}
