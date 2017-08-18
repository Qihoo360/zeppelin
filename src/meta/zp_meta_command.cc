#include "src/meta/zp_meta_command.h"

#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/repeated_field.h>

#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_server.h"

#include "slash/include/slash_string.h"

extern ZPMetaServer *g_meta_server;

void PingCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  // Update Ping time
  std::string node = slash::IpPortString(request->ping().node().ip(),
      request->ping().node().port());
  g_meta_server->UpdateNodeAlive(node);

  // Update node offset
  g_meta_server->UpdateOffset(request->ping());

  response->set_type(ZPMeta::Type::PING);
  response->set_code(ZPMeta::StatusCode::OK);
  response->set_msg("Ping OK!");
  ZPMeta::MetaCmdResponse_Ping* ping = response->mutable_ping();
  ping->set_version(g_meta_server->version());

  DLOG(INFO) << "Receive ping from node: " << request->ping().node().ip()
    << ":" << request->ping().node().port()
    << ", version=" << request->ping().version()
    << ", response version=" << g_meta_server->version();
}

void PullCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);
  response->set_type(ZPMeta::Type::PULL);
  
  Status s = Status::InvalidArgument("error argument");
  ZPMeta::MetaCmdResponse_Pull* ms_info = response->mutable_pull();
  if (request->pull().has_name()) {
    std::string table = slash::StringToLower(request->pull().name());
    s = g_meta_server->GetMetaInfoByTable(table, ms_info);
    if (!s.ok()) {
      LOG(WARNING) << "Pull by table failed: " << s.ToString()
        << ", Table: " << table;
    }
  } else if (request->pull().has_node()) {
    std::string ip_port = slash::IpPortString(request->pull().node().ip(),
        request->pull().node().port());
    s = g_meta_server->GetMetaInfoByNode(ip_port, ms_info);
    if (!s.ok()) {
      LOG(WARNING) << "Pull by node failed: " << s.ToString()
        << ", Node: " << ip_port;
    }
  }

  if (!s.ok() && !s.IsNotFound()) {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  } else {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("Pull Ok!");
  }
}

void InitCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string table = slash::StringToLower(request->init().name());
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::INIT);
  if (table.empty()) {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg("TableName cannot be empty");
    return;
  }

  // Update command such like Init, DropTable, SetMaster, AddSlave and RemoveSlave
  // were handled asynchronously
  g_meta_server->update_thread()->PendingUpdate(
      UpdateTask(kOpAddTable, "", table, request->init().num()));

  response->set_code(ZPMeta::StatusCode::OK);
  response->set_msg("Init OK!");
}

void SetMasterCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->set_master().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->set_master().basic().partition();
  ZPMeta::Node node = request->set_master().basic().node();
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::SETMASTER);
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  g_meta_server->update_thread()->PendingUpdate(
      UpdateTask(kOpSetMaster, ip_port, table, p));

  response->set_code(ZPMeta::StatusCode::OK);
  response->set_msg("SetMaster OK!");
}

void AddSlaveCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->add_slave().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->add_slave().basic().partition();
  ZPMeta::Node node = request->add_slave().basic().node();
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::ADDSLAVE);
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  g_meta_server->update_thread()->PendingUpdate(
      UpdateTask(kOpAddSlave, ip_port, table, p));

  response->set_code(ZPMeta::StatusCode::OK);
  response->set_msg("AddSlave OK!");
}

void RemoveSlaveCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->remove_slave().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->remove_slave().basic().partition();
  ZPMeta::Node node = request->remove_slave().basic().node();
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::REMOVESLAVE);
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  g_meta_server->update_thread()->PendingUpdate(
      UpdateTask(kOpRemoveSlave, ip_port, table, p));

  response->set_code(ZPMeta::StatusCode::OK);
  response->set_msg("RemoveSlave OK!");
}

void ListTableCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
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

void ListMetaCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmdResponse_ListMeta *nodes = response->mutable_list_meta();

  response->set_type(ZPMeta::Type::LISTMETA);

  Status s = g_meta_server->GetAllMetaNodes(nodes);

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("ListMeta OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void MetaStatusCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::METASTATUS);

  std::string result;
  Status s = g_meta_server->GetMetaStatus(&result);

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg(result);
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}


void DropTableCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);
  response->set_type(ZPMeta::Type::DROPTABLE);
  
  std::string table_name = request->drop_table().name();
  if (table_name.empty()) {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg("TableName cannot be empty");
    return;
  }

  Status s = g_meta_server->update_thread()->PendingUpdate(
      UpdateTask(kOpRemoveTable, table_name));

  response->set_code(ZPMeta::StatusCode::OK);
  response->set_msg("DropTable OK!");
}

void MigrateCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);
  
  ZPMeta::MetaCmd_Migrate migrate = request->migrate();
  response->set_type(ZPMeta::Type::MIGRATE);

  std::vector<ZPMeta::RelationCmdUnit> diffs;
  for (const auto& item : migrate.diff()) {
    diffs.push_back(item);
  }

  if (diffs.empty()) {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg("No diff item provided");
    return;
  }

  Status s = g_meta_server->Migrate(migrate.origin_epoch(), diffs);
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("Migrage OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void CancelMigrateCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::CANCELMIGRATE);
  
  Status s = g_meta_server->CancelMigrate();
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("CancelMigrage OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

//void CheckMigrateCmd::Do(const google::protobuf::Message *req,
//    google::protobuf::Message *res, void* partition = NULL) const {
//
//}
