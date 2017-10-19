// Copyright 2017 Qihoo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "src/meta/zp_meta_command.h"

#include <set>
#include <string>
#include <vector>
#include <unordered_map>

#include "glog/logging.h"
#include "slash/include/slash_string.h"
#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_server.h"

extern ZPMetaServer *g_meta_server;

void PingCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);

  // Update Node Info
  g_meta_server->UpdateNodeInfo(request->ping());

  ZPMeta::MetaCmdResponse_Ping* ping = response->mutable_ping();
  ping->set_version(g_meta_server->epoch());
  response->set_type(ZPMeta::Type::PING);
  response->set_code(ZPMeta::StatusCode::OK);
  response->set_msg("Ping OK!");

  DLOG(INFO) << "Receive ping from node: " << request->ping().node().ip()
    << ":" << request->ping().node().port()
    << ", version=" << request->ping().version()
    << ", response epoch=" << g_meta_server->epoch();
}

void PullCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);
  response->set_type(ZPMeta::Type::PULL);
  ZPMeta::MetaCmdResponse_Pull* ms_info = response->mutable_pull();

  Status s = Status::InvalidArgument("error argument");
  if (request->pull().has_name()) {
    std::string raw_table = request->pull().name();
    std::string table = slash::StringToLower(raw_table);
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

  if (!s.ok()) {
    response->release_pull();
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  } else {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("Pull Ok!");
  }
}

void InitCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string raw_table = request->init().name();
  std::string table = slash::StringToLower(raw_table);
  int pnum = request->init().num();
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::INIT);
  if (table.empty()) {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg("TableName cannot be empty");
    return;
  }

  if (pnum <= 0) {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg("Invalid partition number");
    return;
  }

  Status s = g_meta_server->CreateTable(table, pnum);
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("Init OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void SetMasterCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->set_master().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->set_master().basic().partition();
  ZPMeta::Node node = request->set_master().basic().node();
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::SETMASTER);
  Status s = g_meta_server->WaitSetMaster(node, table, p);
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("SetMaster OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void AddSlaveCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->add_slave().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->add_slave().basic().partition();
  ZPMeta::Node node = request->add_slave().basic().node();
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::ADDSLAVE);
  Status s = g_meta_server->AddPartitionSlave(table, p, node);
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("AddSlave OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void RemoveSlaveCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  std::string name = request->remove_slave().basic().name();
  std::string table = slash::StringToLower(name);
  int p = request->remove_slave().basic().partition();
  ZPMeta::Node node = request->remove_slave().basic().node();
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::REMOVESLAVE);

  Status s = g_meta_server->RemovePartitionSlave(table, p, node);
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("RemoveSlave OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void ListTableCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmdResponse_ListTable *table = response->mutable_list_table();

  response->set_type(ZPMeta::Type::LISTTABLE);

  std::set<std::string> table_list;
  Status s = g_meta_server->GetTableList(&table_list);

  if (s.ok()) {
    ZPMeta::TableName *p = table->mutable_tables();
    for (const auto& t : table_list) {
      p->add_name(t);
    }
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("ListTable OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void DropTableCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);
  response->set_type(ZPMeta::Type::DROPTABLE);

  std::string table_name = request->drop_table().name();
  if (table_name.empty()) {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg("TableName cannot be empty");
    return;
  }

  Status s = g_meta_server->DropTable(table_name);
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("DropTable OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void ListNodeCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmdResponse_ListNode *lnodes = response->mutable_list_node();
  ZPMeta::Nodes *nodes = lnodes->mutable_nodes();
  response->set_type(ZPMeta::Type::LISTNODE);

  std::unordered_map<std::string, NodeInfo> node_list;
  Status s = g_meta_server->GetNodeStatusList(&node_list);

  if (s.ok()) {
    int port = 0;
    std::string ip;
    for (const auto& ni : node_list) {
      ZPMeta::NodeStatus* node_status = nodes->add_nodes();
      ZPMeta::Node* n = node_status->mutable_node();
      slash::ParseIpPortString(ni.first, ip, port);
      n->set_ip(ip);
      n->set_port(port);
      if (ni.second.last_alive_time > 0) {
        node_status->set_status(ZPMeta::NodeState::UP);
      } else {
        node_status->set_status(ZPMeta::NodeState::DOWN);
      }
    }
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("ListNode OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void ListMetaCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmdResponse_ListMeta *metas = response->mutable_list_meta();

  response->set_type(ZPMeta::Type::LISTMETA);

  Status s = g_meta_server->GetAllMetaNodes(metas);

  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("ListMeta OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void MetaStatusCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmdResponse_MetaStatus* ms = response->mutable_meta_status();

  response->set_type(ZPMeta::Type::METASTATUS);

  Status s = g_meta_server->GetMetaStatus(ms);
 
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("Get MetaStatus OK");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}

void MigrateCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmd_Migrate migrate = request->migrate();
  response->set_type(ZPMeta::Type::MIGRATE);

  std::vector<ZPMeta::RelationCmdUnit> diffs;
  for (const auto& item : migrate.diff()) {
    if (item.left().ip() == item.right().ip()
        && item.left().port() == item.right().port()) {
      // Skip same node
      LOG(WARNING) << "Skip diff item with same origin and target node. "
        << item.table() << "_" << item.partition()
        << ", from: " << item.left().ip() << "_" << item.left().port()
        << ", to: " << item.right().ip() << "_" << item.right().port();
      continue;
    }
    if (!g_meta_server->IsCharged(item.table(), item.partition(), item.left())
        || g_meta_server->IsCharged(item.table(), item.partition(),
          item.right())) {
      // Skip invalid diff item
      LOG(WARNING) << "Skip diff invalid diff item. "
        << item.table() << "_" << item.partition()
        << ", from: " << item.left().ip() << "_" << item.left().port()
        << ", to: " << item.right().ip() << "_" << item.right().port();
      continue;
    }
    diffs.push_back(item);
    LOG(INFO) << "Migrate diff item: "
      << item.table() << "_" << item.partition()
      << ", from: " << item.left().ip() << "_" << item.left().port()
      << ", to: " << item.right().ip() << "_" << item.right().port();
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
  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);

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

void RemoveNodesCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const ZPMeta::MetaCmd* request = static_cast<const ZPMeta::MetaCmd*>(req);

  std::vector<ZPMeta::Node> nodes;
  for (int i = 0; i < request->remove_nodes().nodes_size(); i++) {
    nodes.push_back(request->remove_nodes().nodes(i));
  }

  ZPMeta::MetaCmdResponse* response
    = static_cast<ZPMeta::MetaCmdResponse*>(res);

  response->set_type(ZPMeta::Type::REMOVENODES);

  Status s = g_meta_server->RemoveNodes(nodes);
  if (s.ok()) {
    response->set_code(ZPMeta::StatusCode::OK);
    response->set_msg("RemoveNodes OK!");
  } else {
    response->set_code(ZPMeta::StatusCode::ERROR);
    response->set_msg(s.ToString());
  }
}
