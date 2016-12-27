/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */
#include "include/zp_cluster.h"

#include <google/protobuf/text_format.h>
#include<iostream>
#include<string>


namespace libzp {

Cluster::Cluster(const Options& options) {
  meta_addr_ = options.meta_addr;
  if (meta_addr_.size() == 0) {
    fprintf(stderr, "you need input at least 1 meta node address:\n");
  }
  InitParam();
}

Cluster::Cluster(const std::string& ip, const int port) {
  meta_addr_.push_back(Node(ip, port));
  InitParam();
}

void Cluster::InitParam() {
  data_cmd_ = client::CmdRequest();
  data_res_ = client::CmdResponse();
  meta_cmd_ = ZPMeta::MetaCmd();
  meta_res_ = ZPMeta::MetaCmdResponse();
  epoch_ = 0;
  tables_ = new std::unordered_map<std::string, Table*>();
  meta_pool_ = new ConnectionPool();
  data_pool_ = new ConnectionPool();
}

Cluster::~Cluster() {
  std::unordered_map<std::string, Table*>::iterator iter = tables_->begin();
  while (iter != tables_->end()) {
    delete iter->second;
    iter++;
  }
  delete tables_;
  delete meta_pool_;
  delete data_pool_;
}


Status Cluster::Set(const std::string& table, const std::string& key,
    const std::string& value) {
  Status s;

  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::SET);
  client::CmdRequest_Set* set_info = data_cmd_.mutable_set();
  set_info->set_table_name(table);
  set_info->set_key(key);
  set_info->set_value(value);
  s = SubmitDataCmd(table, key);
  if (s.IsIOError()) {
    return s;
  }
  if (data_res_.code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::NotSupported(data_res_.msg());
  }
}

Status Cluster::Delete(const std::string& table, const std::string& key) {
  Status s;
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::DEL);
  client::CmdRequest_Del* del_info = data_cmd_.mutable_del();
  del_info->set_table_name(table);
  del_info->set_key(key);
  s = SubmitDataCmd(table, key);
  if (s.IsIOError()) {
    return s;
  }
  if (data_res_.code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::NotSupported(data_res_.msg());
  }
}



Status Cluster::Get(const std::string& table, const std::string& key,
    std::string* value) {
  Status s;
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::GET);
  client::CmdRequest_Get* get_cmd = data_cmd_.mutable_get();
  get_cmd->set_table_name(table);
  get_cmd->set_key(key);

  s = SubmitDataCmd(table, key);

  if (s.IsIOError()) {
    return s;
  }
  if (data_res_.code() == client::StatusCode::kOk) {
    client::CmdResponse_Get info = data_res_.get();
    value->assign(info.value().data(), info.value().size());
    return Status::OK();
  } else if (data_res_.code() == client::StatusCode::kNotFound) {
    return Status::NotFound("key do not exist");
  } else {
    return Status::IOError(data_res_.msg());
  }
}

Status Cluster::CreateTable(const std::string& table_name,
    const int partition_num) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::INIT);
  ZPMeta::MetaCmd_Init* init = meta_cmd_.mutable_init();
  init->set_name(table_name);
  init->set_num(partition_num);

  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  } else {
    return Status::OK();
  }
}


// TODO(all) pull meta_info from data
Status Cluster::Connect() {
  Status s;
  int attempt_count = 0;
  Node meta;
  ZpConn* meta_cli = meta_pool_->GetExistConnection();
  if (meta_cli != NULL) {
    return Status::OK();
  }
  while (attempt_count++ < kMetaAttempt) {
    meta = GetRandomMetaAddr();
    meta_cli = meta_pool_->GetConnection(meta);
    if (meta_cli) {
      return Status::OK();
    }
  }
  return Status::IOError("can't connect meta after attempts");
}

Status Cluster::Pull(const std::string& table) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::PULL);
  ZPMeta::MetaCmd_Pull* pull = meta_cmd_.mutable_pull();
  pull->set_name(table);
  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return ret;
  }

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotFound(meta_res_.msg());
  }

  ZPMeta::MetaCmdResponse_Pull info = meta_res_.pull();

  // update clustermap now
  if (info.info_size() == 0) {
    return Status::NotFound("table does not exist in meta");
  }
  return ResetClusterMap(info);
}

Status Cluster::SetMaster(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::SETMASTER);
  ZPMeta::MetaCmd_SetMaster* set_master_cmd = meta_cmd_.mutable_set_master();
  ZPMeta::BasicCmdUnit* set_master_entity = set_master_cmd->mutable_basic();
  set_master_entity->set_name(table_name);
  set_master_entity->set_partition(partition_num);
  ZPMeta::Node* node = set_master_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  } else {
    return Status::OK();
  }
}
Status Cluster::AddSlave(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::ADDSLAVE);
  ZPMeta::MetaCmd_AddSlave* add_slave_cmd = meta_cmd_.mutable_add_slave();
  ZPMeta::BasicCmdUnit* add_slave_entity = add_slave_cmd->mutable_basic();
  add_slave_entity->set_name(table_name);
  add_slave_entity->set_partition(partition_num);
  ZPMeta::Node* node = add_slave_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::RemoveSlave(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::REMOVESLAVE);
  ZPMeta::MetaCmd_RemoveSlave* remove_slave_cmd =
    meta_cmd_.mutable_remove_slave();
  ZPMeta::BasicCmdUnit* remove_slave_entity = remove_slave_cmd->mutable_basic();
  remove_slave_entity->set_name(table_name);
  remove_slave_entity->set_partition(partition_num);
  ZPMeta::Node* node = remove_slave_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::ListMeta(Node* master, std::vector<Node>* nodes) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::LISTMETA);

  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  }
  ZPMeta::MetaNodes info = meta_res_.list_meta().nodes();
  master->ip = info.leader().ip();
  master->port = info.leader().port();
  for (int i = 0; i < info.followers_size(); i++) {
    Node slave_node;
    slave_node.ip = info.followers(i).ip();
    slave_node.port = info.followers(i).port();
    nodes->push_back(slave_node);
  }
  return Status::OK();
}

Status Cluster::ListNode(std::vector<Node>* nodes,
    std::vector<std::string>* status) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::LISTNODE);

  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  }
  ZPMeta::Nodes info = meta_res_.list_node().nodes();
  for (int i = 0; i < info.nodes_size(); i++) {
    Node data_node;
    data_node.ip = info.nodes(i).node().ip();
    data_node.port = info.nodes(i).node().port();
    nodes->push_back(data_node);
    if (info.nodes(i).status() == 1) {
      status->push_back("down");
    } else {
      status->push_back("up");
    }
  }
  return Status::OK();
}

Status Cluster::ListTable(std::vector<std::string>* tables) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::LISTTABLE);

  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  }
  ZPMeta::TableName info = meta_res_.list_table().tables();
  for (int i = 0; i < info.name_size(); i++) {
    tables->push_back(info.name(i));
  }
  return Status::OK();
}

Status Cluster::SubmitDataCmd(const std::string& table, const std::string& key,
    int attempt, bool has_pull) {
  Status s;
  Node master;

  s = GetDataMaster(table, key, &master);
  if (!s.ok()) {
    if (has_pull) {
      return Status::IOError("can't find data node after pull");
    }
    s = Pull(table);
    has_pull = true;
    if (s.ok()) {
      return SubmitDataCmd(table, key, attempt, has_pull);
    } else {
      return Status::IOError("can't find data node, can't pull either");
    }
  }

  ZpConn* data_cli = data_pool_->GetConnection(master);

  if (data_cli) {
    s = data_cli->Send(&data_cmd_);
    if (s.ok()) {
      s = data_cli->Recv(&data_res_);
    }
    if (s.ok()) {
      return s;
    } else if (s.IsIOError()) {
      data_pool_->RemoveConnection(data_cli);
      if (attempt <= kDataAttempt) {
        return SubmitDataCmd(table, key, attempt+1, has_pull);
      }
      return Status::IOError("data connectin can't work,after attempts");
    }

  } else if (has_pull) {
    return Status::IOError("can't connect to data,after pull");
  } else if (!has_pull) {
    s = Pull(table);
    if (s.ok()) {
      return SubmitDataCmd(table, key, attempt+1, true);
    } else {
      return Status::IOError("can't find data node, can't pull either");
    }
  }
  return Status::Corruption("should never reach here");
}

Status Cluster::SubmitMetaCmd(int attempt) {
  Status s = Status::IOError("got no meta cli");
  Node meta;
  meta = GetRandomMetaAddr();
  ZpConn* meta_cli = meta_pool_->GetConnection(meta);
  if (meta_cli) {
    s = meta_cli->Send(&meta_cmd_);
    if (s.ok()) {
      s = meta_cli->Recv(&meta_res_);
    }
    if (s.ok()) {
      return s;
    } else if (s.IsIOError()) {
      meta_pool_->RemoveConnection(meta_cli);
      if (attempt <= kMetaAttempt) {
        return SubmitMetaCmd(attempt+1);
      }
      return Status::IOError("meta connection can't work,after attempts");
    }
  } else {
    return Status::IOError("can't get meta cli");
  }
  return Status::Corruption("should never reach here");
}


Status Cluster::DebugDumpTable(const std::string& table) {
  std::cout << "epoch:" << epoch_ << std::endl;
  bool found = false;
  auto it = tables_->begin();
  while (it != tables_->end()) {
    if (it->first == table) {
      found = true;
      it->second->DebugDump();
    }
    it++;
  }
  if (found) {
    return Status::OK();
  } else {
    return Status::NotFound("don't have this table's info");
  }
}


const Table::Partition* Cluster::GetPartition(const std::string& table,
    const std::string& key) {
  auto it = tables_->begin();
  while (it != tables_->end()) {
    if (it->first == table) {
      return it->second->GetPartition(key);
    }
    it++;
  }
  return NULL;
}


Node Cluster::GetRandomMetaAddr() {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(0, meta_addr_.size()-1);
  int index = di(mt);
  return meta_addr_[index];
}

Status Cluster::GetDataMaster(const std::string& table,
    const std::string& key, Node* master) {
  std::unordered_map<std::string, Table*>::iterator it =
    tables_->find(table);
  Status s;
  if (it == tables_->end()) {
    s = Pull(table);
    if (s.ok()) {
      it = tables_->find(table);
    } else {
      return Status::NotFound("table does not exist");
    }
  }

  if (it != tables_->end()) {
    *master = it->second->GetKeyMaster(key);
    return Status::OK();
  } else {
    return Status::NotFound("table does not exist");
  }
}

Status Cluster::ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull) {
  epoch_ = pull.version();
  for (int i = 0; i < pull.info_size(); i++) {
    std::cout << "reset table:" << pull.info(i).name() << std::endl;
    auto it = tables_->find(pull.info(i).name());
    if (it != tables_->end()) {
      delete it->second;
      tables_->erase(it);
    }
    Table* new_table = new Table(pull.info(i));
    std::string table_name = pull.info(i).name();
    tables_->insert(std::make_pair(pull.info(i).name(), new_table));
  }
  return Status::OK();
}

Client::Client(const std::string& ip, const int port, const std::string& table)
: cluster_(new Cluster(ip, port)),
  table_(table) {
}

Client::~Client() {
  delete cluster_;
}

Status Client::Connect() {
  Status s = cluster_->Connect();
  if (!s.ok()) {
    return s;
  }
  s = cluster_->Pull(table_);
  return s;
}

Status Client::Set(const std::string& key, const std::string& value) {
  return cluster_->Set(table_, key, value);
}

Status Client::Get(const std::string& key, std::string* value) {
  return cluster_->Get(table_, key, value);
}

Status Client::Delete(const std::string& key) {
  return cluster_->Delete(table_, key);
}

}  // namespace libzp
