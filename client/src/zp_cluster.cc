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
  data_cmd_ = client::CmdRequest();
  data_res_ = client::CmdResponse();
  meta_cmd_ = ZPMeta::MetaCmd();
  meta_res_ = ZPMeta::MetaCmdResponse();

  meta_addr_ = options.meta_addr;
  if (meta_addr_.size() == 0) {
    fprintf(stderr, "you need input at least 1 meta node address:\n");
  }
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
  IpPort meta;
  pink::PbCli* meta_cli = meta_pool_->GetExistConnection(&meta);
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
    const int partition_num, const IpPort& ip_port) {
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
    const int partition_num, const IpPort& ip_port) {
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
    const int partition_num, const IpPort& ip_port) {
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

Status Cluster::ListMeta(std::vector<IpPort>& nodes) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::LISTMETA);

  pink::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  }
  ZPMeta::Nodes info = meta_res_.list_node().nodes();
  for (int i = 0; i < info.nodes_size(); i++) {
    IpPort meta_node;
    meta_node.ip = info.nodes(i).node().ip();
    meta_node.port = info.nodes(i).node().port();
    nodes.push_back(meta_node);
  }
  return Status::OK();
}

Status Cluster::ListNode(std::vector<IpPort>& nodes) {
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
    IpPort data_node;
    data_node.ip = info.nodes(i).node().ip();
    data_node.port = info.nodes(i).node().port();
    nodes.push_back(data_node);
  }
  return Status::OK();
}

Status Cluster::ListTable(std::vector<std::string>& tables) {
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
    tables.push_back(info.name(i));
  }
  return Status::OK();
}

Status Cluster::SubmitDataCmd(const std::string& table, const std::string& key,
    int attempt, bool has_pull) {
  Status s;
  IpPort master;

//  std::cout << "submit" << std::endl;
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

//  std::cout << "data ip:" << master.ip << " port:" << master.port << std::endl;
  pink::PbCli* data_cli = data_pool_->GetConnection(master);

  if (data_cli) {
    s = data_cli->Send(&data_cmd_);
    if (s.ok()) {
      s = data_cli->Recv(&data_res_);
    }
    if (s.ok()) {
      return s;
    } else if (s.IsIOError()) {
      data_pool_->RemoveConnection(master);
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
  IpPort meta;
  meta = GetRandomMetaAddr();
  pink::PbCli* meta_cli = meta_pool_->GetConnection(meta);
  if (meta_cli) {
    s = meta_cli->Send(&meta_cmd_);
    if (s.ok()) {
      s = meta_cli->Recv(&meta_res_);
    }
    if (s.ok()) {
      return s;
    } else if (s.IsIOError()) {
      meta_pool_->RemoveConnection(meta);
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


Table::Partition* Cluster::GetPartition(const std::string& table,
    const std::string& key) {
  std::cout << "epoch:" << epoch_ << std::endl;
  auto it = tables_->begin();
  while (it != tables_->end()) {
    if (it->first == table) {
      return it->second->GetPartition(key);
    }
    it++;
  }
  return NULL;
}


IpPort Cluster::GetRandomMetaAddr() {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(0, meta_addr_.size()-1);
  int index = di(mt);
  return meta_addr_[index];
}

Status Cluster::GetDataMaster(const std::string& table,
    const std::string& key, IpPort* master) {
  std::unordered_map<std::string, Table*>::iterator it =
    tables_->find(table);
  Status s;
  if (it == tables_->end()) {
    std::cout << "know nothing about this table, repull" << std::endl;
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
  std::cout << "get " << pull.info_size() << " table info" << std::endl;
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
  std::cout<< "pull done" <<  std::endl;
  return Status::OK();
}

}  // namespace libzp
