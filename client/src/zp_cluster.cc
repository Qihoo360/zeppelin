/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */
#include "include/zp_cluster.h"

#include <google/protobuf/text_format.h>
#include<iostream>
#include<string>

#include "slash_string.h"
#include "slash_mutex.h"


namespace libzp {

struct CmdRpcArg {
  Cluster* cluster;
  std::string table;
  std::string key;
  client::CmdRequest* request;
  client::CmdResponse* response;
  Status result;

  CmdRpcArg(Cluster* c, const std::string& t, const std::string& k)
    : cluster(c), table(t), key(k),
    result(Status::Incomplete("Not complete")),
    cond_(&mu_), done_(false) {
      request = new client::CmdRequest();
      response = new client::CmdResponse();
    }
  
  ~CmdRpcArg() {
    delete request;
    delete response;
  }

  void WaitRpcDone() {
    slash::MutexLock l(&mu_);
    while (!done_) {
      cond_.Wait();  
    }
  }
  void RpcDone() {
    slash::MutexLock l(&mu_);
    done_ = true;
    cond_.Signal();
  }

private:
  slash::Mutex mu_;
  slash::CondVar cond_;
  bool done_;
};

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
  meta_pool_ = new ConnectionPool();
  data_pool_ = new ConnectionPool();
}

Cluster::~Cluster() {
  for (auto& bg : cmd_workers_) {
    delete bg.second;
  }

  std::unordered_map<std::string, Table*>::iterator iter = tables_.begin();
  while (iter != tables_.end()) {
    delete iter->second;
    iter++;
  }

  delete meta_pool_;
  delete data_pool_;
}


Status Cluster::Set(const std::string& table, const std::string& key,
    const std::string& value, int32_t ttl) {
  Status s;

  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::SET);
  client::CmdRequest_Set* set_info = data_cmd_.mutable_set();
  set_info->set_table_name(table);
  set_info->set_key(key);
  set_info->set_value(value);
  if (ttl >= 0) {
    set_info->mutable_expire()->set_ttl(ttl);
  }

  s = SubmitDataCmd(table, key, data_cmd_, &data_res_);
  if (s.IsIOError() || s.IsCorruption()) {
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
  s = SubmitDataCmd(table, key, data_cmd_, &data_res_);
  if (s.IsIOError() || s.IsCorruption()) {
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

  s = SubmitDataCmd(table, key, data_cmd_, &data_res_);

  if (s.IsIOError() || s.IsCorruption()) {
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

Status Cluster::Mget(const std::string& table,
    const std::vector<std::string>& keys,
    std::map<std::string, std::string>* values) {
  if (values == NULL) {
    return Status::InvalidArgument("Null pointer");
  }

  Status s;
  Node master;
  // Build multi command to nodes: master node -> Command
  std::map<Node, CmdRpcArg*> key_distribute;
  for (auto& k : keys) {
    s = GetDataMaster(table, k, &master);
    if (!s.ok()) {
      return s;
    }

    if (key_distribute.find(master) == key_distribute.end()) {
      CmdRpcArg* arg = new CmdRpcArg(this, table, k);
      arg->request->set_type(client::Type::MGET);
      client::CmdRequest_Mget* new_mget_cmd = arg->request->mutable_mget();
      new_mget_cmd->set_table_name(table);
      key_distribute.insert(std::pair<Node, CmdRpcArg*>(
            master, arg));
    }
    key_distribute[master]->request->mutable_mget()->add_keys(k);
  }

  // Distribute command
  DistributeDataRpc(key_distribute);

  bool has_error = false;
  for (auto& kd : key_distribute) {
    client::CmdResponse *res = kd.second->response;
    if (!kd.second->result.ok()
        || res->code() != client::StatusCode::kOk ) {
      has_error = true;
    }
    for (auto& kv : res->mget()) {
      values->insert(std::pair<std::string, std::string>(kv.key(),
            kv.value()));
    }
    delete kd.second;
  }
  if (has_error) {
    return Status::Corruption("mget error happened");
  }
  return Status::OK();
}

void Cluster::DoSubmitDataCmd(void* arg) {
  CmdRpcArg *carg = static_cast<CmdRpcArg*>(arg);
  carg->result = carg->cluster->SubmitDataCmd(carg->table, carg->key,
      *(carg->request), carg->response);
  carg->RpcDone();
}

void Cluster::DistributeDataRpc(
    const std::map<Node, CmdRpcArg*>& key_distribute) {
  for (auto& kd : key_distribute) {
    if (cmd_workers_.find(kd.first) == cmd_workers_.end()) {
      pink::BGThread* bg = new pink::BGThread();
      bg->StartIfNeed();
      cmd_workers_.insert(std::pair<Node, pink::BGThread*>(kd.first, bg));
    }
    cmd_workers_[kd.first]->Schedule(DoSubmitDataCmd, kd.second);
  }

  for (auto& kd : key_distribute) {
    kd.second->WaitRpcDone();
  }
}
  
Status Cluster::CreateTable(const std::string& table_name,
    const int partition_num) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::INIT);
  ZPMeta::MetaCmd_Init* init = meta_cmd_.mutable_init();
  init->set_name(table_name);
  init->set_num(partition_num);

  Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::DropTable(const std::string& table_name) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::DROPTABLE);
  ZPMeta::MetaCmd_DropTable* drop_table = meta_cmd_.mutable_drop_table();
  drop_table->set_name(table_name);

  Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
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
  ZpCli* meta_cli = meta_pool_->GetExistConnection();
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
  Status ret = SubmitMetaCmd();

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

  Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
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

  Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
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

  Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
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

  Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
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

  Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
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

  Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
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

Status Cluster::InfoQps(const std::string& table, int* qps, int* total_query) {
  Status s;
  *qps = 0;
  *total_query = 0;

  Pull(table);
  auto table_iter = tables_.find(table);
  if (table_iter == tables_.end()) {
    return Status::NotFound("this table does not exist");
  }
  Table* table_map = table_iter->second;

  std::vector<Node> related_nodes;
  table_map->GetNodes(&related_nodes);

  auto node_iter = related_nodes.begin();
  while (node_iter != related_nodes.end()) {
    data_cmd_.Clear();
    data_cmd_.set_type(client::Type::INFOSTATS);
    s = TryDataRpc(*node_iter, data_cmd_, &data_res_);
    node_iter++;
    if (s.IsIOError() || s.IsCorruption()) {
      continue;
    }
    for (int i = 0; i < data_res_.info_stats_size(); i++) {
      std::string name = data_res_.info_stats(i).table_name();
      if (name == table) {
        *qps += data_res_.info_stats(i).qps();
        *total_query += data_res_.info_stats(i).total_querys();
        break;
      }
    }
  }
  return Status::OK();
}

Status Cluster::InfoOffset(const Node& node, const std::string& table,
    std::vector<std::pair<int, BinlogOffset>>* partitions) {
  Status s;

  Pull(table);
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::INFOPARTITION);
  s = TryDataRpc(node, data_cmd_, &data_res_);

  for (int i = 0; i < data_res_.info_partition_size(); i++) {
    std::string name = data_res_.info_partition(i).table_name();
    if (name == table) {
      std::pair<int, BinlogOffset> offset;
      int par_size = data_res_.info_partition(i).sync_offset_size();
      for (int j = 0; j < par_size; j++) {
        offset.first = data_res_.info_partition(i).sync_offset(j).partition();
        offset.second.file_num =
          data_res_.info_partition(i).sync_offset(j).filenum();
        offset.second.offset =
          data_res_.info_partition(i).sync_offset(j).offset();
        partitions->push_back(offset);
      }
      break;
    }
  }
  return Status::OK();
}

Status Cluster::InfoSpace(const std::string& table,
    std::vector<std::pair<Node, SpaceInfo>>* nodes) {
  Status s;

  Pull(table);
  auto table_iter = tables_.find(table);
  if (table_iter == tables_.end()) {
    return Status::NotFound("this table does not exist");
  }
  Table* table_map = table_iter->second;

  std::vector<Node> related_nodes;
  table_map->GetNodes(&related_nodes);

  auto node_iter = related_nodes.begin();
  while (node_iter != related_nodes.end()) {
    data_cmd_.Clear();
    data_cmd_.set_type(client::Type::INFOCAPACITY);
    s = TryDataRpc(*node_iter, data_cmd_, &data_res_);
    if (s.IsIOError() || s.IsCorruption()) {
      node_iter++;
      continue;
    }
    for (int i = 0; i < data_res_.info_capacity_size(); i++) {
      std::string name = data_res_.info_capacity(i).table_name();
      if (name == table) {
        std::pair<Node, SpaceInfo> info;
        info.first = *node_iter;
        info.second.used = data_res_.info_capacity(i).used();
        info.second.remain = data_res_.info_capacity(i).remain();
        nodes->push_back(info);
        break;
      }
    }
    node_iter++;
  }
  return Status::OK();
}

Status Cluster::SubmitDataCmd(const std::string& table, const std::string& key,
    client::CmdRequest& req, client::CmdResponse *res,
    bool has_pull) {
  Status s;
  Node master;

  s = TryGetDataMaster(table, key, &master);
  if (!s.ok()) {
    if (has_pull) {
      return Status::IOError("can't find data node after pull");
    }
    s = Pull(table);
    has_pull = true;
    if (s.ok()) {
      return SubmitDataCmd(table, key, req, res, has_pull);
    } else {
      return Status::IOError("can't find data node, can't pull either");
    }
  }
  s =  TryDataRpc(master, req, res);
  if (!s.ok()) {
    if (has_pull) {
      return Status::IOError("can't connect to data node after pull");
    }
    s = Pull(table);
    has_pull = true;
    if (s.ok()) {
      return SubmitDataCmd(table, key, req, res, has_pull);
    } else {
      return Status::IOError("can't connect to data node, can't pull either");
    }
  }
  return s;
}

Status Cluster::TryDataRpc(const Node& master,
    client::CmdRequest& req, client::CmdResponse *res,
    int attempt) {
  ZpCli* data_cli = data_pool_->GetConnection(master);
  pink::Status s;
  if (data_cli) {
    s = data_cli->cli->Send(&req);
    if (s.ok()) {
      s = data_cli->cli->Recv(res);
    }
    if (s.ok()) {
      return Status::OK();
    } else if (s.IsIOError() || s.IsCorruption()) {
      data_pool_->RemoveConnection(data_cli);
      if (attempt <= kDataAttempt) {
        return TryDataRpc(master, req, res, attempt+1);
      }
      return Status::IOError("data connectin can't work,after attempts");
    }
  }
  return Status::NotFound("can't get data conn");
}

Status Cluster::SubmitMetaCmd(int attempt) {
  pink::Status s = pink::Status::IOError("got no meta cli");
  Node meta;
  meta = GetRandomMetaAddr();
  ZpCli* meta_cli = meta_pool_->GetConnection(meta);
  if (meta_cli) {
    s = meta_cli->cli->Send(&meta_cmd_);
    if (s.ok()) {
      s = meta_cli->cli->Recv(&meta_res_);
    }
    if (s.ok()) {
      return Status::OK();
    } else if (s.IsIOError() || s.IsCorruption()) {
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
  auto it = tables_.begin();
  while (it != tables_.end()) {
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
  auto it = tables_.begin();
  while (it != tables_.end()) {
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

Status Cluster::TryGetDataMaster(const std::string& table,
    const std::string& key, Node* master) {
  std::unordered_map<std::string, Table*>::iterator it =
    tables_.find(table);
  Status s;
  if (it == tables_.end()) {
    s = Pull(table);
    if (s.ok()) {
      it = tables_.find(table);
    } else {
      return Status::NotFound("table does not exist");
    }
  }

  if (it != tables_.end()) {
    *master = it->second->GetKeyMaster(key);
    return Status::OK();
  } else {
    return Status::NotFound("table does not exist");
  }
}

Status Cluster::GetDataMaster(const std::string& table,
    const std::string& key, Node* master, bool has_pull) {
  Status s = TryGetDataMaster(table, key, master);
  if (s.ok()
      || has_pull) {
    return s;
  }
  
  s = Pull(table);
  if (!s.ok()) {
    return s;
  }
  return GetDataMaster(table, key, master, true);
}

Status Cluster::ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull) {
  epoch_ = pull.version();
  for (auto& table : tables_) {
    delete table.second;
  }
  tables_.clear();
  for (int i = 0; i < pull.info_size(); i++) {
    std::cout << "reset table:" << pull.info(i).name() << std::endl;
    Table* new_table = new Table(pull.info(i));
    std::string table_name = pull.info(i).name();
    tables_.insert(std::make_pair(pull.info(i).name(), new_table));
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

Status Client::Set(const std::string& key,
    const std::string& value, int32_t ttl) {
  return cluster_->Set(table_, key, value, ttl);
}

Status Client::Get(const std::string& key, std::string* value) {
  return cluster_->Get(table_, key, value);
}

Status Client::Delete(const std::string& key) {
  return cluster_->Delete(table_, key);
}

Status Client::Mget(const std::vector<std::string>& keys,
    std::map<std::string, std::string>* values) {
  return cluster_->Mget(table_, keys, values);
}

}  // namespace libzp
