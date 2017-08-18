#include "src/meta/zp_meta_info_store.h"

#include "slash/include/slash_string.h"
#include "include/zp_const.h"

static bool IsSameNode(const ZPMeta::Node& node, const std::string& ip_port) {
  return slash::IpPortString(node.ip(), node.port()) == ip_port;
}

static bool AssignPbNode(ZPMeta::Node& node, const std::string& ip_port) {
  std::string ip;
  int port = 0;
  if (!slash::ParseIpPortString(ip_port, ip, port)) {
    return false;
  }
  node.set_ip(ip);
  node.set_port(port);
  return true;
}

ZPMetaInfoStoreSnap::ZPMetaInfoStoreSnap()
  : epoch_(-1),
  node_changed_(false),
  table_changed_(false),
  epoch_changed_(false) {
  }

void ZPMetaInfoStoreSnap::UpNode(const std::string& ip_port) {
  node_changed_ = (nodes_.find(ip_port) == nodes_.end()
      || nodes_[ip_port] != ZPMeta::NodeState::UP);
  nodes_[ip_port] = ZPMeta::NodeState::UP;
}

void ZPMetaInfoStoreSnap::DownNode(const std::string& ip_port) {
  if (nodes_.find(ip_port) != nodes_.end()) {
    node_changed_ = (nodes_[ip_port] != ZPMeta::NodeState::DOWN);
    nodes_[ip_port] = ZPMeta::NodeState::DOWN;
  }
}

Status ZPMetaInfoStoreSnap::AddSlave(const std::string& table, int partition,
    const std::string& ip_port) {
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("Table not exist");
  }
  ZPMeta::Table* tptr = &(tables_[table]);
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);
  if (!pptr) {
    return Status::NotFound("Partition not exist");
  }

  if (IsSameNode(pptr->master(), ip_port)) {
    return Status::OK();  // Already be master
  }
  for (const auto& s : pptr->slaves()) {
    if (IsSameNode(s, ip_port)) {
      return Status::OK();  // Already be slave 
    }
  }

  ZPMeta::Node* new_slave = pptr->add_slaves();
  AssignPbNode(*new_slave, ip_port);
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::DeleteSlave(const std::string& table, int partition,
    const std::string& ip_port) {
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("Table not exist");
  }
  ZPMeta::Table* tptr = &(tables_[table]);
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);
  if (!pptr) {
    return Status::NotFound("Partition not exist");
  }

  if (IsSameNode(pptr->master(), ip_port)) {
    return Status::InvalidArgument("Not slave");  // Already be master
  }

  ZPMeta::Partitions new_p;
  for (const auto& s : pptr->slaves()) {
    if (!IsSameNode(s, ip_port)) {
      ZPMeta::Node* new_slave = new_p.add_slaves();
      AssignPbNode(*new_slave, ip_port);
    }
  }
  new_p.set_id(pptr->id());
  new_p.mutable_state().CopyFrom(pptr->state());
  new_p.mutable_master().CopyFrom(pptr->master());
  
  pptr->CopyFrom(new_p);
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::SetMaster(const std::string& table, int partition,
    const std::string& ip_port) {
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("Table not exist");
  }
  ZPMeta::Table* tptr = &(tables_[table]);
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);
  if (!pptr) {
    return Status::NotFound("Partition not exist");
  }

  if (IsSameNode(pptr->master(), ip_port)) {
    return Status::OK();  // Already be master
  }

  ZPMeta::Node tmp;
  for (const auto& s : pptr->slaves()) {
    if (IsSameNode(s, ip_port)) {
      // TODO
      break;
    }
  }
  
  if (!tmp.IsInitialized()) {
    return Status::NotFound("Node is not slave");
  }
  

}

ZPMetaInfoStore::ZPMetaInfoStore(floyd::Floyd* floyd)
  : floyd_(floyd),
  epoch_(-1) {
  
  }

Status ZPMetaInfoStore::Refresh() {
  std::string value;
  
  // Get Version
  int tmp_epoch = -1;
  Stauts fs = floyd_->Read(kMetaVersion, value);
  if (fs.ok()) {
    tmp_epoch = std::stoi(value);
  } else if (fs.IsNotFound()) {
    LOG(INFO) << "Epoch not found in floyd";
    epoch_ = -1;
    return Status::OK();
  } else {
    LOG(ERROR) << "Load epoch failed: " << fs.ToString();
    return fs
  }

  if (tmp_epoch == epoch_) {
    LOG(INFO) << "Epoch not changed in floyd, no need change, epoch: "
      << epoch_;
    return Status::OK();
  }
  LOG(INFO) << "Load epoch from floyd succ, tmp version : " << tmp_epoch;

  // Read table names
  ZPMeta::TableName table_names;
  fs = floyd_->Read(kMetaTables, value);
  if (!fs.ok()) {
    LOG(ERROR) << "Load meta table names failed: " << fs.ToString();
    return fs
  }
  if (!table_names.ParseFromString(value)) {
    LOG(ERROR) << "Deserialization meta table names failed, value: " << value;
    return Status::Corruption("Parse failed");
  }
  LOG(INFO) << "Load table names from floyd succ, table names size: "
    << table_names.name_size();

  // Read tables and update node_table_, table_info_
  node_table_.clear();
  table_info_.clear();
  std::string ip_port;
  ZPMeta::Table table_info;
  for (const auto& t : table_names.name()) {
    fs = floyd_->Read(t, value);
    if (!fs.ok()) {
      LOG(ERROR) << "Load floyd table_info failed: " << fs.ToString()
        << ", table name: " << t;
      return fs;
    }
    if (!table_info.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table_info failed, table: "
        << t << " value: " << value;
      return Status::Corruption("Parse failed");
    }

    table_info_[t] = ZPMeta::Table();
    table_info_[t].CopyFrom(table_info);

    for (const auto& partition : table_info.partitions()) {
      if (partition.master().ip() != ""
          && partition.master().port() != -1) {
        ip_port = slash::IpPortString(partition.master().ip(),
            partition.master().port());
        AddNodeTable(ip_port, t);
      }

      for (int k = 0; k < partition.slaves_size(); k++) {
        ip_port = slash::IpPortString(partition.slaves(k).ip(),
            partition.slaves(k).port());
        AddNodeTable(ip_port, t);
      }
    }
  }
  LOG(INFO) << "Update node_table_ from floyd succ.";
  NodesDebug();

  // Update Version
  epoch_ = tmp_epoch;
  LOG(INFO) << "Update epoch: " << epoch_;
  return Status::OK();
}

Status ZPMetaInfoStore::RestoreNodeAlive() {
  // Read all nodes
  ZPMeta::Nodes allnodes;
  fs = floyd_->Read(kMetaNodes, value);
  if (!fs.ok()) {
    LOG(ERROR) << "Load meta nodes failed: " << fs.ToString();
    return fs
  }
  if (!allnodes.ParseFromString(value)) {
    LOG(ERROR) << "Deserialization nodes failed, value: " << value;
    return slash::Status::Corruption("Parse failed");
  }
  
  std::string ip_port;
  node_alive_.clear();
  for (const auto& node_s : allnodes) {
    ip_port = slash::IpPortString(node_s.node().ip(), node_s.node().port());
    if (node_s.status() == ZPMeta::NodeState::UP) {
      // Alive node
      node_alive_[ip_port] = slash::NowMicros();
    } else {
      node_alive_[ip_port] = 0;
    
    }
  }
  return Status::OK();
}

// Return true when the node is new alive
bool ZPMetaInfoStore::UpdateNodeAlive(const std::string& node) {
  bool is_new = false;
  if (node_alive_.find(node) == node_alive_.end()
      || node_alive_[node] == 0) {
    is_new = true;
  }
  node_alive_[node] = slash::NowMicros();
  return is_new;
}

void ZPMetaInfoStore::NodesDebug() {
  LOG(INFO) << "--------------Dump nodes-----------------.";
  for (auto iter = node_table_.begin(); iter != node_table_.end(); iter++) {
    std::string str = iter->first + " :";
    for (auto it = iter->second.begin(); it != iter->second.end(); it++) {
      str += (" " + *it);
    }
    LOG(INFO) << str;
  }
  LOG(INFO) << "------------------------------------------.";
}

void ZPMetaInfoStore::AddNodeTable(const std::string& ip_port,
       const std::string& table) {
  const auto iter = node_table_.find(ip_port);
  if (iter == node_table_.end()) {
    node_table_.insert(std::pair<std::string,
        std::set<std::string>(ip_port, std::set<std::string>()));
  }
  node_table_[ip_port].insert(table);
}

Status ZPMetaInfoStore::GetTablesForNode(const std::string &ip_port,
    std::set<std::string> *tables) const {
  const auto iter = node_table_.find(ip_port);
  if (iter == node_table_.end()) {
    return Status::NotFound("node not exist");
  }
  table_list->clear();
  *table_list = iter->second();
  return Status::OK();
}

Status ZPMetaInfoStore::GetTableMeta(const std::string& table,
    ZPMeta::Table* table_meta) const {
  const auto iter = table_info_.find(table);
  if (iter == table_info_.end()) {
    return Status::NotFound("Table meta not found");
  }
  table_meta->CopyFrom(iter->second);
  return Status::OK();
}

void ZPMetaInfoStore::FetchExpiredNode(std::set<std::string>* nodes) {
  nodes->clear();
  auto it = node_alive_.begin();
  while (it != node_alive_.end() ) {
    if (na.second > 0
        && slash::NowMicros() - na.second > kNodeMetaTimeoutM * 1000) {
      nodes->insert(na.first);
      it = node_alive_.erase(it);
    } else {
      ++it;
    }
  }
}

void ZPMetaInfoStore::GetSnapshot(ZPMetaInfoStoreSnap* snap) {
  snap->epoch_ = epoch_;
  GetAllTables(&(snap->tables_))
  GetAllNodes(&(snap->nodes_))
}

void ZPMetaInfoStore::GetAllTables(
    std::unordered_map<std::string, ZPMeta::Table>* all_tables) const {
  all_tables->clear();
  for (const auto& t : table_info_) {
    all_tables[t.first] = ZPMeta::Table();
    all_tables[t.first].CopyFrom(t.second);
  }
}

void ZPMetaInfoStore::GetAllNodes(
    std::unordered_map<std::string, ZPMeta::NodeStatus>* all_nodes) const {
  all_nodes->clear();
  for (const auto& t : node_alive_) {
    if (t.second > 0) {
      all_nodes[t.first] = ZPMeta::NodeStatus::UP;
    } else {
      all_nodes[t.first] = ZPMeta::NodeStatus::DOWN;
    }
  }
}

Status ZPMetaServer::AddTable(const std::string &table_name, int num) {

  if (info_store_.TableExist(table_name)) {
    LOG(WARNING) << "Create table failed, since table already exist, Table: "
      << table_name;
    return Status::Corruption("Already Exist");
  }

  ZPMeta::Nodes nodes;
  s = GetAllNodes(&nodes);
  if (!s.ok()) {
    return s;
  }

  std::vector<ZPMeta::NodeStatus> t_alive_nodes;
  GetAllAliveNode(nodes, &t_alive_nodes);
  if (t_alive_nodes.size() < 3) {
    return Status::Corruption("have no enough alive nodes to create replicats");
  }

  std::vector<ZPMeta::NodeStatus> alive_nodes;
  Reorganize(t_alive_nodes, &alive_nodes);

  int an_num = alive_nodes.size();

  ZPMeta::Table table;

  table.set_name(name);

  std::srand(std::time(0));
  int rand_pos = (std::rand() % an_num);
  LOG(INFO) << "Distribute start at " << rand_pos;

  for (int i = 0; i < num; i++) {
    ZPMeta::Partitions *p = table.add_partitions();
    p->set_id(i);
    p->set_state(ZPMeta::PState::ACTIVE);
    p->mutable_master()->CopyFrom(alive_nodes[(i + rand_pos) % an_num].node());

    ZPMeta::Node *slave = p->add_slaves();
    slave->CopyFrom(alive_nodes[(i + rand_pos + 1) % an_num].node());

    slave = p->add_slaves();
    slave->CopyFrom(alive_nodes[(i + rand_pos + 2) % an_num].node());
  }

  s = SetTable(table);
  if (!s.ok()) {
    LOG(ERROR) << "SetTable error in Distribute, error: " << s.ToString();
    return s;
  }

  s = UpdateTableList(name);
  if (!s.ok()) {
    LOG(ERROR) << "UpdateTableList error: " << s.ToString();
    return s;
  }

  s = Set(kMetaVersion, std::to_string(version_+1));
  if (s.ok()) {
    version_++; 
    LOG(INFO) << "Set version in Distribute : " << version_;
  } else {
    LOG(ERROR) << "Set version error in Distribute, error: " << s.ToString();
    return s;
  }

  std::string ip_port;
  int pnum = 3+num-1;
  for (int i = 0; pnum && i < an_num; i++) {
    ip_port = slash::IpPortString(alive_nodes[(i + rand_pos) % an_num].node().ip(),
        alive_nodes[(i + rand_pos) % an_num].node().port());
    auto it = nodes_.find(ip_port);
    if (it != nodes_.end()) {
      it->second.insert(name);
    } else {
      std::set<std::string> ts;
      ts.insert(name);
      nodes_.insert(std::unordered_map<std::string, std::set<std::string> >::value_type(ip_port, ts));
    }
    pnum--;
  }

  DebugNodes();
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(table, &text_format);
  LOG(INFO) << "table_info : [" << text_format << "]";

  return Status::OK();
}

void ZPMetaServer::Reorganize(const std::vector<ZPMeta::NodeStatus> &t_alive_nodes, std::vector<ZPMeta::NodeStatus> *alive_nodes) {
  std::map<std::string, std::vector<ZPMeta::NodeStatus> >m;

  for (auto iter_v = t_alive_nodes.begin(); iter_v != t_alive_nodes.end(); iter_v++) {
    auto iter_m = m.find(iter_v->node().ip());
    if (iter_m != m.end()) {
      iter_m->second.push_back(*iter_v);
    } else {
      std::vector<ZPMeta::NodeStatus> n;
      n.push_back(*iter_v);
      m.insert(std::map<std::string, std::vector<ZPMeta::NodeStatus> >::value_type(iter_v->node().ip(), n));
    }
  }

  int msize = m.size();
  int empty_count = 0;
  bool done = false;
  while (!done) {
    for (auto iter_m = m.begin(); iter_m != m.end(); iter_m++) {
      if (iter_m->second.empty()) {
        empty_count++;
        if (empty_count == msize) {
          done = true;
          break;
        }
        continue;
      } else {
        LOG(INFO) << "PUSH " << iter_m->second.back().node().ip() << ":" << iter_m->second.back().node().port();
        alive_nodes->push_back(iter_m->second.back());
        iter_m->second.pop_back();
      }
    }
  }
}

Status ZPMetaServer::DropTable(const std::string &name) {
  slash::MutexLock l(&node_mutex_);
  std::string value;
  Status s;

  s = DeleteTable(name);
  if (!s.ok()) {
    LOG(ERROR) << "DeleteTable error in DropTable, error: " << s.ToString();
    return s;
  }

  s = RemoveTableFromTableList(name);
  if (!s.ok()) {
    LOG(ERROR) << "RemoveTableFromTableList error: " << s.ToString();
    return s;
  }

  s = Set(kMetaVersion, std::to_string(version_+1));
  if (s.ok()) {
    version_++; 
    LOG(INFO) << "Set version in DropTable : " << version_;
  } else {
    LOG(ERROR) << "Set version error in DropTable, error: " << s.ToString();
    return s;
  }

  for (auto iter = nodes_.begin(); iter != nodes_.end(); iter++) {
    iter->second.erase(name);
  }

  DebugNodes();

  return Status::OK();
}

Status ZPMetaServer::SetMaster(const std::string &table, int partition, const ZPMeta::Node &node) {
  bool valid = false;
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  LOG(INFO) << "SetMaster " << table << " " << partition << " " << ip_port;

  {
  slash::MutexLock l(&node_mutex_);
  auto iter = nodes_.find(ip_port);
  if (iter == nodes_.end()) {
    return Status::Corruption("this node doesn't hold any table");
  }
  auto it = iter->second.find(table);
  if (it == iter->second.end()) {
    return Status::Corruption("table & node Dismatch");
  }
  }

  ZPMeta::Table table_info;
  Status s = GetTableInfo(table, &table_info);
  if (!s.ok()) {
    return s;
  }
  
  if (partition < 0 || partition >= table_info.partitions_size()) {
    return Status::Corruption("invalid partition");
  }

  ZPMeta::Partitions p = table_info.partitions(partition);

  if (p.master().ip() == node.ip() && p.master().port() == node.port()) {
    LOG(INFO) << "SetMaster: Already master";
    return Status::Corruption("already master");
  }

  for (int i = 0; i < p.slaves_size(); i++) {
    if (p.slaves(i).ip() == node.ip() && p.slaves(i).port() == node.port()) {
      valid = true;
    }
  }

  if (valid) {
    LOG(INFO) << "SetMaster PushTask " << static_cast<int>(task.op) << " " << ip_port << " " << table << " " << partition;
    update_thread_->PendingUpdate(UpdateTask(ZPMetaUpdateOP::kOpSetMaster, ip_port, table, partition));
    return Status::OK();
  } else {
    return Status::Corruption("partition & node Dismatch");
  }

}

Status ZPMetaServer::AddSlave(const std::string &table, int partition, const ZPMeta::Node &node) {
  bool valid = false;
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  LOG(INFO) << "AddSlave " << table << " " << partition << " " << ip_port;

  ZPMeta::Nodes nodes;
  Status s = GetAllNodes(&nodes);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetAllNodes error in AddSlave, error: " << s.ToString();
    return s;
  }

  if (!FindNode(nodes, node.ip(), node.port())) {
    return Status::NotFound("not found");
  }

  ZPMeta::Table table_info;
  s = GetTableInfo(table, &table_info);
  if (!s.ok()) {
    return s;
  }
  
  if (partition < 0 || partition >= table_info.partitions_size()) {
    return Status::Corruption("invalid partition");
  }

  ZPMeta::Partitions p = table_info.partitions(partition);

  if (p.master().ip() == node.ip() && p.master().port() == node.port()) {
    LOG(INFO) << "AddSlave: Already master";
    return Status::OK();
  }

  int i = 0;
  for (i = 0; i < p.slaves_size(); i++) {
    if (p.slaves(i).ip() == node.ip() && p.slaves(i).port() == node.port()) {
      break;
    }
  }
  if (i == p.slaves_size()) {
    valid = true;
  }

  if (valid) {
    LOG(INFO) << "AddSlave PushTask" << static_cast<int>(task.op) << " " << ip_port << " " << table << " " << partition;
    update_thread_->PendingUpdate(UpdateTask(ZPMetaUpdateOP::kOpAddSlave, ip_port, table, partition));
    return Status::OK();
  } else {
    return Status::Corruption("AddSlave: Already slave");
  }

}

Status ZPMetaServer::RemoveSlave(const std::string &table, int partition, const ZPMeta::Node &node) {
  bool valid = false;
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  LOG(INFO) << "RemoveSlave " << table << " " << partition << " " << ip_port;

  ZPMeta::Nodes nodes;
  Status s = GetAllNodes(&nodes);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetAllNodes error in RemoveSlave, error: " << s.ToString();
    return s;
  }

  if (!FindNode(nodes, node.ip(), node.port())) {
    return Status::NotFound("not found");
  }

  ZPMeta::Table table_info;
  s = GetTableInfo(table, &table_info);
  if (!s.ok()) {
    return s;
  }
  
  if (partition < 0 || partition >= table_info.partitions_size()) {
    return Status::Corruption("invalid partition");
  }

  ZPMeta::Partitions p = table_info.partitions(partition);

  if (p.master().ip() == node.ip() && p.master().port() == node.port()) {
    LOG(INFO) << "RemoveSlave: can not remove master";
    return Status::Corruption("can not remove master");
  }

  int i = 0;
  for (i = 0; i < p.slaves_size(); i++) {
    if (p.slaves(i).ip() == node.ip() && p.slaves(i).port() == node.port()) {
      valid = true;
      break;
    }
  }

  if (valid) {
    LOG(INFO) << "RemoveSlave PushTask" << static_cast<int>(task.op) << " " << ip_port << " " << table << " " << partition;
    update_thread_->PendingUpdate(UpdateTask(ZPMetaUpdateOP::kOpRemoveSlave, ip_port, table, partition));
    return Status::OK();
  } else {
    return Status::Corruption("RemoveSlave: node & partition dismatch");
  }

}





Status ZPMetaServer::AddVersion() {
  Status s = Set(kMetaVersion, std::to_string(version_+1));
  if (s.ok()) {
    version_++;
    LOG(INFO) << "AddVersion success: " << version_;
  } else {
    LOG(INFO) << "AddVersion failed: " << s.ToString();
    return Status::Corruption("AddVersion Error");
  }
  return Status::OK();
}

Status ZPMetaServer::Set(const std::string &key, const std::string &value) {
  Status fs = floyd_->Write(key, value);
  if (fs.ok()) {
    return Status::OK();
  } else {
    LOG(ERROR) << "Floyd write failed: " << fs.ToString();
    return Status::Corruption("floyd set error!");
  }
}



