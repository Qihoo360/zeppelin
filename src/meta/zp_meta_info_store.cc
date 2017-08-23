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
  : snap_epoch_(-1),
  node_changed_(false) {
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
  table_changed_[table] = true;
  return Status::OK();
}

// Delete node from partition no mater it's master or slave
Status ZPMetaInfoStoreSnap::DeleteDup(const std::string& table, int partition,
    const std::string& ip_port) {
  Status s = Status::OK();
  if (DeleteSlave(table, partition, ip_port).IsInvalidArgument()) {
    // Table and parition is exist but current node is master
    const ZPMeta::Node one_slave = tables_[table].paritions(parition).slaves(0);
    s = SetMaster(table, partition,
        slash::IpPortString(one_slave.ip(), one_slave.port()));
    if (!s.ok()) {
      return s;
    }
    s = DeleteSlave(table, partition, ip_port);
  }
  return s;
}

// Return InvalidArgument means it is master
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
  
  if (*pptr != new_p) {
    pptr->CopyFrom(new_p);
    table_changed_[table] = true;
  }
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
  for (int i = 0; i < pptr->slaves_size(); i++) {
    if (IsSameNode(pptr->slaves[i], ip_port)) {
      // swap with master
      tmp.CopyFrom(pptr->master());
      pptr->mutable_master()->CopyFrom(pptr->slaves[i]);
      pptr->mutable_slaves(i)->CopyFrom(tmp);
      break;
    }
  }
  
  if (!tmp.IsInitialized()) {
    return Status::NotFound("Node is not slave");
  }
  table_changed_[table] = true;
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::AddTable(const std::string& table, int num) {
  if (tables_.find(table) != tables_.end()) {
    return Status::Complete("Table already exist");
  }

  // Distinguish node with the server address its located on
  std::string ip;
  int port = 0;
  ZPMeta::Node node;
  std::map<std::string, std::vector<ZPMeta::Node> > server_nodes;
  for (const auto& n : nodes_) {
    if (n.second != ZPMeta::NodeState::UP) {
      continue;
    }
    slash::ParseIpPortString(n.first, &ip, &port);
    if (server_nodes.find(ip) == server_nodes.end()) {
      server_nodes.insert(std::pair<std::string, std::vector<std::string> >(
            ip, std::vector<std::string>()));
    }
    node->clear();
    node->set_ip(ip);
    node->set_port(port);
    server_nodes[ip].push_back(node);
  }

  // Reorganize
  int node_index = 0, finish_count = 0;
  std::vector<ZPMeta::Node> cross_nodes;
  while (finish_count < server_nodes.size()) {
    finish_count = 0;
    for (const auto& sn : server_nodes) {
      if (node_index >= sn.second.size()) {
        // No more node on this sever
        finish_count++;
        continue;
      }
      ZPMeta::Node mnode;
      mnode.CopyFrom(sn.second.at(node_index));
      cross_nodes.push_back(monode);
    }
    node_index++;
  }

  // Distribute
  ZPMeta::Table meta_table;
	std::srand(std::time(0));
	int rand_pos = (std::rand() % cross_nodes.size());
  for (int i = 0; i < num; i++) {
    ZPMeta::Partitions *p = meta_table.add_partitions();
    p->set_id(i);
    p->set_state(ZPMeta::PState::ACTIVE);
    
    p->mutable_master()->CopyFrom(
        cross_nodes[(i + rand_pos) % cross_nodes.size()]);
    p->add_slaves()->CopyFrom(cross_nodes[(i + rand_pos + 1) % an_num]);
    p->add_slaves()->CopyFrom(cross_nodes[(i + rand_pos + 2) % an_num]);
  }
  tables_.insert(std::pair<std::string, std::string>(table, meta_table));
  table_changed_.insert(std::pair<std::string, std::string>(table, true));
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::SetStuck(const std::string& table,
    int partition) {
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("Table not exist");
  }
  ZPMeta::Table* tptr = &(tables_[table]);
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);
  if (!pptr) {
    return Status::NotFound("Partition not exist");
  }
  pptr->set_state(ZPMeta::PState::STUCK);
  table_changed_[table] = true;
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::RemoveTable(const std::string& table) {
  tables_.erase(table);
  table_changed_[table] = true;
  return Status::OK();
}

void ZPMetaInfoStoreSnap::RefreshTableWithNodeAlive() {
  std::string ip_port;
  for (auto& table : tables_) {
    for (auto& partition : table.second.partitions) {
      if (IsNodeUp(partition.master())) {
        continue;
      }

      // Find up slave
      table_changed_[table] = true;
      ZPMeta::Node tmp;
      for (int i = 0; i < partition.slaves_size(); i++) {
        if (IsNodeUp(partition.slave(i))) {
          tmp.CopyFrom(partition.master());
          partition.mutable_master()->CopyFrom(partition.slave(i));
          partition.mutable_slaves(i)->CopyFrom(partition.master());
        }
      }

      if (!IsNodeUp(partition.master())) {
        // all master and slave are down
        tmp.clear();
        partition.add_slaves()->CopyFrom(partition.master());
        partition.mutable_master()->CopyFrom(tmp);
      }
    }
  }
}

void ZPMetaInfoStoreSnap::SerializeNodes(ZPMeta::Nodes* nodes_ptr) const {
  int port;
  std::string ip;
  nodes_ptr->Clear();
  for (const auto& n : nodes_) {
    slash::ParseIpPortString(n.first, &ip, &port);
    ZPMeta::NodeStatus* ns = node_ptr->add_nodes();
    ZPMeta::Node* nsn = ns->mutable_node();
    nsn->set_ip(ip);
    nsn->set_port(port);
    ns->set_status(n.second);
  }
}

inline bool ZPMetaInfoStoreSnap::IsNodeUp(const ZPMeta::Node& node) const {
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  if (nodes_.find(ip_port) == nodes_.end()
      || nodes_[ip_port] != ZPMeta::NodeState::UP) {
    return false;
  }
  return true;
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

Status ZPMetaInfoStore::GetTableList(std::set<std::string>* table_list) const {
  table_list->clear();
  for (const auto& t : tables_) {
    table_list->insert(t.first);
  }
  return Status::OK();
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
  snap->snap_epoch_ = epoch_;
  GetAllTables(&(snap->tables_))
  for (const auto& t : snap->tables_) {
    snap->table_changed_[t.first] = false;
  }
  GetAllNodes(&(snap->nodes_))
}

Status ZPMetaInfoStore::Apply(const ZPMetaInfoStoreSnap& snap) {
  if (epoch_ != snap.snap_epoch_) {
    // epoch has changed, which means I'm not master now or not long ago
    return Status::Corruption("With expired epoch");
  }
  Status s;
  std::string value;
  bool epoch_change = false;

  // Update tables_
  ZPMeta::TableName table_list;
  for (const auto& t : snap->tables_) {
    table_list.add_name(t.first);
    if (!snap->table_changed_[t.first]) {
      continue;
    }
    epoch_change = true;  // Epoch update as long as some table changed
    if (!t.sercond.SerializeToString(&value)) {
      LOG(WARNING) << "SerializeToString ZPMeta::Table failed.";
      return Status::InvalidArgument("Failed to serialize Table");
    }
    s = floyd_->Set(t.first, value);
    if (!s.ok()) {
      LOG(ERROR) << "Set table failed: " << s.ToString()
        << ", Value: " << value;
      return s
    }
  }

  // Update tablelist
  if (!table_list.SerializeToString(&value)) {
    LOG(WARNING) << "SerializeToString ZPMeta::TableName failed.";
    return Status::InvalidArgument("Failed to serialize Table List");
  }
  s = floyd_->Set(kMetaTables, value);
  if (!s.ok()) {
    LOG(ERROR) << "Set tablelist failed: " << s.ToString()
      << ", Value: " << value;
    return s
  }

  // Update nodes_
  if (snap.node_changed_) {
    ZPMeta::Nodes new_nodes;
    snap->SerializeNodes(&new_nodes);
    if (!new_nodes.SerializeToString(&value)) {
      LOG(WARNING) << "SerializeToString ZPMeta::Node failed.";
      return Status::InvalidArgument("Failed to serialize nodes");
    }
    s = floyd_->Set(kMetaNodes, value);
    if (!s.ok()) {
      LOG(ERROR) << "Set meta nodes failed: " << s.ToString()
        << ", Value: " << value;
      return s;
    }
  }

  // Epoch + 1
  if (epoch_change) {
    s = floyd_->Set(kMetaVersion, snap->snap_epoch_ + 1);
    if (!s.ok()) {
      LOG(ERROR) << "Add Epoch failed: " << s.ToString()
        << ", Value: " << snap->snap_epoch_ + 1;
      return s
    }
  }

  return Refresh();
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
    std::unordered_map<std::string, ZPMeta::NodeState>* all_nodes) const {
  all_nodes->clear();
  for (const auto& t : node_alive_) {
    if (t.second > 0) {
      all_nodes[t.first] = ZPMeta::NodeState::UP;
    } else {
      all_nodes[t.first] = ZPMeta::NodeState::DOWN;
    }
  }
}


Status ZPMetaInfoStore::GetPartitionMaster(const std::string& table,
    int partition, ZPMeta::Node* master) {
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("Table not exist");
  }

  ZPMeta::Table* tptr = &(tables_[table]);
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(par);
  if (!pptr) {
    return Status::NotFound("Partition not exist");
  }
  master->clear();
  master->CopyFrom(pptr->master());
  return Status::OK();
}


