#include "src/meta/zp_meta_info_store.h"

#include <ctime>
#include <glog/logging.h>
#include "slash/include/env.h"
#include "slash/include/slash_string.h"
#include "slash/include/slash_mutex.h"
#include "include/zp_const.h"

std::string NodeOffsetKey(const std::string& table, int partition_id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s_%u",
      table.c_str(), partition_id);
  return std::string(buf);
}

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

Status NodeInfo::GetOffset(const std::string& table, int partition_id,
    NodeOffset* noffset) const {
  std::string key = NodeOffsetKey(table, partition_id);
  if (offsets.find(key) == offsets.end()) {
    return Status::NotFound("table parititon not found");
  }
  *noffset = offsets.at(key);
  return Status::OK();
}

/*
 * class ZPMetaInfoStoreSnap
 */

ZPMetaInfoStoreSnap::ZPMetaInfoStoreSnap()
  : snap_epoch_(-1),
  node_changed_(false) {
  }

Status ZPMetaInfoStoreSnap::UpNode(const std::string& ip_port) {
  node_changed_ = (nodes_.find(ip_port) == nodes_.end()
      || nodes_[ip_port].last_alive_time == 0);
  nodes_[ip_port].last_alive_time = slash::NowMicros();
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::DownNode(const std::string& ip_port) {
  if (nodes_.find(ip_port) != nodes_.end()) {
    node_changed_ = (nodes_[ip_port].last_alive_time > 0);
    nodes_[ip_port].last_alive_time = 0;
  }
  return Status::OK();
} 

Status ZPMetaInfoStoreSnap::GetNodeOffset(const ZPMeta::Node& node,
    const std::string& table, int partition_id, NodeOffset* noffset) const {
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  if (nodes_.find(ip_port) == nodes_.end()) {
    return Status::NotFound("node not exist");
  }
  return nodes_.at(ip_port).GetOffset(table, partition_id, noffset);
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

// Handover from ip_port_o to ip_port
Status ZPMetaInfoStoreSnap::Handover(const std::string& table, int partition,
    const std::string& ip_port, const std::string& ip_port_o) {
  Status s = Status::OK();
  
  // Assume the ip_port_o is slave and try
  if (DeleteSlave(table, partition, ip_port_o).IsInvalidArgument()) {
    // Table and parition is exist but current node is master
    s = SetMaster(table, partition, ip_port);
    if (!s.ok()) {
      return s;
    }
    s = DeleteSlave(table, partition, ip_port_o);
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
  new_p.set_state(pptr->state());
  new_p.mutable_master()->CopyFrom(pptr->master());
  
  if (pptr->slaves_size() != new_p.slaves_size()) {
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
    if (IsSameNode(pptr->slaves(i), ip_port)) {
      // swap with master
      tmp.CopyFrom(pptr->master());
      pptr->mutable_master()->CopyFrom(pptr->slaves(i));
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
    if (n.second.last_alive_time == 0) {
      continue;
    }
    slash::ParseIpPortString(n.first, ip, port);
    if (server_nodes.find(ip) == server_nodes.end()) {
      server_nodes.insert(std::pair<std::string, std::vector<ZPMeta::Node> >(
            ip, std::vector<ZPMeta::Node>()));
    }
    node.Clear();
    node.set_ip(ip);
    node.set_port(port);
    server_nodes[ip].push_back(node);
  }

  // Reorganize
  size_t node_index = 0, finish_count = 0;
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
      cross_nodes.push_back(mnode);
    }
    node_index++;
  }

  if (cross_nodes.empty()) {
    return Status::Corruption("No zp node exist");
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
    p->add_slaves()->CopyFrom(
        cross_nodes[(i + rand_pos + 1) % cross_nodes.size()]);
    p->add_slaves()->CopyFrom(
        cross_nodes[(i + rand_pos + 2) % cross_nodes.size()]);
  }
  tables_.insert(std::pair<std::string, ZPMeta::Table>(table, meta_table));
  table_changed_.insert(std::pair<std::string, bool>(table, true));
  return Status::OK();
}

// Set stuck if to_stuck is true, otherwise set alive 
Status ZPMetaInfoStoreSnap::ChangePState(const std::string& table,
    int partition, bool to_stuck) {
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("Table not exist");
  }
  ZPMeta::Table* tptr = &(tables_[table]);
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);
  if (!pptr) {
    return Status::NotFound("Partition not exist");
  }
  if (to_stuck) {
    pptr->set_state(ZPMeta::PState::STUCK);
  } else {
    pptr->set_state(ZPMeta::PState::ACTIVE);
  }
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
    for (int i = 0; i < table.second.partitions_size(); i++) {
      ZPMeta::Partitions* partition = table.second.mutable_partitions(i);
      if (IsNodeUp(partition->master())) {
        continue;
      }

      // Find up slave
      table_changed_[table.first] = true;
      int max_slave = -1;
      NodeOffset tmp_offset, max_offset;
      for (int i = 0; i < partition->slaves_size(); i++) {
        if (IsNodeUp(partition->slaves(i))) {
          tmp_offset.Clear();
          GetNodeOffset(partition->slaves(i), table.first, i, &tmp_offset);
          if (tmp_offset >= max_offset) {
            max_slave = i;
            max_offset = tmp_offset;
          }
        }
      }

      ZPMeta::Node tmp;
      if (max_slave == -1) {
        // Find one
        tmp.CopyFrom(partition->slaves(max_slave));
        partition->mutable_slaves(i)->CopyFrom(partition->master());
        partition->mutable_master()->CopyFrom(tmp);
      }

      if (!IsNodeUp(partition->master())) {
        // all master and slave are down
        tmp.Clear();
        partition->add_slaves()->CopyFrom(partition->master());
        partition->mutable_master()->CopyFrom(tmp);
      }
    }
  }
}

void ZPMetaInfoStoreSnap::SerializeNodes(ZPMeta::Nodes* nodes_ptr) const {
  int port;
  std::string ip;
  nodes_ptr->Clear();
  for (const auto& n : nodes_) {
    slash::ParseIpPortString(n.first, ip, port);
    ZPMeta::NodeStatus* ns = nodes_ptr->add_nodes();
    ZPMeta::Node* nsn = ns->mutable_node();
    nsn->set_ip(ip);
    nsn->set_port(port);
    if (n.second.last_alive_time > 0) {
      ns->set_status(ZPMeta::NodeState::UP);
    } else {
      ns->set_status(ZPMeta::NodeState::DOWN);
    }
  }
}

inline bool ZPMetaInfoStoreSnap::IsNodeUp(const ZPMeta::Node& node) const {
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  if (nodes_.find(ip_port) == nodes_.end()
      || nodes_.at(ip_port).last_alive_time == 0) {
    return false;
  }
  return true;
}

/*
 * class ZPMetaInfoStore
 */

ZPMetaInfoStore::ZPMetaInfoStore(floyd::Floyd* floyd)
  : floyd_(floyd),
  epoch_(-1) {
    // We prefer write for nodes_info_
    // since its on the critical path of Ping, which is latency sensitive
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr,
        PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&nodes_rw_, &attr);

    pthread_rwlock_init(&tables_rw_, NULL);
  }

ZPMetaInfoStore::~ZPMetaInfoStore() {
  pthread_rwlock_destroy(&nodes_rw_);
  pthread_rwlock_destroy(&tables_rw_);
}

Status ZPMetaInfoStore::Refresh() {
  std::string value;

  // Get Version
  int tmp_epoch = -1;
  Status fs = floyd_->Read(kMetaVersion, value);
  if (fs.ok()) {
    tmp_epoch = std::stoi(value);
  } else if (fs.IsNotFound()) {
    LOG(INFO) << "Epoch not found in floyd, set -1";
    epoch_ = -1;
    return Status::OK();
  } else {
    LOG(ERROR) << "Load epoch failed: " << fs.ToString();
    return Status::IOError(fs.ToString());
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
    return Status::IOError(fs.ToString());
  }
  if (!table_names.ParseFromString(value)) {
    LOG(ERROR) << "Deserialization meta table names failed, value: " << value;
    return Status::Corruption("Parse failed");
  }
  LOG(INFO) << "Load table names from floyd succ, table names size: "
    << table_names.name_size();

  // Read tables and update node_table_, table_info_
  {
  slash::RWLock l(&tables_rw_, true);
  node_table_.clear();
  table_info_.clear();
  std::string ip_port;
  ZPMeta::Table table_info;
  for (const auto& t : table_names.name()) {
    fs = floyd_->Read(t, value);
    if (!fs.ok()) {
      LOG(ERROR) << "Load floyd table_info failed: " << fs.ToString()
        << ", table name: " << t;
      return Status::IOError(fs.ToString());
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
  }

  // Update Version
  epoch_ = tmp_epoch;
  LOG(INFO) << "Update epoch: " << epoch_;
  return Status::OK();
}

Status ZPMetaInfoStore::RestoreNodeInfos() {
  // Read all nodes
  std::string value;
  ZPMeta::Nodes allnodes;

  slash::RWLock l(&nodes_rw_, true);
  Status fs = floyd_->Read(kMetaNodes, value);
  if (fs.IsNotFound()) {
    node_infos_.clear();
    return Status::OK();  // no meta info exist
  }
  if (!fs.ok()) {
    LOG(ERROR) << "Load meta nodes failed: " << fs.ToString();
    return fs;
  }
  if (!allnodes.ParseFromString(value)) {
    LOG(ERROR) << "Deserialization nodes failed, value: " << value;
    return slash::Status::Corruption("Parse failed");
  }
  
  std::string ip_port;
  node_infos_.clear();
  for (const auto& node_s : allnodes.nodes()) {
    ip_port = slash::IpPortString(node_s.node().ip(), node_s.node().port());
    node_infos_[ip_port] = NodeInfo(node_s.status() == ZPMeta::NodeState::UP);
  }
  return Status::OK();
}

// Return false when the node is new alive
bool ZPMetaInfoStore::UpdateNodeInfo(const ZPMeta::MetaCmd_Ping &ping) {
  slash::RWLock l(&nodes_rw_, true);
  std::string node = slash::IpPortString(ping.node().ip(),
      ping.node().port());
  bool not_found = false;
  if (node_infos_.find(node) == node_infos_.end()
      || node_infos_[node].last_alive_time == 0) {
    not_found = true;
  }
  
  // Update offset
  for (const auto& po : ping.offset()) {
    std::string offset_key = NodeOffsetKey(po.table_name(), po.partition());
    node_infos_[node].offsets[offset_key] = NodeOffset(po.filenum(), po.offset());
  }
  
  if (not_found) {
    // Do not add alive time info here.
    // Leave this in Refresh() to keep it consistent with what in floyd
    return false;
  }

  // Update alive time
  node_infos_.at(node).last_alive_time = slash::NowMicros();
  return true;
}

void ZPMetaInfoStore::FetchExpiredNode(std::set<std::string>* nodes) {
  nodes->clear();
  slash::RWLock l(&nodes_rw_, false);
  auto it = node_infos_.begin();
  while (it != node_infos_.end() ) {
    if (it->second.last_alive_time > 0
        && (slash::NowMicros() - it->second.last_alive_time
          > kNodeMetaTimeoutM * 1000)) {
      nodes->insert(it->first);
      // Do not erase alive info item here.
      // Leave this in Refresh() to keep it consistent with what in floyd
    }
    ++it;
  }
}

void ZPMetaInfoStore::GetAllNodes(
    std::unordered_map<std::string, NodeInfo>* all_nodes) {
  all_nodes->clear();
  slash::RWLock l(&nodes_rw_, false);
  for (const auto& t : node_infos_) {
    (*all_nodes)[t.first] = t.second;
  }
}

Status ZPMetaInfoStore::GetNodeOffset(const ZPMeta::Node& node,
    const std::string& table, int partition_id, NodeOffset* noffset) {
  slash::RWLock l(&nodes_rw_, false);
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  if (node_infos_.find(ip_port) == node_infos_.end()) {
    return Status::NotFound("node not exist");
  }
  return node_infos_.at(ip_port).GetOffset(table, partition_id, noffset);
}

// Requied: hold read or write lock of table_rw_
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

// Requied: hold read or write lock of table_rw_
void ZPMetaInfoStore::AddNodeTable(const std::string& ip_port,
       const std::string& table) {
  const auto iter = node_table_.find(ip_port);
  if (iter == node_table_.end()) {
    node_table_.insert(std::pair<std::string,
        std::set<std::string> >(ip_port, std::set<std::string>()));
  }
  node_table_[ip_port].insert(table);
}

Status ZPMetaInfoStore::GetTableList(std::set<std::string>* table_list) {
  slash::RWLock l(&tables_rw_, false);
  table_list->clear();
  for (const auto& t : table_info_) {
    table_list->insert(t.first);
  }
  return Status::OK();
}

Status ZPMetaInfoStore::GetTablesForNode(const std::string &ip_port,
    std::set<std::string> *tables) {
  slash::RWLock l(&tables_rw_, false);
  const auto iter = node_table_.find(ip_port);
  if (iter == node_table_.end()) {
    return Status::NotFound("node not exist");
  }
  tables->clear();
  *tables = iter->second;
  return Status::OK();
}

Status ZPMetaInfoStore::GetTableMeta(const std::string& table,
    ZPMeta::Table* table_meta) {
  slash::RWLock l(&tables_rw_, false);
  const auto iter = table_info_.find(table);
  if (iter == table_info_.end()) {
    return Status::NotFound("Table meta not found");
  }
  table_meta->CopyFrom(iter->second);
  return Status::OK();
}

void ZPMetaInfoStore::GetAllTables(
    std::unordered_map<std::string, ZPMeta::Table>* all_tables) {
  slash::RWLock l(&tables_rw_, false);
  all_tables->clear();
  for (const auto& t : table_info_) {
    (*all_tables)[t.first] = ZPMeta::Table();
    (*all_tables)[t.first].CopyFrom(t.second);
  }
}

Status ZPMetaInfoStore::GetPartitionMaster(const std::string& table,
    int partition, ZPMeta::Node* master) {
  slash::RWLock l(&tables_rw_, false);
  if (table_info_.find(table) == table_info_.end()) {
    return Status::NotFound("Table not exist");
  }

  const ZPMeta::Table* tptr = &(table_info_[table]);
  if (tptr->partitions_size() <= partition) {
    return Status::NotFound("Partition not exist");
  }
  master->Clear();
  master->CopyFrom(tptr->partitions(partition).master());
  return Status::OK();
}

void ZPMetaInfoStore::GetSnapshot(ZPMetaInfoStoreSnap* snap) {
  // No lock here may give rise to the inconsistence
  // between snap epch and snap table or snap nodes,
  // for example a newer table info with an older epoch.
  //
  // But it is acceptable since this occurs rarely,
  // considering the only point to change epoch is in Apply Function,
  // which will only be invoked by update thread sequentially after GetSnapshot.
  //
  // So the only inconsistence happened when Leader changed,
  // under which situation the snapshot will be invalid and should be discarded,
  // Apply function will check and handle this.
  snap->snap_epoch_ = epoch_;
  GetAllTables(&(snap->tables_));
  for (const auto& t : snap->tables_) {
    snap->table_changed_[t.first] = false;
  }
  GetAllNodes(&(snap->nodes_));
}

// Return IOError means error happened when access floyd. 
// Notice: the Apply process may be partially completed,
// so it is designed to be reentrant
Status ZPMetaInfoStore::Apply(const ZPMetaInfoStoreSnap& snap) {
  if (epoch_ != snap.snap_epoch_) {
    // Epoch has changed, which means leader has changed not long ago 
    // simply discard and leave the outside retry
    return Status::Corruption("With expired epoch");
  }
  Status s;
  std::string value;
  bool epoch_change = false;

  // Update tables_
  ZPMeta::TableName table_list;
  for (const auto& t : snap.tables_) {
    table_list.add_name(t.first);
    if (!snap.table_changed_.at(t.first)) {
      continue;
    }
    epoch_change = true;  // Epoch update as long as some table changed
    if (!t.second.SerializeToString(&value)) {
      LOG(WARNING) << "SerializeToString ZPMeta::Table failed. Table: "
        << t.first;
      return Status::InvalidArgument("Failed to serialize Table");
    }
    s = floyd_->Write(t.first, value);
    if (!s.ok()) {
      LOG(ERROR) << "Set table failed: " << s.ToString()
        << ", Table: " << t.first << ", Value: " << value;
      return Status::IOError(s.ToString());
    }
    LOG(INFO) << "Write table to floyd succ, table : " << t.first;
  }

  // Update tablelist
  if (!table_list.SerializeToString(&value)) {
    LOG(WARNING) << "SerializeToString ZPMeta::TableName failed.";
    return Status::InvalidArgument("Failed to serialize Table List");
  }
  s = floyd_->Write(kMetaTables, value);
  if (!s.ok()) {
    LOG(ERROR) << "Set tablelist failed: " << s.ToString()
      << ", Value: " << value;
    return Status::IOError(s.ToString());
  }
  LOG(INFO) << "Write table list to floyd succ, table list: " << value;

  // Update nodes_
  if (snap.node_changed_) {
    ZPMeta::Nodes new_nodes;
    snap.SerializeNodes(&new_nodes);
    if (!new_nodes.SerializeToString(&value)) {
      LOG(WARNING) << "SerializeToString ZPMeta::Node failed.";
      return Status::InvalidArgument("Failed to serialize nodes");
    }
    s = floyd_->Write(kMetaNodes, value);
    if (!s.ok()) {
      LOG(ERROR) << "Set meta nodes failed: " << s.ToString()
        << ", Value: " << value;
      return Status::IOError(s.ToString());
    }
    LOG(INFO) << "Write nodes to floyd succ";
  }

  // Epoch + 1
  if (epoch_change) {
    int new_epoch = snap.snap_epoch_ + 1;
    s = floyd_->Write(kMetaVersion, std::to_string(new_epoch));
    if (!s.ok()) {
      LOG(ERROR) << "Add Epoch failed: " << s.ToString()
        << ", Value: " << new_epoch;
      return Status::IOError(s.ToString());
    }
    LOG(INFO) << "Write new epoch to floyd succ, new epoch: " << new_epoch;
  }

  LOG(INFO) << "Apply snap to floyd succ";
  return Refresh();
}

