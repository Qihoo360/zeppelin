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
#include "src/meta/zp_meta_info_store.h"

#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <map>
#include <ctime>
#include <utility>
#include <vector>

#include "slash/include/env.h"
#include "slash/include/slash_string.h"
#include "slash/include/slash_coding.h"
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

static bool IsNodeEmpty(const ZPMeta::Node& node) {
  return node.ip().empty() || node.port() == 0;
}

static bool AssignPbNode(const std::string& ip_port,
    ZPMeta::Node* node) {
  std::string ip;
  int port = 0;
  if (!slash::ParseIpPortString(ip_port, ip, port)) {
    return false;
  }
  node->set_ip(ip);
  node->set_port(port);
  return true;
}

static void AddNodeTable(const std::string& ip_port,
    const std::string& table,
    std::unordered_map<std::string, std::set<std::string> >* ntable) {
  const auto iter = ntable->find(ip_port);
  if (iter == ntable->end()) {
    ntable->insert(std::pair<std::string,
        std::set<std::string> >(ip_port, std::set<std::string>()));
  }
  (*ntable)[ip_port].insert(table);
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

void NodeInfo::Dump() {
  LOG(INFO) << "---------dump NodeInfo---------";
  LOG(INFO) << "last alive: " << last_alive_time;
  for (const auto& of : offsets) {
    LOG(INFO) << of.first << " => " << of.second.filenum
      << "_" << of.second.offset;
  }
  LOG(INFO) << "-------------------------------";
}

/*
 * class ZPMetaInfoStoreSnap
 */

ZPMetaInfoStoreSnap::ZPMetaInfoStoreSnap()
  : snap_epoch_(-1),
  node_changed_(false) {
  }

Status ZPMetaInfoStoreSnap::UpNode(const std::string& ip_port) {
  if (nodes_.find(ip_port) == nodes_.end()
      || nodes_[ip_port].last_alive_time == 0) {
    node_changed_ = true;
  }
  nodes_[ip_port].last_alive_time = slash::NowMicros();
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::DownNode(const std::string& ip_port) {
  if (nodes_.find(ip_port) != nodes_.end()) {
    if (nodes_[ip_port].last_alive_time > 0) {
      node_changed_ = true;
    }
    nodes_[ip_port].last_alive_time = 0;
  }
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::RemoveNodes(
    const ZPMeta::MetaCmd_RemoveNodes& remove_nodes_cmd) {
  for (int i = 0; i < remove_nodes_cmd.nodes_size(); i++) {
    const ZPMeta::Node& n = remove_nodes_cmd.nodes(i);
    std::string node = slash::IpPortString(n.ip(), n.port());
    if (IsNodeUp(n)) {
      return Status::Corruption("Node " + node + " is running");
    }
    if (node_table_.find(node) != node_table_.end() &&
        node_table_.at(node).size() > 0) {  // node has table load
      return Status::Corruption("Node " + node + " has table load");
    }
    node_changed_ = true;
    nodes_.erase(node);
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
  if (partition < 0 || partition >= tptr->partitions_size()) {
    return Status::NotFound("Partition not exist");
  }
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);

  if (IsSameNode(pptr->master(), ip_port)) {
    return Status::OK();  // Already be master
  }
  for (const auto& s : pptr->slaves()) {
    if (IsSameNode(s, ip_port)) {
      return Status::OK();  // Already be slave
    }
  }

  ZPMeta::Node* new_slave = pptr->add_slaves();
  AssignPbNode(ip_port, new_slave);
  table_changed_[table] = true;
  return Status::OK();
}

// Handover from left_node to right_node
Status ZPMetaInfoStoreSnap::Handover(const std::string& table, int partition,
    const std::string& left_node, const std::string& right_node) {
  Status s = Status::OK();

  // Assume the left_node is slave and try
  if (DeleteSlave(table, partition, left_node).IsInvalidArgument()) {
    // Table and parition is exist but current node is master
    s = SetMaster(table, partition, right_node);
    if (!s.ok()) {
      return s;
    }
    s = DeleteSlave(table, partition, left_node);
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
  if (partition < 0 || partition >= tptr->partitions_size()) {
    return Status::NotFound("Partition not exist");
  }
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);

  if (IsSameNode(pptr->master(), ip_port)) {
    return Status::InvalidArgument("Not slave");  // not slave
  }

  ZPMeta::Partitions new_p;
  for (const auto& s : pptr->slaves()) {
    if (!IsSameNode(s, ip_port)) {
      ZPMeta::Node* new_slave = new_p.add_slaves();
      new_slave->CopyFrom(s);
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
  if (partition < 0 || partition >= tptr->partitions_size()) {
    return Status::NotFound("Partition not exist");
  }
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);

  if (IsSameNode(pptr->master(), ip_port)) {
    return Status::OK();  // Already be master
  }

  ZPMeta::Node tmp;
  for (int i = 0; i < pptr->slaves_size(); i++) {
    if (IsSameNode(pptr->slaves(i), ip_port)) {
      // swap with master
      tmp.CopyFrom(pptr->master());
      pptr->mutable_master()->CopyFrom(pptr->slaves(i));
      if (!IsNodeEmpty(tmp)) {
        pptr->mutable_slaves(i)->CopyFrom(tmp);
      } else {
        // old master is empty, discard it
        pptr->mutable_slaves(i)->CopyFrom(
            pptr->slaves(pptr->slaves_size() - 1));
        pptr->mutable_slaves()->RemoveLast();
      }
      break;
    }
  }

  if (!tmp.IsInitialized()) {
    return Status::NotFound("Node is not slave");
  }
  table_changed_[table] = true;
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::AddTable(const ZPMeta::Table& table) {
  const std::string& table_name = table.name();
  if (tables_.find(table_name) != tables_.end()) {
    return Status::Complete("Table already exist");
  }

  tables_.insert(std::make_pair(table_name, table));
  table_changed_.insert(std::make_pair(table_name, true));
  return Status::OK();
}

// Set stuck if to_stuck is true, otherwise set alive
Status ZPMetaInfoStoreSnap::ChangePState(const std::string& table,
    int partition, const ZPMeta::PState& target_s) {
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("Table not exist");
  }
  ZPMeta::Table* tptr = &(tables_[table]);
  if (partition < 0 || partition >= tptr->partitions_size()) {
    return Status::NotFound("Partition not exist");
  }
  ZPMeta::Partitions* pptr = tptr->mutable_partitions(partition);
  
  if (pptr->state() == target_s) {
    // No changed
    return Status::OK();
  }

  pptr->set_state(target_s);
  table_changed_[table] = true;
  return Status::OK();
}

Status ZPMetaInfoStoreSnap::RemoveTable(const std::string& table) {
  if (tables_.find(table) == tables_.end()) {
    return Status::OK();
  }
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
      int slave_count = partition->slaves_size();
      for (int j = 0; j < slave_count; j++) {
        if (IsNodeUp(partition->slaves(j))) {
          tmp_offset.Clear();
          GetNodeOffset(partition->slaves(j), table.first, j, &tmp_offset);
          if (tmp_offset >= max_offset) {
            max_slave = j;
            max_offset = tmp_offset;
          }
        }
      }

      // Swap with slave with max offset
      ZPMeta::Node tmp;
      if (max_slave != -1) {
        tmp.CopyFrom(partition->master());
        partition->mutable_master()->CopyFrom(partition->slaves(max_slave));
        if (IsNodeEmpty(tmp)) {
          // Remove the emtpy master
          tmp.CopyFrom(partition->slaves(slave_count - 1));
          partition->mutable_slaves()->RemoveLast();
        }
        partition->mutable_slaves(max_slave)->CopyFrom(tmp);
      }

      // All master and slave are down
      if (!IsNodeEmpty(partition->master())
          && !IsNodeUp(partition->master())) {
        tmp.Clear();
        tmp.set_ip("");
        tmp.set_port(0);
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

// Refresh node_table_, table_info_
Status ZPMetaInfoStore::Refresh() {
  std::string value;

  // Get Version
  int tmp_epoch = -1;
  Status fs = floyd_->Read(kMetaVersion, &value);
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
    return Status::OK();
  }
  LOG(INFO) << "Load epoch from floyd succ, tmp version : " << tmp_epoch;

  // Read table names
  ZPMeta::TableName table_names;
  fs = floyd_->Read(kMetaTables, &value);
  if (!fs.ok()) {
    LOG(ERROR) << "Load meta table names failed: " << fs.ToString();
    return Status::IOError(fs.ToString());
  }
  if (!table_names.ParseFromString(value)) {
    LOG(ERROR) << "Deserialization meta table names failed";
    return Status::Corruption("Parse failed");
  }
  LOG(INFO) << "Load table names from floyd succ, table names size: "
    << table_names.name_size() << ", value size: " << value.size();

  // Read tables and update node_table_, table_info_
  std::string ip_port;
  ZPMeta::Table table_info;
  std::set<std::string> miss_tables;
  std::unordered_map<std::string, std::set<std::string> > tmp_node_table;
  for (const auto& ot : table_info_) {
    miss_tables.insert(ot.first);
  }
  for (const auto& t : table_names.name()) {
    fs = floyd_->Read(t, &value);
    if (!fs.ok()) {
      LOG(ERROR) << "Load floyd table_info failed: " << fs.ToString()
        << ", table name: " << t;
      return Status::IOError(fs.ToString());
    }
    if (!table_info.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table_info failed, table: "
        << t;
      return Status::Corruption("Parse failed");
    }

    {
    slash::RWLock l(&tables_rw_, true);
    miss_tables.erase(t);
    if (table_info_.find(t) == table_info_.end()) {
      table_info_.insert(
          std::pair<std::string, ZPMeta::Table>(t, ZPMeta::Table()));
    }
    table_info_.at(t).CopyFrom(table_info);

    for (const auto& partition : table_info.partitions()) {
      if (!IsNodeEmpty(partition.master())) {
        ip_port = slash::IpPortString(partition.master().ip(),
            partition.master().port());
        AddNodeTable(ip_port, t, &tmp_node_table);
      }

      for (int k = 0; k < partition.slaves_size(); k++) {
        ip_port = slash::IpPortString(partition.slaves(k).ip(),
            partition.slaves(k).port());
        AddNodeTable(ip_port, t, &tmp_node_table);
      }
    }

    }
  }

  {
  slash::RWLock l(&tables_rw_, true);
  for (const auto& mt : miss_tables) {
    table_info_.erase(mt);
  }
  node_table_.clear();
  node_table_ = tmp_node_table;
  }

  LOG(INFO) << "Update node_table_ from floyd succ.";
  //NodesDebug();

  // Update Version
  epoch_ = tmp_epoch;
  LOG(INFO) << "Update epoch: " << epoch_;
  return Status::OK();
}

Status ZPMetaInfoStore::RestoreNodeInfos() {
  {
    slash::RWLock l(&nodes_rw_, true);
    node_infos_.clear();
  }
  return RefreshNodeInfos();
}

Status ZPMetaInfoStore::RefreshNodeInfos() {
  // Read all nodes
  std::string value;
  ZPMeta::Nodes allnodes;

  slash::RWLock l(&nodes_rw_, true);
  Status fs = floyd_->Read(kMetaNodes, &value);
  if (fs.IsNotFound()) {
    node_infos_.clear();
    return Status::OK();  // no meta info exist
  }
  if (!fs.ok()) {
    LOG(ERROR) << "Load meta nodes failed: " << fs.ToString();
    return fs;
  }
  if (!allnodes.ParseFromString(value)) {
    LOG(ERROR) << "Deserialization nodes failed";
    return slash::Status::Corruption("Parse failed");
  }

  std::string ip_port;
  std::set<std::string> miss;
  for (const auto& n : node_infos_) {
    miss.insert(n.first);
  }
  for (const auto& node_s : allnodes.nodes()) {
    ip_port = slash::IpPortString(node_s.node().ip(), node_s.node().port());
    miss.erase(ip_port);

    if (node_infos_.find(ip_port) == node_infos_.end()    // new node
        || !(node_infos_.at(ip_port).StateEqual(node_s.status()))) {
      // node state changed
      node_infos_[ip_port] = NodeInfo(node_s.status());
    }
  }
  for (const auto& m : miss) {
    // Remove overdue node
    node_infos_.erase(m);
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
    DLOG(INFO) << "update offset"
      << ", node: " << node
      << ", key: " << offset_key
      << ", offset: " << po.filenum() << "_" << po.offset();
    node_infos_[node].offsets[offset_key] = NodeOffset(po.filenum(),
        po.offset());
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

bool ZPMetaInfoStore::GetNodeInfo(const ZPMeta::Node& node, NodeInfo* info) {
  slash::RWLock l(&nodes_rw_, false);
  std::string n = slash::IpPortString(node.ip(), node.port());
  if (node_infos_.find(n) == node_infos_.end()) {
    return false;
  }
  *info = node_infos_.at(n);
  return true;
}

void ZPMetaInfoStore::FetchExpiredNode(std::set<std::string>* nodes) {
  nodes->clear();
  slash::RWLock l(&nodes_rw_, false);
  auto it = node_infos_.begin();
  while (it != node_infos_.end()) {
    if (it->second.last_alive_time > 0
        && (slash::NowMicros() - it->second.last_alive_time
          > kNodeMetaTimeoutM * 1000 * 1000)) {
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
  //LOG(INFO) << "node: " << node.ip() << ":" << node.port();
  //node_infos_.at(ip_port).Dump();
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

Status ZPMetaInfoStore::GetTableList(std::set<std::string>* table_list) {
  table_list->clear();
  slash::RWLock l(&tables_rw_, false);
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
  if (table_info_.find(table) == table_info_.end()
      || table_info_.at(table).partitions_size() <= partition) {
    return Status::NotFound("Table or partition not exist");
  }

  master->Clear();
  master->CopyFrom(table_info_.at(table).partitions(partition).master());
  return Status::OK();
}

bool ZPMetaInfoStore::IsSlave(const std::string& table,
    int partition, const ZPMeta::Node& target) {
  slash::RWLock l(&tables_rw_, false);
  if (!PartitionExistNoLock(table, partition)) {
    return false;
  } 

  for (const auto slave :
      table_info_.at(table).partitions(partition).slaves()) {
    if (slave.ip() == target.ip()
        && slave.port() == target.port()) {
      return true;
    }
  }
  return false;
}

bool ZPMetaInfoStore::IsMaster(const std::string& table,
    int partition, const ZPMeta::Node& target) {
  slash::RWLock l(&tables_rw_, false);
  if (!PartitionExistNoLock(table, partition)) {
    return false;
  } 

  ZPMeta::Node master = table_info_.at(table).partitions(partition).master();
  if (master.ip() == target.ip()
      && master.port() == target.port()) {
    return true;
  }
  return false;
}

bool ZPMetaInfoStore::PartitionExist(const std::string& table,
    int partition) {
  slash::RWLock l(&tables_rw_, false);
  return PartitionExistNoLock(table, partition);
}

bool ZPMetaInfoStore::PartitionExistNoLock(const std::string& table,
    int partition) {
  if (table_info_.find(table) == table_info_.end()
      || table_info_.at(table).partitions_size() <= partition
      || partition < 0) {
    return false;
  }
  return true;
}

void ZPMetaInfoStore::GetSnapshot(ZPMetaInfoStoreSnap* snap) {
  // No lock here may give rise to the inconsistence
  // between snap epcho and snap table or snap nodes,
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
  GetAllTables(&snap->tables_);
  for (const auto& t : snap->tables_) {
    snap->table_changed_[t.first] = false;
  }
  GetAllNodes(&snap->nodes_);
  snap->node_table_ = node_table_;
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
  for (const auto& t : snap.table_changed_) {
    auto iter_table = snap.tables_.find(t.first);
    if (iter_table == snap.tables_.end()) {
      // Table be removed
      epoch_change = true;
      continue;
    }
    table_list.add_name(t.first);

    if (!t.second) {
      continue;
    }
    epoch_change = true;  // Epoch update as long as some table changed

    std::string text_format;
    google::protobuf::TextFormat::PrintToString(iter_table->second, &text_format);
    DLOG(INFO) << "Set table to floyd [" << text_format << "]";

    if (!iter_table->second.SerializeToString(&value)) {
      LOG(WARNING) << "SerializeToString ZPMeta::Table failed. Table: "
        << t.first;
      return Status::InvalidArgument("Failed to serialize Table");
    }
    s = floyd_->Write(t.first, value);
    if (!s.ok()) {
      LOG(ERROR) << "Set table failed: " << s.ToString()
        << ", Table: " << t.first << ", value size: " << value.size();
      return Status::IOError(s.ToString());
    }
    LOG(INFO) << "Write table to floyd succ, table : " << t.first;
  }

  // Update tablelist
  if (epoch_change) {
    if (!table_list.SerializeToString(&value)) {
      LOG(WARNING) << "SerializeToString ZPMeta::TableName failed.";
      return Status::InvalidArgument("Failed to serialize Table List");
    }
    s = floyd_->Write(kMetaTables, value);
    if (!s.ok()) {
      LOG(ERROR) << "Set tablelist failed: " << s.ToString();
      return Status::IOError(s.ToString());
    }
    LOG(INFO) << "Write table list to floyd succ";
  }

  if (snap.node_changed_) {
    // Update nodes_
    ZPMeta::Nodes new_nodes;
    snap.SerializeNodes(&new_nodes);
    if (!new_nodes.SerializeToString(&value)) {
      LOG(WARNING) << "SerializeToString ZPMeta::Node failed.";
      return Status::InvalidArgument("Failed to serialize nodes");
    }
    s = floyd_->Write(kMetaNodes, value);
    if (!s.ok()) {
      LOG(ERROR) << "Set meta nodes failed: " << s.ToString();
      return Status::IOError(s.ToString());
    }
    LOG(INFO) << "Write nodes to floyd succ";
  }

  if (epoch_change) {
    // Epoch + 1
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

  // Refresh cache, notice the order
  if (epoch_change) {
    s = Refresh();
    if (!s.ok()) {
      LOG(ERROR) << "Refresh tables info after apply failed: " << s.ToString();
      return s;
    }
  }

  if (snap.node_changed_) {
    s = RefreshNodeInfos();
    if (!s.ok()) {
      LOG(ERROR) << "Refresh nodes info after apply failed: " << s.ToString();
      return s;
    }
  }
  LOG(INFO) << "Refresh meta info after apply succ";

  return Status::OK();
}

