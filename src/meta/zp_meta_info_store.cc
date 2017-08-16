#include "src/meta/zp_meta_info_store.h"

#include "include/zp_const.h"

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
    if (node_s.status() == 0) {
      // Alive node
      ip_port = slash::IpPortString(node_s.node().ip(), node_s.node().port());
      node_alive_[ip_port] = slash::NowMicros();
    }
  }
  return Status::OK();
}

// Return true when the node is new added
bool ZPMetaInfoStore::UpdateNodeAlive(const std::string& node) {
  bool is_new = false;
  if (node_alive_.find(node) == node_alive_.end()) {
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
    if (slash::NowMicros() - na.second > kNodeMetaTimeoutM * 1000) {
      nodes->insert(na.first);
      it = node_alive_.erase(it);
    } else {
      ++it;
    }
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



