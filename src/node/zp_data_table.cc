#include "zp_data_table.h"

#include <glog/logging.h>

#include "zp_data_server.h"

extern ZPDataServer* zp_data_server;

Table* NewTable(const std::string &table_name, const std::string log_path, const std::string data_path) {
  Table* table = new Table(table_name, log_path, data_path);
  // TODO maybe need check
  return table;
}

//
// Table
//
Table::Table(const std::string& table_name, const std::string &log_path, const std::string &data_path)
  : table_name_(table_name),
  log_path_(log_path),
  data_path_(data_path) {
  if (log_path_.back() != '/') {
    log_path_.push_back('/');
  }
  if (data_path_.back() != '/') {
    data_path_.push_back('/');
  }
  log_path_ += table_name_ + "/";
  data_path_ += table_name_ + "/";

  slash::CreatePath(log_path_);
  slash::CreatePath(data_path_);

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&partition_rw_, NULL);
}

Table::~Table() {
  {
    slash::RWLock l(&partition_rw_, true);
    for (auto iter = partitions_.begin(); iter != partitions_.end(); iter++) {
      delete iter->second;
    }
  }
  pthread_rwlock_destroy(&partition_rw_);
}

bool Table::SetPartitionCount(const int count) {
  partition_cnt_ = count;
  DLOG(INFO) << " Set Table: " << table_name_ << " with " << partition_cnt_ << " partitions.";
  return true;
}

Partition* Table::GetPartition(const std::string &key) {
  slash::RWLock l(&partition_rw_, false);
  if (partition_cnt_ > 0) {
    int partition_id = std::hash<std::string>()(key) % partition_cnt_;
    return partitions_[partition_id]; 
  }
  return NULL;
}

Partition* Table::GetPartitionById(const int partition_id) {
  slash::RWLock l(&partition_rw_, false);
  auto it = partitions_.find(partition_id);
  //std::map<int, Partition*>::const_iterator it = partitions_.find(partition_id);
  if (it != partitions_.end()) {
    return it->second;
  }
  return NULL;
}

bool Table::UpdateOrAddPartition(const int partition_id,
    ZPMeta::PState state, const Node& master, const std::vector<Node>& slaves) {
  slash::RWLock l(&partition_rw_, true);
  auto iter = partitions_.find(partition_id);
  if (iter != partitions_.end()) {
    //Exist partition: update it
    (iter->second)->Update(state, master, slaves);
    return true;
  }

  if (state != ZPMeta::PState::ACTIVE) {
    DLOG(WARNING) << "New Partition with no active state";
    return false;
  }

  // New Partition
  Partition* partition = NewPartition(table_name_,
      log_path_, data_path_, partition_id, master, slaves);
  assert(partition != NULL);
  partitions_[partition_id] = partition;

  return true;
}

uint32_t Table::KeyToPartition(const std::string &key) {
  assert(partition_cnt_ != 0);
  return std::hash<std::string>()(key) % partition_cnt_;
}

void Table::Dump() {
  slash::RWLock l(&partition_rw_, false);
  DLOG(INFO) << "=========================";
  DLOG(INFO) << "    Table : " << table_name_;
  for (auto iter = partitions_.begin(); iter != partitions_.end(); iter++) {
    iter->second->Dump();
  }
  DLOG(INFO) << "--------------------------";
}


void Table::DoTimingTask() {
  slash::RWLock l(&partition_rw_, false);
  for (auto pair : partitions_) {
    //sleep(1);
    pair.second->AutoPurge();
  }
}
