#include "zp_data_table.h"

#include <sys/statvfs.h>
#include <glog/logging.h>

#include "zp_data_server.h"

extern ZPDataServer* zp_data_server;

std::shared_ptr<Table> NewTable(const std::string &table_name,
    const std::string log_path, const std::string data_path) {
  std::shared_ptr<Table> table(new Table(table_name, log_path, data_path));
  // TODO maybe need check
  return table;
}

//
// Table
//
Table::Table(const std::string& table_name, const std::string &log_path,
    const std::string &data_path)
  : table_name_(table_name),
  log_path_(log_path),
  data_path_(data_path),
  partition_cnt_(0) {
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
  pthread_rwlock_init(&partition_rw_, &attr);
}

Table::~Table() {
  pthread_rwlock_destroy(&partition_rw_);
  LOG(INFO) << " Table " << table_name_ << " exit!!!";
}

bool Table::SetPartitionCount(const int count) {
  partition_cnt_ = count;
  DLOG(INFO) << " Set Table: " << table_name_ << " with " << partition_cnt_ << " partitions.";
  return true;
}

std::shared_ptr<Partition> Table::GetPartition(const std::string &key) {
  slash::RWLock l(&partition_rw_, false);
  if (partition_cnt_ > 0) {
    int partition_id = std::hash<std::string>()(key) % partition_cnt_;
    auto it = partitions_.find(partition_id);
    if (it != partitions_.end()) {
      return it->second;
    }
  }
  return NULL;
}

std::shared_ptr<Partition> Table::GetPartitionById(const int partition_id) {
  slash::RWLock l(&partition_rw_, false);
  auto it = partitions_.find(partition_id);
  if (it != partitions_.end()) {
    return it->second;
  }
  return NULL;
}

bool Table::UpdateOrAddPartition(const int partition_id,
    ZPMeta::PState state, const Node& master, const std::set<Node>& slaves) {
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
  std::shared_ptr<Partition> partition = NewPartition(table_name_,
      log_path_, data_path_, partition_id, master, slaves);
  assert(partition != NULL);

  partition->Update(ZPMeta::PState::ACTIVE, master, slaves);
  partitions_[partition_id] = partition;

  return true;
}

void Table::LeaveAllPartition() {
  slash::RWLock l(&partition_rw_, false);
  for (auto iter : partitions_) {
    (iter.second)->Leave();
  }
}

uint32_t Table::KeyToPartition(const std::string &key) {
  assert(partition_cnt_ != 0);
  return std::hash<std::string>()(key) % partition_cnt_;
}

void Table::Dump() {
  slash::RWLock l(&partition_rw_, false);
  LOG(INFO) << "=========================";
  LOG(INFO) << "    Table : " << table_name_;
  for (auto iter = partitions_.begin(); iter != partitions_.end(); iter++) {
    iter->second->Dump();
  }
  LOG(INFO) << "--------------------------";
}


void Table::DoTimingTask() {
  slash::RWLock l(&partition_rw_, false);
  for (auto pair : partitions_) {
    pair.second->DoTimingTask();
  }
}

void Table::DumpPartitionBinlogOffsets(std::map<int, BinlogOffset> *offset) {
  slash::RWLock l(&partition_rw_, false);
  BinlogOffset tboffset;
  for (auto& pair : partitions_) {
    (pair.second)->GetBinlogOffsetWithLock(&(tboffset.filenum), &(tboffset.offset));
    offset->insert(std::pair<int, BinlogOffset>(pair.first, tboffset));
  }
}

uint64_t Df(const std::string& path) {
  struct statvfs sfs;
  if (statvfs(path.data(), &sfs) != -1) {
    return (uint64_t)(sfs.f_bsize * sfs.f_blocks);
  }
  return 0LL;
}

void Table::GetCapacity(Statistic *stat) {
  stat->Reset();
  stat->table_name = table_name_;
  stat->used_disk = slash::Du(data_path_) + slash::Du(log_path_);
  // TODO anan: we will support table based disk; 
  // For now, just use node's free disk instead.
  stat->free_disk = Df(data_path_);
  DLOG(INFO) << "GetCapacity for table " << table_name_ << ":";
  stat->Dump();
}

void Table::GetReplInfo(client::CmdResponse_InfoRepl* repl_info) {
  repl_info->set_table_name(table_name_);
  repl_info->set_partition_cnt(partition_cnt_);
  client::PartitionState* partition_state = NULL;
  for (auto& p : partitions_) {
    partition_state = repl_info->add_partition_state();
    p.second->GetState(partition_state);
  }
}


