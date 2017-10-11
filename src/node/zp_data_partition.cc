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
#include "src/node/zp_data_partition.h"

#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <glog/logging.h>

#include <fstream>
#include <utility>

#include "slash/include/rsync.h"
#include "src/node/zp_data_server.h"

extern ZPDataServer* zp_data_server;

struct DBSyncArg {
  Partition* p;
  std::string ip;
  int port;
  DBSyncArg(Partition* _p, const std::string& _ip, int _port)
    : p(_p), ip(_ip), port(_port) {}
};

struct PurgeArg {
  Partition* p;
  uint32_t to;
  bool manual;
};

Partition::Partition(const std::string& table_name, const int partition_id,
    const std::string& log_path, const std::string& data_path,
    const std::string& trash_path)
  : table_name_(table_name),
  partition_id_(partition_id),
  opened_(false),
  pstate_(ZPMeta::PState::ACTIVE),
  role_(Role::kNodeSingle),
  repl_state_(ReplState::kNoConnect),
  do_recovery_sync_(false),
  recover_sync_flag_(0),
  last_sync_time_(slash::NowMicros()),
  sync_lease_(kBinlogDefaultLease),
  stuck_recover_sync_flag_(0),
  purging_(false),
  purged_index_(0) {
    // Partition related path
    log_path_ = NewPartitionPath(log_path, partition_id_);
    data_path_ = NewPartitionPath(data_path, partition_id_);
    trash_path_ = NewPartitionPath(trash_path, partition_id_);
    slash::CreatePath(trash_path_);
    sync_path_ = NewPartitionPath(zp_data_server->db_sync_path()
        + table_name_ + "/", partition_id_);
    bgsave_path_ = NewPartitionPath(zp_data_server->bgsave_path()
        + table_name_ + "/", partition_id_);

    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr,
        PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

    pthread_rwlock_init(&state_rw_, &attr);
    pthread_rwlock_init(&suspend_rw_, &attr);
    pthread_rwlock_init(&purged_index_rw_, NULL);
    pthread_rwlock_init(&fallback_rw_, &attr);
  }

// Requeired: hold write lock of state_rw_
Status Partition::Open() {
  if (opened_) {
    return Status::OK();
  }

  // Check and update purged_index_
  if (!CheckBinlogFiles()) {
    // Binlog unavailable
    LOG(FATAL) << "CheckBinlogFiles failed. table: " << table_name_
      << ", partition_id: " << partition_id_;
    return Status::Corruption("Check binlog file failed!");
  }

  // Create db handle
  rocksdb::Status rs = rocksdb::DBNemo::Open(*(zp_data_server->db_options()),
      data_path_, &db_);
  if (!rs.ok()) {
    LOG(FATAL) << "DBNemo open failed. table: " << table_name_
      << ", partition_id: " << partition_id_ << ", error: " << rs.ToString();
    return Status::Corruption(rs.ToString());
  }

  // Binlog
  Status s = Binlog::Create(log_path_, kBinlogSize, &logger_);
  if (!s.ok()) {
    LOG(FATAL) << "Create binlog failed. table: " << table_name_
      << ", partition_id: " << partition_id_ << ", error: " << s.ToString();
    delete db_;
    return s;
  }

  opened_ = true;

  slash::RWLock l(&fallback_rw_, true);
  fallback_.time = 0;
  return s;
}

// Requeired: hold write lock of state_rw_
void Partition::Close() {
  if (!opened_) {
    return;
  }
  delete db_;
  delete logger_;

  opened_ = false;
}

// Requeired: hold write lock of state_rw_
void Partition::MoveToTrash() {
  if (opened_) {
    return;
  }

  // Move data and log to Trash
  std::string db_trash(trash_path_ + "db/"), log_trash(trash_path_ + "log/");
  slash::DeleteDirIfExist(db_trash);
  if (0 != slash::RenameFile(data_path_.c_str(),
        db_trash.c_str())) {
    LOG(WARNING) << "Failed to move db to trash, error: " << strerror(errno);
  }
  slash::DeleteDirIfExist(log_trash);
  if (0 != slash::RenameFile(log_path_.c_str(),
        log_trash.c_str())) {
    LOG(WARNING) << "Failed to move db to trash, error: " << strerror(errno);
  }
}

Partition::~Partition() {
  {
  slash::RWLock l(&state_rw_, true);
  Close();
  }
  pthread_rwlock_destroy(&fallback_rw_);
  pthread_rwlock_destroy(&purged_index_rw_);
  pthread_rwlock_destroy(&suspend_rw_);
  pthread_rwlock_destroy(&state_rw_);
  LOG(INFO) << " Partition " << table_name_ << "_"
    << partition_id_ << " exit!!!";
}

bool Partition::ShouldWaitDBSync() {
  slash::RWLock l(&state_rw_, false);
  return repl_state_ == ReplState::kWaitDBSync;
}

void Partition::SetWaitDBSync() {
  slash::RWLock l(&state_rw_, true);
  assert(ReplState::kShouldConnect == repl_state_);
  repl_state_ = ReplState::kWaitDBSync;
}

void Partition::WaitDBSyncDone() {
  slash::RWLock l(&state_rw_, true);
  assert(ReplState::kWaitDBSync == repl_state_);
  repl_state_ = ReplState::kShouldConnect;
  LOG(INFO) << "Partition: " << table_name_ << "_" << partition_id_
    << " WaitDBSyncDone  set repl_state: " << ReplStateMsg[repl_state_]
    << " Master Node:" << master_node_.ip << ":" << master_node_.port;
}

bool Partition::ShouldTrySync() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) << "Partition: " << table_name_ << "_" << partition_id_
    << " ShouldTrySync "
    << (repl_state_ == ReplState::kShouldConnect ? "true" : "false")
    <<  ", repl_state: " << ReplStateMsg[repl_state_];
  return repl_state_ == ReplState::kShouldConnect;
}

void Partition::TrySyncDone() {
  slash::RWLock l(&state_rw_, true);
  assert(ReplState::kShouldConnect == repl_state_);
  repl_state_ = ReplState::kConnected;
  LOG(INFO) << " Partition: " << table_name_ << "_" << partition_id_
    << " TrySyncDone  set repl_state: " << ReplStateMsg[repl_state_]
    <<", Master Node:" << master_node_.ip << ":" << master_node_.port;
}

// Required: hold write mutex of state_rw_ or write mutex of suspend_rw_
// as to block any other operation
Status Partition::ChangeDb(const std::string& new_path) {
  std::string tmp_path(trash_path_ + "obsolete");
  slash::DeleteDirIfExist(tmp_path);
  DLOG(INFO) << "Prepare change db from: " << tmp_path;
  delete db_;
  if (0 != slash::RenameFile(data_path_, tmp_path)) {
    LOG(FATAL) << "Failed to rename db path: " << data_path_
      << " to tmp path " << tmp_path << " when change db, table: "
      << table_name_ << "_" << partition_id_
      << ", error: " << strerror(errno);
    return Status::Corruption(strerror(errno));
  }

  if (0 != slash::RenameFile(new_path, data_path_)) {
    DLOG(INFO) << "Rename (" << new_path.c_str() << ", " << data_path_.c_str();
    LOG(FATAL) << "Failed to rename new db path: " << new_path
      << " to db path: " << data_path_ << " when change db, table: "
      << table_name_ << "_" << partition_id_
      << ", error: " << strerror(errno);
    return Status::Corruption(strerror(errno));
  }
  rocksdb::Status s = rocksdb::DBNemo::Open(*(zp_data_server->db_options()),
      data_path_, &db_);
  if (!s.ok()) {
    LOG(FATAL) << "Failed to open new db: " << data_path_
      << " when change db, table: "
      << table_name_ << "_" << partition_id_
      << ", error: " << strerror(errno);
    return Status::Corruption(s.ToString());
  }
  LOG(WARNING) << "Success to Changedb: " << data_path_
    << ", table: "<< table_name_ << "_" << partition_id_;
  return Status::OK();
}

// Required: hold read mutex of state_rw_
Status Partition::FlushDb() {
  std::string empty_path(trash_path_ + "null");
  slash::DeleteDirIfExist(empty_path);
  slash::CreatePath(empty_path);

  slash::RWLock l(&suspend_rw_, true);
  LOG(WARNING) << "FlushDb: "<< table_name_ << "_" << partition_id_;
  return ChangeDb(empty_path);
}

////// BGSave //// / /

// Prepare env
// Required: hold read mutex of state_rw_
bool Partition::InitBgsaveEnv() {
  slash::MutexLock l(&bgsave_protector_);
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S",
      localtime(&bgsave_info_.start_time));
  bgsave_info_.s_start_time.assign(s_time, len);
  bgsave_info_.path = bgsave_path_;
  if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(WARNING) << "Remove exist bgsave dir failed, Partition:"
      << table_name_ << "_" << partition_id_;
    return false;
  }
  slash::CreatePath(bgsave_info_.path, 0755);  // create parent directory
  bgsave_info_.path += std::string(s_time, 8);

  // Prepare for failed dir
  if (!slash::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
    LOG(WARNING) << "remove exist fail bgsave dir failed, Partition:"
      << table_name_ << "_" << partition_id_;
    return false;
  }

  return true;
}

// Prepare bgsave env
// Required: hold read mutex of state_rw_
bool Partition::InitBgsaveContent(rocksdb::DBNemoCheckpoint* cp,
    CheckpointContent* content) {
  rocksdb::Status s;
  {
    slash::RWLock l(&suspend_rw_, true);
    {
      slash::MutexLock l(&bgsave_protector_);
      logger_->GetProducerStatus(&bgsave_info_.filenum, &bgsave_info_.offset);
    }
    s = cp->GetCheckpointFiles(content->live_files,
        content->live_wal_files,
        content->manifest_file_size,
        content->sequence_number);
    if (!s.ok()) {
      LOG(WARNING) << "Set backup content failed " << s.ToString()
        << ", Partition" << table_name_ << "_" << partition_id_;
      return false;
    }
  }
  return true;
}

bool Partition::RunBgsave() {
  // Create new checkpoint
  CheckpointContent content;
  rocksdb::DBNemoCheckpoint* cp;

  slash::RWLock l(&state_rw_, false);
  if (!opened_) {
    LOG(WARNING) << "db already closed when try to do bgsave"
      << ", Table:" << table_name_ << ", Partition:" << partition_id_;
    return false;
  }

  rocksdb::Status s = rocksdb::DBNemoCheckpoint::Create(db_, &cp);
  if (!s.ok()) {
    LOG(WARNING) << "Create DBNemoCheckpoint failed :" << s.ToString()
      << ", Table:" << table_name_ << ", Partition:" << partition_id_;
    return false;
  }

  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveContent(cp, &content)) {
    delete cp;
    return false;
  }

  BGSaveInfo info = bgsave_info();
  DLOG(INFO) << "   bgsave_info: path=" << info.path
    << ",  filenum=" << info.filenum
    << ", offset=" << info.offset;

  // Backup to tmp dir
  s = cp->CreateCheckpointWithFiles(info.path,
      content.live_files,
      content.live_wal_files,
      content.manifest_file_size,
      content.sequence_number);
  LOG(INFO) << "Create new backup finished, path is " << info.path
      << ", Table:" << table_name_ << ", Partition:" << partition_id_;

  delete cp;
  if (!s.ok()) {
    LOG(WARNING) << "Backup failed, Error:" << s.ToString()
      << ", Table:" << table_name_ << ", Partition:" << partition_id_;
    return false;
  }
  return true;
}

void Partition::Bgsave() {
  // Only one thread can go through
  {
    slash::MutexLock l(&bgsave_protector_);
    if (bgsave_info_.bgsaving) {
      DLOG(INFO) << "Already bgsaving, cancel it";
      return;
    }
    bgsave_info_.bgsaving = true;
  }

  zp_data_server->BGSaveTaskSchedule(&DoBgsave, static_cast<void*>(this));
}

void Partition::DoBgsave(void* arg) {
  Partition* p = static_cast<Partition*>(arg);

  // Do bgsave
  bool ok = p->RunBgsave();

  BGSaveInfo info = p->bgsave_info();
  DLOG(INFO) << "DoBGSave info file " << zp_data_server->local_ip()
    << ":" << zp_data_server->local_port() << " filenum=" << info.filenum
    << " offset=" << info.offset;
  // Some output
  std::ofstream out;
  out.open(info.path + "/info", std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(NULL) - info.start_time) << "s\n"
      << zp_data_server->local_ip() << "\n"
      << zp_data_server->local_port() << "\n"
      << info.filenum << "\n"
      << info.offset << "\n";
    out.close();
  }
  if (!ok) {
    std::string fail_path = info.path + "_FAILED";
    slash::RenameFile(info.path.c_str(), fail_path.c_str());
    slash::MutexLock l(&p->bgsave_protector_);
    p->bgsave_info_.Clear();
  }

  slash::MutexLock l(&p->bgsave_protector_);
  p->bgsave_info_.bgsaving = false;
}

bool Partition::TryUpdateMasterOffset() {
  // Check dbsync finished
  std::string info_path = sync_path_ + kBgsaveInfoFile;
  if (!slash::FileExists(info_path)) {
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Failed to open info file after db sync, table: "
      << table_name_ << " Partition:" << partition_id_
      << " info_path:" << info_path;
    return false;
  }
  std::string line, master_ip;
  int lineno = 0;
  int64_t filenum = 0, offset = 0, tmp = 0, master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 6) {
      if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
        LOG(WARNING) << "Format of info file after db sync error, Partition:"
          << table_name_ << "_" << partition_id_ << ", line : " << line;
        is.close();
        return false;
      }
      if (lineno == 3) {
        master_port = tmp;
      } else if (lineno == 4) {
        filenum = tmp;
      } else {
        offset = tmp;
      }

    } else if (lineno > 5) {
      LOG(WARNING) << "Format of info file after db sync error, Partition:"
        << table_name_ << "_" << partition_id_ << " line : " << line;
      is.close();
      return false;
    }
  }
  is.close();
  DLOG(INFO) << " info_path is " << info_path
    << ", sync_path_ is " << sync_path_;
  LOG(INFO) << "Information from dbsync info. Paritition: "
    << table_name_ << "_" << partition_id_
    << ", master_ip: " << master_ip
    << ", master_port: " << master_port
    << ", filenum: " << filenum
    << ", offset: " << offset;

  // Sanity check
  slash::RWLock l(&state_rw_, true);
  if (!opened_) {
    LOG(WARNING) << "Partition is not opened"
      << ", Partition:" << table_name_ << "_" << partition_id_;
    return false;
  }

  if (master_ip != master_node_.ip || master_port != master_node_.port) {
    LOG(WARNING) << "Error master : " << master_ip << ":" << master_port
      << ". current master : " << master_node_.ip << ":" << master_node_.port
      << ", Partition:" << table_name_ << "_" << partition_id_;
    return false;
  }

  slash::DeleteFile(info_path);
  LOG(WARNING) << "Change db after db sync for " << table_name_
    << "_" << partition_id_;
  if (!ChangeDb(sync_path_).ok()) {
    return false;
  }

  // Update master offset
  SetBinlogOffset(BinlogOffset(filenum, offset));
  return true;
}

// Try to be master of node
// Return EndFile when the sync offset is larger than current one
// Return InvalidArgument when the offset is invalid
// Return Incomplete when neet sync db
// Required: state_rw hold and partition opened
Status Partition::SlaveAskSync(const Node &node, BinlogOffset boffset) {
  // Check role
  if (role_ != Role::kNodeMaster
      || slave_nodes_.find(node) == slave_nodes_.end()) {
    LOG(WARNING) << "I'm not the master for :" << node
      << ", table: " << table_name_
      << ", partition: " << partition_id_;
    return Status::Corruption("Current node is not the master");
  }

  // Sanity check
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (cur_filenum < boffset.filenum
      || (cur_filenum == boffset.filenum && cur_offset < boffset.offset)) {
    return Status::EndFile("AddBinlogSender invalid binlog offset");
  }

  // Binlog already be purged
  slash::RWLock lp(&purged_index_rw_, false);
  LOG(INFO) << "Partition:" << table_name_ << "_" << partition_id_
    << ", We " << (purged_index_ > boffset.filenum ? "will" : "won't")
    << " TryDBSync, purged_index_=" << purged_index_
    << ", filenum=" << boffset.filenum;
  if (purged_index_ > boffset.filenum) {
    TryDBSync(node.ip, node.port + kPortShiftRsync, cur_filenum);
    return Status::Incomplete("Bgsaving and DBSync first");
  }

  // Add binlog send task
  Status s = zp_data_server->AddBinlogSendTask(table_name_, partition_id_,
      logger_->filename(), Node(node.ip, node.port + kPortShiftSync),
      boffset.filenum, boffset.offset);
  if (s.ok()) {
    LOG(INFO) << "Success AddBinlogSendTask for Table " << table_name_
      << " Partition " << partition_id_ << " To "
      << node.ip << ":" << node.port << " at "
      << boffset.filenum << ", " << boffset.offset;
  } else if (s.IsInvalidArgument()) {
    // Invalid filenum and offset
    LOG(INFO) << "Failed AddBinlogSendTask for Table " << table_name_
      << " Partition " << partition_id_ << " To " << node.ip << ":" << node.port
      << " Since the Invalid Offset : " << boffset.filenum
      << ", " << boffset.offset;
  } else {
    LOG(WARNING) << "Failed AddBinlogSendTask for Table " << table_name_
      << " Partition " << partition_id_ << " To " << node.ip << ":" << node.port
      << " at " << boffset.filenum << ", " << boffset.offset
      << ", Error:" << s.ToString() << ", cur filenum:" << cur_filenum
      << ", cur offset" << cur_offset;
  }
  return s;
}

// Requeired: hold write lock of state_rw_
void Partition::CleanSlaves(const std::set<Node> &old_slaves) {
  for (auto& old : old_slaves) {
    LOG(INFO) << "Delete BinlogSendTask for Table " << table_name_
      << " Partition " << partition_id_ << " To "
      << old.ip << ":" << old.port + kPortShiftSync;
    zp_data_server->RemoveBinlogSendTask(table_name_,
        partition_id_, Node(old.ip, old.port + kPortShiftSync));
  }
}

// Requeired: hold write lock of state_rw_
void Partition::BecomeSingle() {
  LOG(INFO) << "Table: " << table_name_
    << ", Partition " << partition_id_ << " BecomeSingle";
  role_ = Role::kNodeSingle;
  repl_state_ = ReplState::kNoConnect;
  Close();
  if (g_zp_conf->enable_data_delete()) {
    LOG(INFO) << "Move all data to trash, Table: " << table_name_
      << ", Partition " << partition_id_;
    MoveToTrash();
  }
}

// Requeired: hold write lock of state_rw_
void Partition::BecomeMaster() {
  Open();
  LOG(INFO) << "Table: " << table_name_
    << ", Partition " << partition_id_ << " BecomeMaster";
  role_ = Role::kNodeMaster;
  repl_state_ = ReplState::kNoConnect;

  // Record binlog offset when I win the master for the later slave sync
  GetBinlogOffset(&win_boffset_);
}

// Requeired: hold write lock of state_rw_
void Partition::BecomeSlave() {
  Open();
  LOG(INFO) << "Table: " << table_name_
    << ", Partition " << partition_id_
    << " BecomeSlave, master is " << master_node_.ip
    << ":" << master_node_.port;
  role_ = Role::kNodeSlave;
  repl_state_ = ReplState::kShouldConnect;

  zp_data_server->AddSyncTask(table_name_, partition_id_);

  // Reset sync related status;
  last_sync_time_ = slash::NowMicros();
  sync_lease_ = kBinlogDefaultLease;
  ResetRecoverSync();
  stuck_recover_sync_flag_ = 0;
}

// Get binlog offset when I win the election
// Return false if I'm not a master
bool Partition::GetWinBinlogOffset(BinlogOffset* win) {
  slash::RWLock l(&state_rw_, false);
  if (role_ != Role::kNodeMaster) {
    return false;
  }
  *win = win_boffset_;
  return true;
}

void Partition::Update(ZPMeta::PState state, const Node &master,
    const std::set<Node> &slaves) {
  slash::RWLock l(&state_rw_, true);

  // Update partition state
  pstate_ = state;

  // Update master slave nodes
  bool change_master = false;
  std::set<Node> miss_slaves;

  if (master_node_ != master) {
    master_node_ = master;
    change_master = true;
  }
  if (slave_nodes_ != slaves) {
    miss_slaves = slave_nodes_;
    slave_nodes_.clear();
    for (auto& slave : slaves) {
      slave_nodes_.insert(slave);
      // Remove those who also exist in the new slaves
      // And what's remain in miss_slaves is those
      // who will not be slave any more
      miss_slaves.erase(slave);
    }
  }

  // Determine new role
  Role new_role = Role::kNodeSingle;
  if (zp_data_server->IsSelf(master)) {
    new_role = Role::kNodeMaster;
  }
  for (auto slave : slaves) {
    if (zp_data_server->IsSelf(slave)) {
      new_role = Role::kNodeSlave;
      break;
    }
  }

  // Clean Slaves
  if (role_ == Role::kNodeMaster) {
    CleanSlaves(miss_slaves);
    if (new_role != Role::kNodeMaster) {
      // Clean all others also
      CleanSlaves(slave_nodes_);
    }
  }

  // Update role
  if (role_ != new_role) {
    // Change role
    if (new_role == Role::kNodeMaster) {
      BecomeMaster();
    } else if (new_role == Role::kNodeSlave) {
      BecomeSlave();
    } else {
      BecomeSingle();
    }
  } else if (change_master && role_ == Role::kNodeSlave) {
    // Change master
    BecomeSlave();
  }
}

void Partition::Leave() {
  slash::RWLock l(&state_rw_, true);
  slave_nodes_.erase(Node(zp_data_server->local_ip(),
        zp_data_server->local_port()));
  if (role_ == Role::kNodeMaster) {
    CleanSlaves(slave_nodes_);
  }
  BecomeSingle();
}

std::string NewPartitionPath(const std::string& name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s/%u/", name.c_str(), current);
  return std::string(buf);
}

std::shared_ptr<Partition> NewPartition(const std::string &table_name,
    const std::string& log_path, const std::string& data_path,
    const std::string& trash_path,  const int partition_id,
    const Node& master, const std::set<Node> &slaves) {
  std::shared_ptr<Partition> partition(new Partition(table_name,
      partition_id, log_path, data_path, trash_path));
  return partition;
}

Status Partition::SetBinlogOffsetWithLock(const BinlogOffset& target) {
  slash::RWLock l(&state_rw_, false);
  return SetBinlogOffset(target);
}

// Required: hold read mutex of state_rw_
Status Partition::SetBinlogOffset(const BinlogOffset& target) {
  uint64_t actual_offset = 0;
  BinlogOffset old;
  Status s = logger_->SetProducerStatus(target.filenum, target.offset,
      &actual_offset, &old.filenum, &old.offset);
  if (target.offset != actual_offset) {
    LOG(WARNING) << "SetBinlogOffset actual_offset small than expected"
      << ", expect:" << target.offset << ", actual_offset:" << actual_offset;
  }

  if (target < old) {
    // Fallback to a smaller sync point, record for later checking
    LOG(WARNING) << "SetBinlogOffset fallback to a smaller sync point"
      << ", from:" << old.filenum << "_" << old.offset
      << ", to:" << target.filenum << "_" << target.offset;
    slash::RWLock l(&fallback_rw_, true);
    fallback_.time = slash::NowMicros();
    fallback_.before = old;
    fallback_.after = target;
  }
  return s;
}

bool Partition::GetBinlogOffsetWithLock(BinlogOffset* boffset) {
  slash::RWLock l(&state_rw_, false);
  return GetBinlogOffset(boffset);
}

// Required: hold read mutex of state_rw_
bool Partition::GetBinlogOffset(BinlogOffset* boffset) const {
  if (!opened_) {
    return false;
  }
  logger_->GetProducerStatus(&boffset->filenum, &boffset->offset);
  return true;
}

// Required: hold read mutex of state_rw_
bool Partition::CheckSyncOption(const PartitionSyncOption& option,
    bool has_offset) {
  // Check from node
  if (option.from_node != slash::IpPortString(master_node_.ip,
        master_node_.port)) {
    LOG(WARNING) << "Discard binlog item from " << option.from_node
      << ", partition:" << partition_id_
      << ", current my master is " << master_node_;
    return false;
  }

  // Check current status
  if (!opened_
      || role_ != Role::kNodeSlave
      || repl_state_ != ReplState::kConnected) {
    LOG(WARNING) << "Discard binlog item from " << option.from_node
      << ", is opened:" << opened_
      << ", partition:" << partition_id_
      << ", my current role: " << static_cast<int>(role_)
      << ", my current connection state: " << static_cast<int>(repl_state_);
    return false;
  }

  // Update last sync_time
  last_sync_time_ = slash::NowMicros();

  // Check offset
  if (!has_offset) {
    return true;
  }
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (option.filenum != cur_filenum || option.offset != cur_offset) {
    DLOG(WARNING) << "Discard binlog item from " << option.from_node
      << ", table:" << table_name_
      << ", partition:" << partition_id_
      << ", with offset (" << option.filenum << ", " << option.offset << ")"
      << ", my current offset: (" << cur_filenum << ", " << cur_offset << ")";
    if (option.filenum > cur_filenum ||
        (option.filenum == cur_filenum && option.offset > cur_offset)) {
      // Under this circumstance, slave has no chance to recovery itself
      // So Try to schedule a new trysync bg job
      TryRecoverSync();
    }
    return false;
  }
  ResetRecoverSync();
  return true;
}

// Keep binlog order outside
void Partition::DoBinlogCommand(const PartitionSyncOption& option,
    const Cmd* cmd, const client::CmdRequest &req) {
  slash::RWLock l(&state_rw_, false);
  if (!CheckSyncOption(option)) {
    return;
  }

  uint64_t start_us = 0;
  if (g_zp_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  // Add read lock for no suspend command
  if (!cmd->is_suspend()) {
    pthread_rwlock_rdlock(&suspend_rw_);
  }

  client::CmdResponse res;
  cmd->Do(&req, &res, this);

  std::string raw;
  req.SerializeToString(&raw);
  Status s = logger_->Put(raw);
  if (!s.ok()) {
    LOG(WARNING) << "Binlog Put failed : " << s.ToString()
      << ", table: " << table_name_
      << ", partition: " << partition_id_
      << ", content: [" << raw << "]";
  }

  if (!cmd->is_suspend()) {
    pthread_rwlock_unlock(&suspend_rw_);
  }

  if (g_zp_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_zp_conf->slowlog_slower_than()) {
      LOG(WARNING) << "slow sync command:" << cmd->name()
        << ", duration(us): " << duration
        << ", For " << table_name_ << "_" << partition_id_;
    }
  }
}

// Keep binlog order outside
void Partition::DoBinlogSkip(const PartitionSyncOption& option,
    uint64_t gap) {
  slash::RWLock l(&state_rw_, false);
  if (!CheckSyncOption(option)) {
    return;
  }

  Status s = logger_->PutBlank(gap);
  if (!s.ok()) {
    LOG(WARNING) << "Binlog PutBlank failed : " << s.ToString()
      << ", table: " << table_name_
      << ", partition: " << partition_id_
      << ", gap: " << gap;
  }
}

void Partition::DoBinlogLeaseRenew(const PartitionSyncOption& option,
    uint64_t lease) {
  slash::RWLock l(&state_rw_, false);
  if (!CheckSyncOption(option, false)) {
    return;
  }
  sync_lease_ = lease;
}

void Partition::DoCommand(const Cmd* cmd, const client::CmdRequest &req,
    client::CmdResponse *res) {
  std::string key = cmd->ExtractKey(&req);

  slash::RWLock l(&state_rw_, false);
  if (!opened_
      || role_ != Role::kNodeMaster) {
    res->set_type(req.type());
    res->set_code(client::StatusCode::kMove);
    res->set_msg("Command Redirect");

    client::Node* node = res->mutable_redirect();
    node->set_ip(master_node_.ip);
    node->set_port(master_node_.port);

    DLOG(WARNING) << "Should redirect, failed to DoCommand  at table: "
      << table_name_ << ", Partition: " << partition_id_
      << " Role:" << RoleMsg[role_] << " redirect to master:" << node;
    return;
  }

  if (cmd->is_write() && pstate_ == ZPMeta::PState::STUCK) {
    res->set_type(req.type());
    res->set_code(client::StatusCode::kWait);
    res->set_msg("partition stucked");

    DLOG(WARNING) << "Partition Stuck, failed to DoCommand  at table: "
      << table_name_ << ", Partition: " << partition_id_
      << " Role:" << RoleMsg[role_] << " ParititionState:"
      << static_cast<int>(pstate_);
    return;
  }

  uint64_t start_us = 0;
  if (g_zp_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  // Add read lock for no suspend command
  if (!cmd->is_suspend()) {
    pthread_rwlock_rdlock(&suspend_rw_);
  }

  if (cmd->is_write()) {
    mutex_record_.Lock(key);
  }

  cmd->Do(&req, res, this);

  if (cmd->is_write()) {
    if (res->code() == client::StatusCode::kOk) {
      // Restore Message
      std::string raw;
      if (cmd->GenerateLog(&req, &raw)) {
        logger_->Put(raw);
      }
    }
    mutex_record_.Unlock(key);
  }

  if (!cmd->is_suspend()) {
    pthread_rwlock_unlock(&suspend_rw_);
  }

  if (g_zp_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_zp_conf->slowlog_slower_than()) {
      LOG(WARNING) << "slow client command:" << cmd->name()
        << ", duration(us): " << duration
        << ", For " << table_name_ << "_" << partition_id_;
    }
  }
}

inline void Partition::TryRecoverSync() {
  do_recovery_sync_ = true;
}

inline void Partition::ResetRecoverSync() {
  // no mutex protect this two together
  // since we can tolerant the inconsistence
  do_recovery_sync_ = false;
  recover_sync_flag_ = 0;
}

// Required: hold write mutex of state_rw_
bool Partition::NeedRecoverSync() {
  if (role_ != Role::kNodeSlave) {
    return false;
  }

  // Recover trigger by stucking out of kConnected state
  if (repl_state_ != ReplState::kConnected) {
    // repl state mybe one of kNoConnect, kShouldConnect or kWaitDBSync
    stuck_recover_sync_flag_++;
    if (stuck_recover_sync_flag_ > kStuckRecoverSyncDelayCronCount) {
      LOG(INFO) << "Slave stuck out of kConnected for more than "
        << kStuckRecoverSyncDelayCronCount << " cron times, would redo trysync."
        << " table: " << table_name_ << ", partition_id: " << partition_id_;
      return true;
    }
    return false;
  }

  // kConnected
  // Recover trigger by serially error binlog
  if (do_recovery_sync_) {
    recover_sync_flag_++;
    if (recover_sync_flag_ > kRecoverSyncDelayCronCount) {
      // will be reset when BecomeSlave
      LOG(INFO) << "Serially error binlog received, would redo trysync."
        << " table: " << table_name_ << ", partition_id: " << partition_id_;
      return true;
    }
  }

  // Recover trigger by lease timeout
  uint64_t now_time = slash::NowMicros();
  if (now_time - last_sync_time_ > sync_lease_ * 1000 * 1000) {
    // We know last_sync_time_ and sync_lease is not atomic here,
    // but it's not critical, there will be one more trysync at worse
    LOG(INFO) << "Sync lease timeout, lease:" << sync_lease_
      << ", now: " << now_time << ", last sync: " << last_sync_time_
      << ", would redo trysync" << " table: " << table_name_
      << ", partition_id: " << partition_id_;
    return true;
  }
  return false;
}

void Partition::DoTimingTask() {
  // Purge log
  if (!PurgeLogs(0, false)) {
    return;
  }

  // Maybe trysync
  slash::RWLock l(&state_rw_, true);
  if (NeedRecoverSync()) {
    BecomeSlave();
  }
}

// Required: hold read mutex of state_rw_
void Partition::TryDBSync(const std::string& ip, int port, int32_t top) {
  std::string bg_path;
  uint32_t bg_filenum = 0;
  {
    slash::MutexLock l(&bgsave_protector_);
    bg_path = bgsave_info_.path;
    bg_filenum = bgsave_info_.filenum;
  }

  if (0 != slash::IsDir(bg_path) ||  // Bgsaving dir exist
      !slash::FileExists(NewFileName(logger_->filename(), bg_filenum)) ||
                                     // filenum can be found in binglog
      top - bg_filenum > kDBSyncMaxGap) {  // The file is not too old
    // Need Bgsave first
    Bgsave();
  }

  DBSync(ip, port);
}

// Required: hold read mutex of state_rw_
void Partition::DBSync(const std::string& ip, int port) {
  // Only one DBSync task for every ip_port
  std::string ip_port = slash::IpPortString(ip, port);
  {
    slash::MutexLock l(&db_sync_protector_);
    if (db_sync_slaves_.find(ip_port) != db_sync_slaves_.end()) {
      LOG(INFO) << " DBSync with (" << ip_port << ") already in schedule"
        << ", Parition " << table_name_ << "_" << partition_id_;
      return;
    }
    db_sync_slaves_.insert(ip_port);
  }

  LOG(INFO) << " DBSync add new SyncTask for (" << ip_port
    << "), Parition " << table_name_ << "_" << partition_id_;
  // Reuse the bg_thread for Bgsave
  // Since we expect Bgsave and DBSync execute serially
  DBSyncArg *arg = new DBSyncArg(this, ip, port);
  zp_data_server->BGSaveTaskSchedule(&DoDBSync, static_cast<void*>(arg));
}

void Partition::DoDBSync(void* arg) {
  DBSyncArg *psync = static_cast<DBSyncArg*>(arg);
  Partition* partition = psync->p;

  partition->DBSyncSendFile(psync->ip, psync->port);
  
  // remove slave
  {
    slash::MutexLock l(&(partition->db_sync_protector_));
    partition->db_sync_slaves_.erase(slash::IpPortString(psync->ip, psync->port));
  }

  delete psync;
}

void Partition::DBSyncSendFile(const std::string& ip, int port) {
  std::string bg_path;
  {
    slash::RWLock l(&state_rw_, false);
    if (!opened_) {
      LOG(WARNING) << "Partition has been closed when try to dbsync"
        << ", Table:" << table_name_ << ", Partition: "<< partition_id_;
      return;
    }    
    {    
      slash::MutexLock l(&bgsave_protector_);
      bg_path = bgsave_info_.path;
    }    
  }

  // Get all files need to send
  std::vector<std::string> descendant;
  if (!slash::GetDescendant(bg_path, descendant)) {
    LOG(WARNING) << "Get Descendant when try to do db sync failed";
    return;
  }

  LOG(INFO) << "Begin to Send DBSync file, file size: " << descendant.size()
    << ", to node: " << ip << ":" << port << ", bg_path: " << bg_path
    << ", Partition: " << table_name_<< "_" << partition_id_;

  // Iterate to send files
  int ret = 0;
  std::string target_path;
  std::string target_dir = NewPartitionPath(table_name_ + "/.", partition_id_);
  std::string module = kDBSyncModule;
  std::vector<std::string>::iterator it = descendant.begin();
  slash::RsyncRemote remote(ip, port, module, kDBSyncSpeedLimit * 1024);
  for (; it != descendant.end(); ++it) {
    target_path = (*it).substr(bg_path.size() + 1);
    DLOG(INFO) << "--- descendant: " << target_path;
    if (target_path == kBgsaveInfoFile) {
      continue;
    }
    // We need specify the speed limit for every single file
    int retry_count = kDBSyncRetryTime;
    do {
      ret = slash::RsyncSendFile(*it, target_dir + target_path, remote);
    } while (0 != ret && retry_count--);

    if (0 != ret) {
      LOG(WARNING) << "rsync send file failed after retry: " << ret
        << ", to node: " << ip << ":" << port << ", bg_path: " << bg_path
        << ", Partition: " << table_name_<< "_" << partition_id_;
      break;
    }
    if (!opened_) {
      LOG(WARNING) << "Partition has been closed when try to send dbsync"
        << ", to node: " << ip << ":" << port << ", bg_path: " << bg_path
        << ", Partition: " << table_name_<< "_" << partition_id_;
      return;  // Terminate as soon as possible
    }
  }

  // Clear target path
  slash::RsyncSendClearTarget(bg_path, target_dir, remote);

  // Send info file at last
  if (0 == ret) {
    if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile,
            target_dir + kBgsaveInfoFile, remote))) {
      LOG(WARNING) << "send info file failed: " << ret
        << ", to node: " << ip << ":" << port << ", bg_path: " << bg_path
        << ", Partition: " << table_name_<< "_" << partition_id_;
    }
  }

  if (0 == ret) {
    LOG(INFO) << "rsync send files success" 
      << ", to node: " << ip << ":" << port << ", bg_path: " << bg_path
      << ", Partition: " << table_name_<< "_" << partition_id_;
  }
}

bool Partition::PurgeLogs(uint32_t to, bool manual) {
  // Only one thread can go through
  bool expect = false;
  if (!purging_.compare_exchange_strong(expect, true)) {
    LOG(WARNING) << "purge process already exist"
      << ", table:" << table_name_ << ", partition:" << partition_id_;
    return false;
  }

  slash::RWLock l(&state_rw_, false);
  if (!opened_) {
    purging_ = false;
    return true;
  }
  PurgeArg *arg = new PurgeArg();
  arg->p = this;
  arg->to = to;
  arg->manual = manual;
  zp_data_server->BGPurgeTaskSchedule(&DoPurgeLogs, static_cast<void*>(arg));
  return true;
}

void Partition::DoPurgeLogs(void* arg) {
  PurgeArg *ppurge = static_cast<PurgeArg*>(arg);
  Partition* ps = ppurge->p;

  ps->PurgeFiles(ppurge->to, ppurge->manual);

  ps->purging_ = false;
  delete ppurge;
}

bool Partition::PurgeFiles(uint32_t to, bool manual) {
  slash::RWLock l(&state_rw_, false);
  if (!opened_) {
    return true;
  }

  std::map<uint32_t, std::string> binlogs;
  if (!GetBinlogFiles(&binlogs)) {
    LOG(WARNING) << "Could not get binlog files!"
      << ", Partition: " << table_name_ << "_" << partition_id_; 
    return false;
  }

  if (binlogs.size() <= kBinlogRemainMinCount) {
    // No need purge
    return true;
  }

  int delete_num = 0;
  struct stat file_stat;
  int remain_expire_num = binlogs.size() - kBinlogRemainMaxCount;
  std::map<uint32_t, std::string>::iterator it;

  for (it = binlogs.begin(); it != binlogs.end(); ++it) {
    if ((manual && it->first <= to) ||           // Argument bound
        remain_expire_num > 0 ||                 // Expire num trigger
        (stat(((log_path_ + it->second)).c_str(), &file_stat) == 0 &&
         file_stat.st_mtime < time(NULL) - kBinlogRemainMaxDay*24*3600)) {  // Expire time trigger
      // We check this every time to avoid lock when we do file deletion
      if (!CouldPurge(it->first)) {
        return false;
      }

      // Do delete
      slash::Status s = slash::DeleteFile(log_path_ + it->second);
      if (s.ok()) {
        ++delete_num;
        --remain_expire_num;
      } else {
        LOG(WARNING) << "Purge log file : " << (it->second)
          << " failed! error:" << s.ToString()
          << ", Partition: " << table_name_ << "_" << partition_id_; 
      }
    } else {
      // Break when face the first one not satisfied
      // Since the binlogs is order by the file index
      break;
    }
  }
  if (delete_num) {
    LOG(INFO) << "Success purge "<< delete_num << " for "
      << table_name_ << "_" << partition_id_;
  }

  return true;
}

// Required hold read lock of state_rw_ and  partition opened
bool Partition::CheckBinlogFiles() {
  if (!slash::FileExists(log_path_)) {
    return true;
  }

  std::vector<std::string> children;
  int ret = slash::GetChildren(log_path_, children);
  if (ret != 0) {
    LOG(WARNING) << "CheckBinlogFiles Get all files in log path failed! Partition:"
      << table_name_ << "_" << partition_id_ << " Error:" << ret;
    return false;
  }

  int64_t index = 0;
  std::string sindex;
  std::set<uint32_t> binlog_nums;
  std::vector<std::string>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kBinlogPrefixLen);
    if (slash::string2l(sindex.c_str(), sindex.size(), &index) == 1) {
      binlog_nums.insert(index);
    }
  }
  std::set<uint32_t>::iterator num_it = binlog_nums.begin(),
    pre_num_it = binlog_nums.begin();
  {
    slash::RWLock lp(&purged_index_rw_, true);
    purged_index_ = *num_it++;  // update the purged_index_
  }
  LOG(INFO) << "Update purged index to " << purged_index_
    << ", Partition: " << table_name_ << "_" << partition_id_;
  for (; num_it != binlog_nums.end(); ++num_it, ++pre_num_it) {
    if (*num_it != *pre_num_it + 1) {
      LOG(ERROR) << " There is a hole among the binglogs between "
        <<  *num_it << " and "  << *pre_num_it
        << ", Partiton : " << table_name_ << "_" << partition_id_;
      // there is a hole
      return false;
    }
  }
  return true;
}

// Required: hold read lock of state_rw_ and partition opened
bool Partition::GetBinlogFiles(std::map<uint32_t, std::string>* binlogs) {
  std::vector<std::string> children;
  int ret = slash::GetChildren(log_path_, children);
  if (ret != 0) {
    LOG(WARNING) << "GetBinlogFiles Get all files in log path failed:" << ret
      << ", Partition: " << table_name_ << "_" << partition_id_;
    return false;
  }

  int64_t index = 0;
  std::string sindex;
  std::vector<std::string>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kBinlogPrefixLen);
    if (slash::string2l(sindex.c_str(), sindex.size(), &index) == 1) {
      binlogs->insert(std::pair<uint32_t, std::string>(
            static_cast<uint32_t>(index), *it));
    }
  }
  return true;
}

// Required: hold read lock of state_rw_ and partition opened
bool Partition::CouldPurge(uint32_t index) {
  uint32_t pro_num;
  uint64_t tmp;
  logger_->GetProducerStatus(&pro_num, &tmp);
  if (index >= pro_num) {
    return false;
  }

  std::set<Node>::iterator it;
  slash::RWLock lp(&purged_index_rw_, true);
  for (it = slave_nodes_.begin(); it != slave_nodes_.end(); ++it) {
    int32_t filenum = zp_data_server->GetBinlogSendFilenum(table_name_,
        partition_id_, Node((*it).ip, (*it).port + kPortShiftSync));
    if (filenum < 0 || index >= static_cast<uint32_t>(filenum)) {
      return false;
    }
  }
  purged_index_ = index + 1;
  return true;
}

void Partition::Dump() {
  slash::RWLock l(&state_rw_, false);
  LOG(INFO) << "----------------------------";
  LOG(INFO) << "  +Partition    " << partition_id_;
  switch (role_) {
    case Role::kNodeMaster:
      LOG(INFO) << "  +I'm master";
      break;
    case Role::kNodeSlave:
      LOG(INFO) << "  +I'm slave";
      break;
    default:
      LOG(INFO) << "  +I'm single";
  }
  if (pstate_ == ZPMeta::PState::ACTIVE) {
    LOG(INFO) << "     -*State ACTIVE";
  } else if (pstate_ == ZPMeta::PState::STUCK) {
    LOG(INFO) << "     -*State STUCK";
  }
  LOG(INFO) << "     -*Master node " << master_node_;
  for (auto& slave : slave_nodes_) {
    LOG(INFO) << "     -* slave  " <<  slave;
  }
}

bool Partition::GetState(client::PartitionState* state) {
  state->set_partition_id(partition_id_);
  slash::RWLock l(&state_rw_, false);
  if (!opened_) {
    return false;
  }
  state->set_role(RoleMsg[role_]);
  state->set_repl_state(ReplStateMsg[repl_state_]);
  state->mutable_master()->set_ip(master_node_.ip);
  state->mutable_master()->set_port(master_node_.port);
  for (auto& s : slave_nodes_) {
    client::Node* slave = state->add_slaves();
    slave->set_ip(s.ip);
    slave->set_port(s.port);
  }

  // SyncOffset
  client::SyncOffset* sync_offset = state->mutable_sync_offset();
  BinlogOffset boffset;
  GetBinlogOffset(&boffset);
  sync_offset->set_filenum(boffset.filenum);
  sync_offset->set_offset(boffset.offset);

  // Fallback
  if (role_ == Role::kNodeSlave) {
    slash::RWLock l(&fallback_rw_, false);
    if (fallback_.time == 0) {
      // No fallback
      return true;
    }
    client::SlaveFallback* fallback = state->mutable_fallback();
    fallback->set_time(fallback_.time);
    client::SyncOffset* before = fallback->mutable_before();
    before->set_filenum(fallback_.before.filenum);
    before->set_offset(fallback_.before.offset);
    client::SyncOffset* after = fallback->mutable_after();
    after->set_filenum(fallback_.after.filenum);
    after->set_offset(fallback_.after.offset);
  }
  return true;
}

