#include "zp_data_partition.h"

#include <vector>
#include <fstream>
#include <glog/logging.h>
#include "zp_data_server.h"
#include "rsync.h"

extern ZPDataServer* zp_data_server;

Partition::Partition(const std::string &table_name, const int partition_id, const std::string &log_path, const std::string &data_path)
  : table_name_(table_name),
  partition_id_(partition_id),
  readonly_(false),
  pstate_(ZPMeta::PState::ACTIVE),
  role_(Role::kNodeSingle),
  repl_state_(ReplState::kNoConnect),
  do_recovery_sync_(false),
  recover_sync_flag_(0),
  purging_(false),
  purged_index_(0) {
    // Partition related path
    log_path_ = NewPartitionPath(log_path, partition_id_);
    data_path_ = NewPartitionPath(data_path, partition_id_);
    sync_path_ = NewPartitionPath(zp_data_server->db_sync_path() + table_name_ + "/", partition_id_);
    bgsave_path_ = NewPartitionPath(zp_data_server->bgsave_path() + table_name_ + "/", partition_id_);

    if (log_path_.back() != '/') {
      log_path_.push_back('/');
    }
    if (data_path_.back() != '/') {
      data_path_.push_back('/');
    }
    if (sync_path_.back() != '/') {
      sync_path_.push_back('/');
    }
    if (bgsave_path_.back() != '/') {
      bgsave_path_.push_back('/');
    }

    
    pthread_rwlock_init(&state_rw_, NULL);
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&partition_rw_, &attr);
    
    pthread_rwlock_init(&purged_index_rw_, NULL);

  }

Status Partition::Init() {
  // Create db handle
  rocksdb::Status rs = rocksdb::DBNemo::Open(*(zp_data_server->db_options()),
      data_path_, &db_);
  if (!rs.ok()) {
    LOG(ERROR) << "DBNemo open failed: " << rs.ToString();
    return Status::Corruption(rs.ToString());
  }

  // Binlog
  Status s = Binlog::Create(log_path_, kBinlogSize, &logger_);
  if (!s.ok()) {
    LOG(ERROR) << "Create binlog failed: " << s.ToString();
    return s;
  }

  // Check and update purged_index_
  if (!CheckBinlogFiles()) {
    // Binlog unavailable
    LOG(ERROR) << "CheckBinlogFiles failed: " << s.ToString();
    return Status::Corruption("Check binlog file failed!");
  }
  return s;
}

Partition::~Partition() {
  delete logger_;
  delete db_;
  pthread_rwlock_destroy(&purged_index_rw_);
  pthread_rwlock_destroy(&partition_rw_);
  pthread_rwlock_destroy(&state_rw_);
  LOG(INFO) << " Partition " << table_name_ << "_" << partition_id_ << " exit!!!";
}

bool Partition::ShouldWaitDBSync() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) << " Partition: " << table_name_ << "_" << partition_id_ << " ShouldWaitDBSync " 
    << (repl_state_ == ReplState::kWaitDBSync ? "true" : "false")
    << ", repl_state: " << ReplStateMsg[repl_state_] << " role: " << RoleMsg[role_];
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
  LOG(INFO) << "Partition: " << table_name_ << "_" << partition_id_ << " WaitDBSyncDone  set repl_state: " << ReplStateMsg[repl_state_] <<
    "Master Node:" << master_node_.ip << ":" << master_node_.port;
}

bool Partition::ShouldTrySync() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) << "Partition: " << table_name_ << "_" << partition_id_ << " ShouldTrySync " 
    << (repl_state_ == ReplState::kShouldConnect ? "true" : "false")
    <<  ", repl_state: " << ReplStateMsg[repl_state_];
  return repl_state_ == ReplState::kShouldConnect;
}

void Partition::TrySyncDone() {
  slash::RWLock l(&state_rw_, true);
  assert(ReplState::kShouldConnect == repl_state_);
  repl_state_ = ReplState::kConnected;
  LOG(INFO) << " Partition: " << table_name_ << "_" << partition_id_ << " TrySyncDone  set repl_state: " << ReplStateMsg[repl_state_] <<
    ", Master Node:" << master_node_.ip << ":" << master_node_.port;
}

bool Partition::ChangeDb(const std::string& new_path) {
  std::string tmp_path(data_path_);
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  tmp_path += "_bak";
  slash::DeleteDirIfExist(tmp_path);
  slash::RWLock l(&partition_rw_, true);
  DLOG(INFO) << "Prepare change db from: " << tmp_path;
  delete db_;
  if (0 != slash::RenameFile(data_path_.c_str(), tmp_path)) {
    LOG(WARNING) << "Failed to rename db path when change db, error: " << strerror(errno);
    return false;
  }

  if (0 != slash::RenameFile(new_path.c_str(), data_path_.c_str())) {
    DLOG(INFO) << "Rename (" << new_path.c_str() << ", " << data_path_.c_str();
    LOG(WARNING) << "Failed to rename new db path when change db, error: " << strerror(errno);
    return false;
  }
  rocksdb::Status s = rocksdb::DBNemo::Open(*(zp_data_server->db_options()),
      data_path_, &db_);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to change to new db error: " << s.ToString();
  }
  slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Change Parition " << table_name_ << "_" << partition_id_ << " db success";
  return true;
}


////// BGSave //////

// Prepare env
bool Partition::InitBgsaveEnv() {
  slash::MutexLock l(&bgsave_protector_);
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
  bgsave_info_.s_start_time.assign(s_time, len);
  bgsave_info_.path = bgsave_path_ + bgsave_prefix();
  if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(WARNING) << "Remove exist bgsave dir failed, Partition:" << partition_id_;
    return false;
  }
  slash::CreatePath(bgsave_info_.path, 0755); // create parent directory
  bgsave_info_.path += std::string(s_time, 8);

  // Prepare for failed dir
  if (!slash::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
    LOG(WARNING) << "remove exist fail bgsave dir failed, Partition:" << partition_id_;
    return false;
  }

  return true;
}

// Prepare bgsave env
bool Partition::InitBgsaveContent(rocksdb::DBNemoCheckpoint* cp,
    CheckpointContent* content) {
  rocksdb::Status s;
  {
    slash::RWLock l(&partition_rw_, true);
    {
      slash::MutexLock l(&bgsave_protector_);
      logger_->GetProducerStatus(&bgsave_info_.filenum, &bgsave_info_.offset);
    }
    s = cp->GetCheckpointFiles(content->live_files,
        content->live_wal_files,
        content->manifest_file_size,
        content->sequence_number);
    if (!s.ok()){
      LOG(WARNING) << "set backup content failed " << s.ToString();
      return false;
    }
  }
  return true;
}

bool Partition::RunBgsave() {
  // Create new checkpoint
  CheckpointContent content;
  rocksdb::DBNemoCheckpoint* cp;
  rocksdb::Status s = rocksdb::DBNemoCheckpoint::Create(db_, &cp);
  if (!s.ok()) {
    LOG(WARNING) << "Create DBNemoCheckpoint failed :" << s.ToString();
    return false;
  }

  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveContent(cp, &content)) {
    delete cp;
    return false;
  }
  
  BGSaveInfo info = bgsave_info();
  DLOG(INFO) << "   bgsave_info: path=" << info.path << ",  filenum=" << info.filenum
      << ", offset=" << info.offset;
  
  // Backup to tmp dir
  s = cp->CreateCheckpointWithFiles(info.path,
      content.live_files,
      content.live_wal_files,
      content.manifest_file_size,
      content.sequence_number);
  DLOG(INFO) << "Create new backup finished, path is " << info.path;
  
  delete cp;
  if (!s.ok()) {
    LOG(WARNING) << "backup failed :" << s.ToString();
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
  DLOG(INFO) << "DoBGSave info file " << zp_data_server->local_ip() << ":" << zp_data_server->local_port() << " filenum=" << info.filenum << " offset=" << info.offset;
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
    p->ClearBgsave();
  }
  p->FinishBgsave();
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
    LOG(WARNING) << "Failed to open info file after db sync, table: " << table_name_ << " Partition:" << partition_id_ << " info_path:" << info_path;
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
        LOG(WARNING) << "Format of info file after db sync error, Partition:" << partition_id_ << "line : " << line;
        is.close();
        return false;
      }
      if (lineno == 3) { master_port = tmp; }
      else if (lineno == 4) { filenum = tmp; }
      else { offset = tmp; }

    } else if (lineno > 5) {
      LOG(WARNING) << "Format of info file after db sync error, Partition:" << partition_id_ << " line : " << line;
      is.close();
      return false;
    }
  }
  is.close();
  DLOG(INFO) << " info_path is " << info_path << ", sync_path_ is " << sync_path_;
  LOG(INFO) << "Information from dbsync info. Paritition: " << partition_id_
    << " master_ip: " << master_ip
    << ", master_port: " << master_port
    << ", filenum: " << filenum
    << ", offset: " << offset;

  // Sanity check
  {
    slash::RWLock l(&state_rw_, false);
    if (master_ip != master_node_.ip || master_port != master_node_.port) {
      LOG(WARNING) << "Error master ip port: " << master_ip << ":" << master_port << ". current master ip port: " << master_node_.ip <<":" << master_node_.port ;
      return false;
    }
  }

  slash::DeleteFile(info_path);
  if (!ChangeDb(sync_path_)) {
    return false;
  }

  // Update master offset
  SetBinlogOffset(filenum, offset);
  return true;
}

// Try to be master of node
// Return EndFile when the sync offset is larger than current one
// Return InvalidArgument when the offset is invalid
// Return Incomplete when neet sync db
Status Partition::SlaveAskSync(const Node &node, uint32_t filenum, uint64_t offset) {
  // Check role
  slash::RWLock ls(&state_rw_, false);
  if (role_ != Role::kNodeMaster
      || slave_nodes_.find(node) == slave_nodes_.end()) {
    DLOG(INFO) << "I'm not the master for :" << node
      << ", table: " << table_name_
      << ", partition: " << partition_id_;
    return Status::Corruption("Current node is not the master");
  }

  // Sanity check
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (cur_filenum < filenum || (cur_filenum == filenum && cur_offset < offset)) {
    return Status::EndFile("AddBinlogSender invalid binlog offset");
  }

  // Binlog already be purged
  LOG(INFO) << "Partition:" << table_name_ << "_" << partition_id_
    << ", We " << (purged_index_ > filenum ? "will" : "won't")
    << " TryDBSync, purged_index_=" << purged_index_ << ", filenum=" << filenum;
  slash::RWLock lp(&purged_index_rw_, false);
  if (purged_index_ > filenum) {
    TryDBSync(node.ip, node.port + kPortShiftRsync, cur_filenum);
    return Status::Incomplete("Bgsaving and DBSync first");
  }

  // Add binlog send task
  Status s = zp_data_server->AddBinlogSendTask(table_name_, partition_id_,
      Node(node.ip, node.port + kPortShiftSync), filenum, offset);
  if (s.ok()) {
    LOG(INFO) << "Success AddBinlogSendTask for Table " << table_name_
      << " Partition " << partition_id_ << " To "
      << node.ip << ":" << node.port << " at " << filenum << ", " << offset;
  } else if (s.IsInvalidArgument()) {
    // Invalid filenum and offset
    LOG(INFO) << "Failed AddBinlogSendTask for Table " << table_name_
      << " Partition " << partition_id_ << " To " << node.ip << ":" << node.port
      << " Since the Invalid Offset : " << filenum << ", " << offset;
  } else {
    LOG(WARNING) << "Failed AddBinlogSendTask for Table " << table_name_
      << " Partition " << partition_id_ << " To " << node.ip << ":" << node.port
      << " at " << filenum << ", " << offset << ", Error:" << s.ToString()
      << ", cur filenum:" << cur_filenum << ", cur offset" << cur_offset;
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
  LOG(INFO) << " Partition " << partition_id_ << " BecomeSingle";
  role_ = Role::kNodeSingle;
  repl_state_ = ReplState::kNoConnect;
  readonly_ = true;
}

// Requeired: hold write lock of state_rw_
void Partition::BecomeMaster() {
  LOG(INFO) << " Partition " << partition_id_ << " BecomeMaster";
  role_ = Role::kNodeMaster;
  repl_state_ = ReplState::kNoConnect;
  readonly_ = false;
  
  // Record binlog offset when I win the master for the later slave sync
  GetBinlogOffset(&win_filenum_, &win_offset_);
}

// Requeired: hold write lock of state_rw_
void Partition::BecomeSlave() {
  LOG(INFO) << " Partition " << partition_id_
    << " BecomeSlave, master is " << master_node_.ip << ":" << master_node_.port;
  role_ = Role::kNodeSlave;
  repl_state_ = ReplState::kShouldConnect;
  readonly_ = true;

  zp_data_server->AddSyncTask(this);
}

// Get binlog offset when I win the election
// Return false if I'm not a master
bool Partition::GetWinBinlogOffset(uint32_t* filenum, uint64_t* offset) {
  slash::RWLock l(&state_rw_, false);
  if (role_ != Role::kNodeMaster) {
    return false;
  }
  *filenum = win_filenum_;
  *offset = win_offset_;
  return true;
}

// Update partition state
// and return current state
ZPMeta::PState Partition::UpdateState(ZPMeta::PState state) {
  // Notice: could not become ACTIVE from STUCK
  // without master changed
  if (state == ZPMeta::PState::STUCK) {
    readonly_ == true;
  }
  pstate_ = state;
  return pstate_;
}

void Partition::Update(ZPMeta::PState state, const Node &master,
    const std::set<Node> &slaves) {
  slash::RWLock l(&state_rw_, true);

  // Check Status first
  if (ZPMeta::PState::ACTIVE != UpdateState(state)) {
    // Do not update master and slaves for stuck partition,
    // which will be updated when it become active
    return;
  }

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
      // And what's remain in miss_slaves is those who will not be slave any more
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
  snprintf(buf, sizeof(buf), "%s/%u", name.c_str(), current);
  return std::string(buf);
}

Partition* NewPartition(const std::string &table_name,
    const std::string& log_path, const std::string& data_path,
    const int partition_id, const Node& master, const std::set<Node> &slaves) {
  Partition* partition = new Partition(table_name, partition_id, log_path, data_path);

  Status s = partition->Init();
  if (!s.ok()) {
    delete partition;
    return NULL;
  }

  return partition;
}

Status Partition::SetBinlogOffset(uint32_t filenum, uint64_t offset) {
  uint64_t actual = 0;
  Status s = logger_->SetProducerStatus(filenum, offset, &actual);
  if (offset != actual) {
    LOG(WARNING) << "SetBinlogOffset actual small than expected"
      << ", expect:" << offset << ", actual:" << actual;
  }
  return s;
}

// Required: hold read mutex of status_rw_
bool Partition::CheckSyncOption(const PartitionSyncOption& option) {
  // Check current status
  if (role_ != Role::kNodeSlave || repl_state_ != ReplState::kConnected) {
    LOG(WARNING) << "Discard binlog item from " << option.from_node
      << ", partition:" << partition_id_
      << ", my current role: " << static_cast<int>(role_)
      << ", my current connection state: " << static_cast<int>(repl_state_);
    return false;
  }

  // Check from node
  if (option.from_node != slash::IpPortString(master_node_.ip, master_node_.port)) {
    LOG(WARNING) << "Discard binlog item from " << option.from_node
      << ", partition:" << partition_id_
      << ", current my master is " << master_node_;
    return false;
  }

  // Check offset
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (option.filenum != cur_filenum || option.offset != cur_offset) {
    LOG(WARNING) << "Discard binlog item from " << option.from_node
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
  CancelRecoverSync();
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
    pthread_rwlock_rdlock(&partition_rw_);
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
    pthread_rwlock_unlock(&partition_rw_);
  }

  if (g_zp_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_zp_conf->slowlog_slower_than()) {
      LOG(WARNING) << "slow sync command:" << cmd->name()
        << ", duration(us): " << duration;
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

void Partition::DoCommand(const Cmd* cmd, const client::CmdRequest &req,
    client::CmdResponse &res) {
  std::string key = cmd->ExtractKey(&req);

  slash::RWLock l(&state_rw_, false);
  if (cmd->is_write() && readonly_) {
    res.set_type(req.type());
    res.set_code(client::StatusCode::kError);
    res.set_msg("readonly mode");

    client::Node* node = res.mutable_redirect();
    node->set_ip(master_node_.ip);
    node->set_port(master_node_.port);

    LOG(WARNING) << "Readonly mode, failed to DoCommand  at Partition: " << partition_id_
      << " Role:" << RoleMsg[role_] << " ParititionState:" << static_cast<int>(pstate_);
    return;
  }

  uint64_t start_us = 0;
  if (g_zp_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  // Add read lock for no suspend command
  if (!cmd->is_suspend()) {
    pthread_rwlock_rdlock(&partition_rw_);
  }


  if (cmd->is_write()) {
    mutex_record_.Lock(key);
  }
  
  cmd->Do(&req, &res, this);

  if (cmd->is_write()) {
    if (res.code() == client::StatusCode::kOk  && req.type() != client::Type::SYNC) {
      // Restore Message
      std::string raw;
      req.SerializeToString(&raw);
      logger_->Put(raw);
    }
    mutex_record_.Unlock(key);
  }

  if (!cmd->is_suspend()) {
    pthread_rwlock_unlock(&partition_rw_);
  }

  if (g_zp_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_zp_conf->slowlog_slower_than()) {
      LOG(WARNING) << "slow client command:" << cmd->name()
        << ", duration(us): " << duration;
    }
  }
}

inline void Partition::TryRecoverSync() {
  do_recovery_sync_ = true;
}

inline void Partition::CancelRecoverSync() {
  // no mutex protect this two together
  // since we can tolerant the inconsistence
  do_recovery_sync_ = false;
  recover_sync_flag_ = 0;
}

void Partition::MaybeRecoverSync() {
  if (do_recovery_sync_) {
    recover_sync_flag_++;
    if (recover_sync_flag_ > kRecoverSyncDelayCronCount) {
      recover_sync_flag_ = 0;
      do_recovery_sync_ = false;
      slash::RWLock l(&state_rw_, true);
      BecomeSlave();
    }
  } else {
      recover_sync_flag_ = 0;
  }
}

void Partition::DoTimingTask() {
  // Maybe trysync
  MaybeRecoverSync();

  // Purge log
  if (!PurgeLogs(0, false)) {
    DLOG(WARNING) << "Auto purge failed";
    return;
  }
}

void Partition::TryDBSync(const std::string& ip, int port, int32_t top) {
  DLOG(INFO) << "TryDBSync " << ip << ":" << port << ", top=" << top << " Partition:" << partition_id_;

  std::string bg_path;
  uint32_t bg_filenum = 0;
  {
    slash::MutexLock l(&bgsave_protector_);
    bg_path = bgsave_info_.path;
    bg_filenum = bgsave_info_.filenum;
  }

  if (0 != slash::IsDir(bg_path) ||                               //Bgsaving dir exist
      !slash::FileExists(NewFileName(logger_->filename(), bg_filenum)) ||  //filenum can be found in binglog
      top - bg_filenum > kDBSyncMaxGap) {      //The file is not too old
    // Need Bgsave first
    Bgsave();
  }

  DBSync(ip, port);
}

void Partition::DBSync(const std::string& ip, int port) {
  // Only one DBSync task for every ip_port

  std::string ip_port = slash::IpPortString(ip, port);
  {
    slash::MutexLock l(&db_sync_protector_);
    if (db_sync_slaves_.find(ip_port) != db_sync_slaves_.end()) {
      DLOG(INFO) << " DBSync with (" << ip_port << ") already in schedule";
      return;
    }
    db_sync_slaves_.insert(ip_port);
  }

  DLOG(INFO) << " DBSync add new SyncTask for (" << ip_port << ")";
  // Reuse the bg_thread for Bgsave
  // Since we expect Bgsave and DBSync execute serially
  DBSyncArg *arg = new DBSyncArg(this, ip, port);
  zp_data_server->BGSaveTaskSchedule(&DoDBSync, static_cast<void*>(arg));
}

void Partition::DoDBSync(void* arg) {
  DBSyncArg *psync = static_cast<DBSyncArg*>(arg);
  Partition* partition = psync->p;

  //sleep(3);
  DLOG(INFO) << "DBSync begin sendfile " << psync->ip << ":" << psync->port;
  partition->DBSyncSendFile(psync->ip, psync->port);
  
  delete (DBSyncArg*)arg;
}

void Partition::DBSyncSendFile(const std::string& ip, int port) {
  std::string bg_path;
  {
    slash::MutexLock l(&bgsave_protector_);
    bg_path = bgsave_info_.path;
  }
  // Get all files need to send
  std::vector<std::string> descendant;
  if (!slash::GetDescendant(bg_path, descendant)) {
    LOG(WARNING) << "Get Descendant when try to do db sync failed";
    return;
  }

  DLOG(INFO) << "DBSyncSendFile descendant size= " << descendant.size() << " bg_path=" << bg_path;

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
    //DLOG(INFO) << "      RsyncSendFile("  << *it << ", " << target_dir << "/" << target_path << ")";
    // We need specify the speed limit for every single file
    ret = slash::RsyncSendFile(*it, target_dir + "/" + target_path, remote);
    if (0 != ret) {
      LOG(WARNING) << "rsync send file failed! From: " << *it
        << ", To: " << target_path
        << ", At: " << ip << ":" << port
        << ", Error: " << ret;
      break;
    }
  }
 
  // TODO anan rm
  //DLOG(INFO) << "      RsyncSendClearTarget("  << bg_path << "/[kv|hash], " << target_dir << "/[kv|hash])";
  // TODO clear sync_path if needed
  // Clear target path
  slash::RsyncSendClearTarget(bg_path, target_dir, remote);

  // Send info file at last
  if (0 == ret) {
    if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, target_dir + "/" + kBgsaveInfoFile, remote))) {
      LOG(WARNING) << "send info file failed";
    }
  }

  // remove slave
  std::string ip_port = slash::IpPortString(ip, port);
  {
    slash::MutexLock l(&db_sync_protector_);
    db_sync_slaves_.erase(ip_port);
  }
  if (0 == ret) {
    LOG(INFO) << "rsync send files success";
  }
}

bool Partition::FlushAll() {
  {
    slash::MutexLock l(&bgsave_protector_);
    if (bgsave_info_.bgsaving) {
      return false;
    }
  }
  std::string dbpath = data_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  int pos = dbpath.find_last_of('/');
  dbpath = dbpath.substr(0, pos);
  dbpath.append("/deleting");

  LOG(INFO) << "Delete old db...";
  delete db_;
  if (slash::RenameFile(data_path_, dbpath.c_str()) != 0) {
    LOG(WARNING) << "Failed to rename db path when flushall, error: " << strerror(errno);
    return false;
  }
 
  LOG(INFO) << "Prepare open new db...";
  rocksdb::Status s = rocksdb::DBNemo::Open(*(zp_data_server->db_options()),
      data_path_, &db_);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to open new db error: " << s.ToString();
  }
  LOG(INFO) << "open new db success";
  // TODO PurgeDir(dbpath);
  return true; 
}

bool Partition::PurgeLogs(uint32_t to, bool manual) {
  //usleep(300000);

  // Only one thread can go through
  bool expect = false;
  if (!purging_.compare_exchange_strong(expect, true)) {
    LOG(WARNING) << "purge process already exist : " << partition_id_;
    return false;
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

  ps->ClearPurge();
  delete (PurgeArg*)arg;
}

bool Partition::PurgeFiles(uint32_t to, bool manual)
{
  std::map<uint32_t, std::string> binlogs;
  if (!GetBinlogFiles(binlogs)) {
    LOG(WARNING) << "Could not get binlog files!";
    return false;
  }
  
  if (binlogs.size() <= kBinlogRemainMinCount) {
    // No neet purge
    return true;
  }

  int delete_num = 0;
  struct stat file_stat;
  int remain_expire_num = binlogs.size() - kBinlogRemainMaxCount;
  std::map<uint32_t, std::string>::iterator it;

  //DLOG(INFO) << "partition " << table_name_ << "_" << partition_id_ << " PugeFiles remain_expire_num is " << remain_expire_num;

  for (it = binlogs.begin(); it != binlogs.end(); ++it) {
    if ((manual && it->first <= to) ||           // Argument bound
        remain_expire_num > 0 ||                 // Expire num trigger
        (stat(((log_path_ + "/" + it->second)).c_str(), &file_stat) == 0 &&     
         file_stat.st_mtime < time(NULL) - kBinlogRemainMaxDay*24*3600)) // Expire time trigger
    {
      // We check this every time to avoid lock when we do file deletion
      if (!CouldPurge(it->first)) {
        //LOG(WARNING) << "Could not purge "<< (it->first) << ", since it is already be used";
        return false;
      }

      // Do delete
      slash::Status s = slash::DeleteFile(log_path_ + "/" + it->second);
      if (s.ok()) {
        ++delete_num;
        --remain_expire_num;
      } else {
        LOG(WARNING) << "Purge log file : " << (it->second) <<  " failed! error:" << s.ToString();
      }
    } else {
      // Break when face the first one not satisfied
      // Since the binlogs is order by the file index
      break;
    }
  }
  if (delete_num) {
    LOG(INFO) << "Success purge "<< delete_num << " for " << table_name_ << "_" << partition_id_;
  }

  return true;
}

bool Partition::CheckBinlogFiles() {
  std::vector<std::string> children;
  int ret = slash::GetChildren(log_path_, children);
  if (ret != 0){
    LOG(WARNING) << "CheckBinlogFiles Get all files in log path failed! Partition:" << partition_id_ << " Error:" << ret; 
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
  std::set<uint32_t>::iterator num_it = binlog_nums.begin(), pre_num_it = binlog_nums.begin();
  {
    slash::RWLock lp(&purged_index_rw_, true);
    purged_index_ = *num_it++; // update the purged_index_
  }
  DLOG(INFO) << "Partition: " << partition_id_ << " Update purged index to " << purged_index_; 
  for (; num_it != binlog_nums.end(); ++num_it, ++pre_num_it) {
    if (*num_it != *pre_num_it + 1) {
      LOG(ERROR) << "Partiton : " << partition_id_
        << " There is a hole among the binglogs between " <<  *num_it << " and "  << *pre_num_it; 
      // there is a hole
      return false;
    }
  }
  return true;
}

bool Partition::GetBinlogFiles(std::map<uint32_t, std::string>& binlogs) {
  std::vector<std::string> children;
  int ret = slash::GetChildren(log_path_, children);
  if (ret != 0){
    LOG(WARNING) << "GetBinlogFiles Get all files in log path failed! error:" << ret; 
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
      binlogs.insert(std::pair<uint32_t, std::string>(static_cast<uint32_t>(index), *it)); 
    }
  }
  return true;
}

bool Partition::CouldPurge(uint32_t index) {
  uint32_t pro_num;
  uint64_t tmp;
  logger_->GetProducerStatus(&pro_num, &tmp);

  if (index >= pro_num) {
    return false;
  }

  std::set<Node>::iterator it;
  slash::RWLock ls(&state_rw_, false);

  slash::RWLock lp(&purged_index_rw_, true);
  for (it = slave_nodes_.begin(); it != slave_nodes_.end(); ++it) {
    int32_t filenum = zp_data_server->GetBinlogSendFilenum(table_name_,
        partition_id_, Node((*it).ip, (*it).port + kPortShiftSync));
    //LOG(WARNING) << "slave node : " << Node((*it).ip, (*it).port + kPortShiftSync)
    //  << "filenum : " << filenum 
    //  << "index : " << index << index;
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

