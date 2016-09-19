#include "zp_data_partition.h"

#include <fstream>
#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_binlog_sender_thread.h"

#include "rsync.h"

extern ZPDataServer* zp_data_server;

Partition::Partition(const int partition_id, const std::string &log_path, const std::string &data_path)
  : logger_(NULL),
  partition_id_(partition_id),
  readonly_(false),
  role_(Role::kNodeSingle),
  repl_state_(ReplState::kNoConnect) {
    log_path_ = NewPartitionPath(log_path, partition_id_);
    data_path_ = NewPartitionPath(data_path, partition_id_);
    sync_path_ = NewPartitionPath(zp_data_server->db_sync_path(), partition_id_);
    bgsave_path_ = NewPartitionPath(zp_data_server->bgsave_path(), partition_id_);
    pthread_rwlock_init(&state_rw_, NULL);

    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&partition_rw_, &attr);
  }

Partition::~Partition() {
  delete logger_;
  pthread_rwlock_destroy(&partition_rw_);
  pthread_rwlock_destroy(&state_rw_);
}

bool Partition::FindSlave(const Node& node) {
  for (auto iter = slaves_.begin(); iter != slaves_.end(); iter++) {
    if (iter->node == node) {
      return true;
    }
  }
  return false;
}

bool Partition::ShouldWaitDBSync() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) << "ShouldWaitDBSync " 
      << (repl_state_ == ReplState::kWaitDBSync ? "true" : "false")
      << ", repl_state: " << repl_state_ << " role: " << role_;
  return repl_state_ == ReplState::kWaitDBSync;
}

void Partition::SetWaitDBSync() {
  slash::RWLock l(&state_rw_, true);
  repl_state_ = ReplState::kWaitDBSync;
}

void Partition::WaitDBSyncDone() {
  slash::RWLock l(&state_rw_, true);
  if (repl_state_ == ReplState::kWaitDBSync) {
    repl_state_ = ReplState::kShouldConnect;
  }
}

bool Partition::ShouldTrySync() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) << "ShouldTrySync " 
      << (repl_state_ == ReplState::kShouldConnect ? "true" : "false")
      <<  ", repl_state: " << repl_state_;
  return repl_state_ == ReplState::kShouldConnect;
}

void Partition::TrySyncDone() {
  slash::RWLock l(&state_rw_, true);
  if (repl_state_ == ReplState::kShouldConnect) {
    repl_state_ = ReplState::kConnected;
  }
}

bool Partition::ChangeDb(const std::string& new_path) {
  nemo::Options option;
  // TODO set options
  //option.write_buffer_size = g_pika_conf->write_buffer_size();
  //option.target_file_size_base = g_pika_conf->target_file_size_base();

  std::string tmp_path(data_path_);
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  tmp_path += "_bak";
  slash::DeleteDirIfExist(tmp_path);
  slash::RWLock l(&partition_rw_, true);
  LOG(INFO) << "Prepare change db from: " << tmp_path;
  db_.reset();
  if (0 != slash::RenameFile(data_path_.c_str(), tmp_path)) {
    LOG(WARNING) << "Failed to rename db path when change db, error: " << strerror(errno);
    return false;
  }
 
  if (0 != slash::RenameFile(new_path.c_str(), data_path_.c_str())) {
    DLOG(INFO) << "Rename (" << new_path.c_str() << ", " << data_path_.c_str();
    LOG(WARNING) << "Failed to rename new db path when change db, error: " << strerror(errno);
    return false;
  }
  db_.reset(new nemo::Nemo(data_path_, option));
  assert(db_);
  slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Change Parition " << partition_id_ << " db success";
  return true;
}


////// BGSave //////

// Prepare engine, need bgsave_protector protect
bool Partition::InitBgsaveEnv() {
  {
    slash::MutexLock l(&bgsave_protector_);
    // Prepare for bgsave dir
    bgsave_info_.start_time = time(NULL);
    char s_time[32];
    int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
    bgsave_info_.s_start_time.assign(s_time, len);
    bgsave_info_.path = bgsave_path_ + bgsave_prefix() + std::string(s_time, 8);
    if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
      LOG(WARNING) << "remove exist bgsave dir failed";
      return false;
    }
    slash::CreatePath(bgsave_info_.path, 0755);
    // Prepare for failed dir
    if (!slash::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
      LOG(WARNING) << "remove exist fail bgsave dir failed :";
      return false;
    }
  }
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool Partition::InitBgsaveEngine() {
  delete bgsave_engine_;
  nemo::Status result = nemo::BackupEngine::Open(db_.get(), &bgsave_engine_);
  if (!result.ok()) {
    LOG(WARNING) << "open backup engine failed " << result.ToString();
    return false;
  }

  {
    slash::RWLock l(&partition_rw_, true);
    {
      slash::MutexLock l(&bgsave_protector_);
      logger_->GetProducerStatus(&bgsave_info_.filenum, &bgsave_info_.offset);
    }
    result = bgsave_engine_->SetBackupContent();
    if (!result.ok()){
      LOG(WARNING) << "set backup content failed " << result.ToString();
      return false;
    }
  }
  return true;
}

bool Partition::RunBgsaveEngine(const std::string path) {
  // Backup to tmp dir
  nemo::Status result = bgsave_engine_->CreateNewBackup(path);
  DLOG(INFO) << "Create new backup finished.";
  
  if (!result.ok()) {
    LOG(WARNING) << "backup failed :" << result.ToString();
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
  
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return;
  }
  LOG(INFO) << " BGsave start";

  // Start new thread if needed
  bgsave_thread_.StartIfNeed();
  bgsave_thread_.Schedule(&DoBgsave, static_cast<void*>(this));
}

void Partition::DoBgsave(void* arg) {
  Partition* p = static_cast<Partition*>(arg);
  BGSaveInfo info = p->bgsave_info();

  // Do bgsave
  bool ok = p->RunBgsaveEngine(info.path);

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
  }
  p->FinishBgsave();
}

bool Partition::Bgsaveoff() {
  {
    slash::MutexLock l(&bgsave_protector_);
    if (!bgsave_info_.bgsaving) {
      return false;
    }
  }
  if (bgsave_engine_ != NULL) {
    bgsave_engine_->StopBackup();
  }
  return true;
}


// TODO
bool Partition::TryUpdateMasterOffset() {
  // Check dbsync finished
  std::string info_path = sync_path_ + kBgsaveInfoFile;
  if (!slash::FileExists(info_path)) {
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Failed to open info file after db sync";
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
        LOG(WARNING) << "Format of info file after db sync error, line : " << line;
        is.close();
        return false;
      }
      if (lineno == 3) { master_port = tmp; }
      else if (lineno == 4) { filenum = tmp; }
      else { offset = tmp; }

    } else if (lineno > 5) {
      LOG(WARNING) << "Format of info file after db sync error, line : " << line;
      is.close();
      return false;
    }
  }
  is.close();
  LOG(INFO) << "Information from dbsync info. master_ip: " << master_ip
    << ", master_port: " << master_port
    << ", filenum: " << filenum
    << ", offset: " << offset;

  // Sanity check
  {
    slash::RWLock l(&state_rw_, false);
    if (master_ip != master_node_.ip || master_port != master_node_.port) {
      LOG(ERROR) << "Error master ip port: " << master_ip << ":" << master_port;
      return false;
    }
  }

  // Replace the old db
  //slash::StopRsync(zp_data_server->db_sync_path());

  slash::DeleteFile(info_path);
  if (!ChangeDb(sync_path_)) {
    LOG(WARNING) << "Failed to change db";
    return false;
  }

  // Update master offset
  logger_->SetProducerStatus(filenum, offset);
  WaitDBSyncDone();
  return true;
}

// slave_mutex should be held
Status Partition::AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t offset) {
  // Sanity check
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (cur_filenum < filenum || (cur_filenum == filenum && cur_offset < offset)) {
    return Status::InvalidArgument("AddBinlogSender invalid binlog offset");
  }

  slash::SequentialFile *readfile;
  std::string confile = NewFileName(logger_->filename, filenum);
  if (!slash::FileExists(confile)) {
    // Not found binlog specified by filenum
    //return Status::Incomplete("Bgsaving and DBSync first");
    return Status::InvalidArgument("AddBinlogSender invalid binlog filenum");
  }

  if (!slash::NewSequentialFile(confile, &readfile).ok()) {
    return Status::IOError("AddBinlogSender new sequtialfile");
  }

  ZPBinlogSenderThread* sender = new ZPBinlogSenderThread(this, readfile, filenum, offset);

  slave.sender = sender;

  if (sender->trim() == 0) {
    sender->StartThread();
    pthread_t tid = sender->thread_id();
    slave.sender_tid = tid;

    LOG(INFO) << "AddBinlogSender ok, tid is " << slave.sender_tid << " hd_fd: " << slave.sync_fd;
    // Add sender
    slash::MutexLock l(&slave_mutex_);
    slaves_.push_back(slave);

    return Status::OK();
  } else {
    LOG(WARNING) << "AddBinlogSender failed";
    return Status::NotFound("AddBinlogSender bad sender");
  }
}

void Partition::DeleteSlave(int fd) {
  slash::MutexLock l(&slave_mutex_);
  for (auto iter = slaves_.begin(); iter != slaves_.end(); iter++) {
    if (iter->sync_fd == fd) {
      delete static_cast<ZPBinlogSenderThread*>(iter->sender);
      slaves_.erase(iter);
      LOG(INFO) << "Delete slave success";
      break;
    }
  }
}

void Partition::BecomeMaster() {
  DLOG(INFO) << " Partition " << partition_id_ << " BecomeMaster";
  //TODO zp_data_server->binlog_receiver_thread()->KillBinlogSender();

  {
    slash::RWLock l(&state_rw_, true);
    role_ = Role::kNodeMaster;
    repl_state_ = ReplState::kNoConnect;
    readonly_ = false;
  }
}

void Partition::BecomeSlave() {
  LOG(INFO) << "BecomeSlave, master is " << master_node_.ip << ":" << master_node_.port;
  {
    slash::MutexLock l(&slave_mutex_);
    for (auto iter = slaves_.begin(); iter != slaves_.end(); iter++) {
      delete static_cast<ZPBinlogSenderThread*>(iter->sender);
      iter = slaves_.erase(iter);
      LOG(INFO) << "Delete slave success";
    }
  }
  {
    slash::RWLock l(&state_rw_, true);
    role_ = Role::kNodeSlave;
    repl_state_ = ReplState::kShouldConnect;
    readonly_ = true;
  }
}

void Partition::Update(const std::vector<Node> &nodes) {
  assert(nodes.size() == kReplicaNum);

  slash::RWLock l(&state_rw_, true);
  master_node_ = nodes.at(0); 
  slave_nodes_.clear();
  for (size_t i = 1; i < nodes.size(); i++) {
    slave_nodes_.push_back(nodes[i]);
  }
}

void Partition::Init(const std::vector<Node> &nodes) {
  assert(nodes.size() == kReplicaNum);
  master_node_ = nodes.at(0); 
  for (size_t i = 1; i < nodes.size(); i++) {
    slave_nodes_.push_back(nodes[i]);
  }

  // Create DB handle
  nemo::Options nemo_option;
  DLOG(INFO) << "Loading data at " << data_path_ << " ...";
  db_ = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(data_path_, nemo_option));
  assert(db_);

  // Binlog
  logger_ = new Binlog(log_path_);

  // TODO  whether to remove this block
  // Is master
  if (master_node_.ip == zp_data_server->local_ip()
      && master_node_.port == zp_data_server->local_port()) {
      BecomeMaster();
  } else {
      BecomeSlave();
  }
}

std::string Partition::NewPartitionPath(const std::string& name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s/%u", name.c_str(), current);
  return std::string(buf);
}

Partition* NewPartition(const std::string log_path, const std::string data_path, const int partition_id, const std::vector<Node> &nodes) {
  Partition* partition = new Partition(partition_id, log_path, data_path);
  partition->Init(nodes);
  return partition;
}

void Partition::DoBinlogCommand(Cmd* cmd, client::CmdRequest &req, client::CmdResponse &res, const std::string &raw_msg) {
  DoCommand(cmd, req, res, raw_msg, true);
}

void Partition::DoCommand(Cmd* cmd, client::CmdRequest &req, client::CmdResponse &res,
    const std::string &raw_msg, bool is_from_binlog) {
  std::string key = cmd->key();

  if (!is_from_binlog && cmd->is_write()) {
    // TODO  very ugly implementation for readonly
    if (readonly()) {
      cmd->Do(&req, &res, this, true);
      return;
    }
  }

  // Add read lock for no suspend command
  if (!cmd->is_suspend()) {
    pthread_rwlock_rdlock(&partition_rw_);
  }

  // Add RecordLock for write cmd
  if (cmd->is_write()) {
    mutex_record_.Lock(key);
  }
  
  cmd->Do(&req, &res, this);

  if (cmd->is_write()) {
    if (cmd->result().ok() && req.type() != client::Type::SYNC) {
      // Restore Message
      WriteBinlog(raw_msg);
    }
    mutex_record_.Unlock(key);
  }

  if (!cmd->is_suspend()) {
    pthread_rwlock_unlock(&partition_rw_);
  }
}

inline void Partition::WriteBinlog(const std::string &content) {
  logger_->Lock();
  logger_->Put(content);
  logger_->Unlock();
}

