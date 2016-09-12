#include "zp_data_partition.h"

#include <fstream>
#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_binlog_sender_thread.h"

#include "rsync.h"

extern ZPDataServer* zp_data_server;

Partition::Partition(const int partition_id, const std::string &log_path, const std::string &data_path)
  : partition_id_(partition_id),
  role_(Role::kNodeSingle),
  repl_state_(ReplState::kNoConnect),
  logger_(NULL) {
    log_path_ = NewPartitionPath(log_path, partition_id_);
    data_path_ = NewPartitionPath(data_path, partition_id_);
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
  DLOG(INFO) << "ShouldWaitDBSync repl_state: " << repl_state_ << " role: " << role_;
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
  DLOG(INFO) <<  "repl_state: " << repl_state_;
  return repl_state_ == ReplState::kShouldConnect;
}

void Partition::TrySyncDone() {
  slash::RWLock l(&state_rw_, true);
  if (repl_state_ == ReplState::kShouldConnect) {
    repl_state_ = ReplState::kConnected;
  }
}

// TODO
bool Partition::TryUpdateMasterOffset() {
  // Check dbsync finished
  std::string info_path = zp_data_server->db_sync_path() + kBgsaveInfoFile;
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
  if (master_ip != zp_data_server->master_ip() ||
      master_port != zp_data_server->master_port()) {
    LOG(ERROR) << "Error master ip port: " << master_ip << ":" << master_port;
    return false;
  }

  // Replace the old db
  slash::StopRsync(zp_data_server->db_sync_path());
  //rsync_flag_ = false;
  slash::DeleteFile(info_path);
  if (!zp_data_server->ChangeDb(zp_data_server->db_sync_path())) {
    LOG(WARNING) << "Failed to change db";
    return false;
  }

  // Update master offset
  zp_data_server->logger_->SetProducerStatus(filenum, offset);
  zp_data_server->WaitDBSyncDone();
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
  }
}

void Partition::BecomeSlave() {
  LOG(INFO) << "BecomeSlave, master is " << master_node_.ip << " : " << master_node_.port;
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

void Partition::DoCommand(Cmd* cmd, google::protobuf::Message &req, google::protobuf::Message &res,
    const std::string &raw_msg) {
  std::string key = cmd->key();

  // Add read lock for no suspend command
  if (!cmd->is_suspend()) {
    pthread_rwlock_rdlock(&partition_rw_);
  }

  mutex_record_.Lock(key);

  cmd->Do(&req, &res);

  if (cmd->result().ok()) {
    // Restore Message
    WriteBinlog(raw_msg);
  }
  
  zp_data_server->mutex_record_.Unlock(key);

  if (!cmd->is_suspend()) {
    pthread_rwlock_unlock(&partition_rw_);
  }
}

inline void Partition::WriteBinlog(const std::string &content) {
  logger_->Lock();
  logger_->Put(content);
  logger_->Unlock();
}

