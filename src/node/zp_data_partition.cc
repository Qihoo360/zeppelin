#include "zp_data_partition.h"

#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_binlog_sender_thread.h"

extern ZPDataServer* zp_data_server;

Partition::Partition(const int partition_id, const std::string &log_path, const std::string &data_path)
  : partition_id_(partition_id),
  readonly_(false),
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
  DLOG(INFO) << "BecomeMaster";
  //TODO zp_data_server->binlog_receiver_thread()->KillBinlogSender();

  {
    slash::RWLock l(&state_rw_, true);
    role_ = Role::kNodeMaster;
    repl_state_ = ReplState::kNoConnect;
    readonly_ = false;
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
    readonly_ = true;
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
      cmd->Do(&req, &res, true);
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
  
  cmd->Do(&req, &res);

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

