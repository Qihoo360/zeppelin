#include "zp_data_server.h"

#include <glog/logging.h>

#include "zp_data_worker_thread.h"
#include "zp_data_dispatch_thread.h"

ZPDataServer::ZPDataServer(const ZPOptions& options)
  : options_(options),
  role_(Role::kNodeSingle),
  repl_state_(ReplState::kNoConnect),
  should_rejoin_(false),
  meta_state_(MetaState::kMetaConnect) {
  pthread_rwlock_init(&state_rw_, NULL);

  // Create nemo handle
  nemo::Options option;
  std::string db_path = options.data_path;

  LOG(INFO) << "Loading data...";
  db_ = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(db_path, option));
  assert(db_);

  LOG(INFO) << " Success";

  // Create thread
  worker_num_ = 2; 
  for (int i = 0; i < worker_num_; i++) {
    zp_worker_thread_[i] = new ZPDataWorkerThread(kWorkerCronInterval);
  }

  zp_dispatch_thread_ = new ZPDataDispatchThread(options_.local_port, worker_num_, zp_worker_thread_, kDispatchCronInterval);
  zp_ping_thread_ = new ZPPingThread();
  zp_metacmd_worker_thread_ = new ZPMetacmdWorkerThread(options_.local_port + kPortShiftDataCmd, kMetaCmdCronInterval);
 
  zp_binlog_receiver_thread_ = new ZPBinlogReceiverThread(options_.local_port + kPortShiftSync, kBinlogReceiverCronInterval);
  zp_trysync_thread_ = new ZPTrySyncThread();

  logger_ = new Binlog(options_.log_path);

  // TODO rm
  //LOG(INFO) << "local_host " << options_.local_ip << ":" << options.local_port;
}

ZPDataServer::~ZPDataServer() {
  //delete zp_heartbeat_thread_;
  delete zp_trysync_thread_;
  delete zp_ping_thread_;
  delete zp_dispatch_thread_;
  delete zp_metacmd_worker_thread_;

  for (int i = 0; i < worker_num_; i++) {
    delete zp_worker_thread_[i];
  }

  delete zp_binlog_receiver_thread_;
  
  // TODO 
  pthread_rwlock_destroy(&state_rw_);
}

Status ZPDataServer::Start() {
  zp_dispatch_thread_->StartThread();
  zp_binlog_receiver_thread_->StartThread();

  zp_ping_thread_->StartThread();
  zp_trysync_thread_->StartThread();
  zp_metacmd_worker_thread_->StartThread();

  // TEST 
  LOG(INFO) << "ZPDataServer started on port:" <<  options_.local_port << ", seed is " << options_.seed_ip.c_str() << ":" << options_.seed_port;
//  if (options_.local_port != options_.seed_port || options_.local_ip != options_.seed_ip) {
//    repl_state_ = ReplState::kShouldConnect;
//  }

  server_mutex_.Lock();
  server_mutex_.Lock();
  return Status::OK();
}

bool ZPDataServer::FindSlave(const Node& node) {
  for (auto iter = slaves_.begin(); iter != slaves_.end(); iter++) {
    if (iter->node == node) {
      return true;
    }
  }
  return false;
}

bool ZPDataServer::ShouldTrySync() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) <<  "repl_state: " << repl_state_;
  return repl_state_ == ReplState::kShouldConnect;
}

void ZPDataServer::TrySyncDone() {
  slash::RWLock l(&state_rw_, true);
  if (repl_state_ == ReplState::kShouldConnect) {
    repl_state_ = ReplState::kConnected;
  }
}

bool ZPDataServer::ShouldJoinMeta() {
  slash::RWLock l(&meta_state_rw_, false);
  DLOG(INFO) <<  "meta_state: " << meta_state_;
  return meta_state_ == MetaState::kMetaConnect;
}

// slave_mutex should be held
Status ZPDataServer::AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t offset) {
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

  ZPBinlogSenderThread* sender = new ZPBinlogSenderThread(slave.node.ip, slave.node.port + kPortShiftSync, readfile, filenum, offset);

  slave.sender = sender;

  if (sender->trim() == 0) {
    sender->StartThread();
    pthread_t tid = sender->thread_id();
    slave.sender_tid = tid;

    LOG(INFO) << "AddBinlogSender ok, tid is " << slave.sender_tid << " hd_fd: " << slave.sync_fd;
    // Add sender
//    slash::MutexLock l(&slave_mutex_);
    slaves_.push_back(slave);

    return Status::OK();
  } else {
    LOG(WARNING) << "AddBinlogSender failed";
    return Status::NotFound("AddBinlogSender bad sender");
  }
}

void ZPDataServer::DeleteSlave(int fd) {
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

void ZPDataServer::BecomeMaster() {
  LOG(INFO) << "BecomeMaster";
  zp_binlog_receiver_thread_->KillBinlogSender();

  {
  slash::RWLock l(&state_rw_, true);
  role_ = Role::kNodeMaster;
  repl_state_ = ReplState::kNoConnect;
  master_ip_ = "";
  master_port_ = -1;
  }
}

void ZPDataServer::BecomeSlave(const std::string& master_ip, int master_port) {
  LOG(INFO) << "BecomeSlave, master ip: " << master_ip << " master port: " << master_port;
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
  master_ip_ = master_ip;
  master_port_ = master_port;
  }
}

void ZPDataServer::PlusMetaServerConns() {
  slash::RWLock l(&meta_state_rw_, true);
  if (meta_server_conns_ < 2) {
    if ((++meta_server_conns_) >= 2) {
      meta_state_ = MetaState::kMetaConnected;
      meta_server_conns_ = 2;
    }
  }
}

void ZPDataServer::MinusMetaServerConns() {
  slash::RWLock l(&meta_state_rw_, true);
  if (meta_server_conns_ > 0) {
    if ((--meta_server_conns_) <= 0) {
      meta_state_ = MetaState::kMetaConnect;
      meta_server_conns_ = 0;
    }
  }
}

