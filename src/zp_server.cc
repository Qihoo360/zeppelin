#include "zp_server.h"

#include <glog/logging.h>
#include "server_control.pb.h"

#include "zp_worker_thread.h"
#include "zp_dispatch_thread.h"

ZPServer::ZPServer(const ZPOptions& options)
  : options_(options),
  repl_state_(ReplState::kNoConnect),
  is_seed(options.local_ip == options.seed_ip && options.local_port == options.seed_port)
  {

  DLOG(INFO) << "is_seed " << is_seed;
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
    zp_worker_thread_[i] = new ZPWorkerThread(kWorkerCronInterval);
  }

  zp_dispatch_thread_ = new ZPDispatchThread(options_.local_port, worker_num_, zp_worker_thread_, kDispatchCronInterval);
  zp_ping_thread_ = new ZPPingThread();
  zp_heartbeat_thread_ = new ZPHeartbeatThread(options_.local_port + kPortShiftHeartbeat, kHeartbeatCronInterval);
  zp_binlog_receiver_thread_ = new ZPBinlogReceiverThread(options_.local_port + kPortShiftSync, kBinlogReceiverCronInterval);

  logger_ = new Binlog(options_.log_path);

  // TODO rm
  //LOG(INFO) << "local_host " << options_.local_ip << ":" << options.local_port;
}

ZPServer::~ZPServer() {
  delete zp_heartbeat_thread_;
  delete zp_ping_thread_;
  delete zp_dispatch_thread_;

  for (int i = 0; i < worker_num_; i++) {
    delete zp_worker_thread_[i];
  }

  delete zp_binlog_receiver_thread_;
  
  // TODO 
  if (is_seed) {
    delete zp_heartbeat_thread_;
  } else {
    delete zp_ping_thread_;
  }
  pthread_rwlock_destroy(&state_rw_);
}

Status ZPServer::Start() {
  zp_dispatch_thread_->StartThread();
  zp_binlog_receiver_thread_->StartThread();

  if (is_seed) {
    // TODO seed or normal both need
    zp_heartbeat_thread_->StartThread();
  } else {
    zp_ping_thread_->StartThread();
  }

  LOG(INFO) << "ZPServer started on port:" <<  options_.local_port << ", seed is " << options_.seed_ip.c_str() << ":" << options_.seed_port;
  server_mutex_.Lock();
  server_mutex_.Lock();
  return Status::OK();
}

bool ZPServer::FindSlave(const Node& node) {
  for (auto iter = slaves_.begin(); iter != slaves_.end(); iter++) {
    if (iter->node == node) {
      return true;
    }
  }
  return false;
}

bool ZPServer::ShouldJoin() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) <<  "repl_state: " << repl_state_;
  return !is_seed && repl_state_ == ReplState::kNoConnect;
}

// slave_mutex should be held
Status ZPServer::AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t offset) {
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

    LOG(INFO) << "AddBinlogSender ok, tid is " << slave.sender_tid << " hd_fd: " << slave.hb_fd;
    // Add sender
//    slash::MutexLock l(&slave_mutex_);
    slaves_.push_back(slave);

    return Status::OK();
  } else {
    LOG(WARNING) << "AddBinlogSender failed";
    return Status::NotFound("AddBinlogSender bad sender");
  }
}

void ZPServer::DeleteSlave(int fd) {
  slash::MutexLock l(&slave_mutex_);
  for (auto iter = slaves_.begin(); iter != slaves_.end(); iter++) {
    if (iter->hb_fd == fd) {
      delete static_cast<ZPBinlogSenderThread*>(iter->sender);
      slaves_.erase(iter);
      LOG(INFO) << "Delete slave success";
      break;
    }
  }
}
