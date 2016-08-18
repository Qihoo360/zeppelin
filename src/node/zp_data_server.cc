#include "zp_data_server.h"

#include <fstream>
#include <random>
#include <glog/logging.h>

#include "zp_data_worker_thread.h"
#include "zp_data_dispatch_thread.h"

#include "rsync.h"

ZPDataServer::ZPDataServer(const ZPOptions& options)
  : options_(options),
  role_(Role::kNodeSingle),
  repl_state_(ReplState::kNoConnect),
  readonly_(false),
  should_rejoin_(false),
  meta_state_(MetaState::kMetaConnect),
  bgsave_engine_(NULL) {

  pthread_rwlock_init(&state_rw_, NULL);
  pthread_rwlock_init(&meta_state_rw_, NULL);

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&server_rw_, &attr);

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

  // TEST
  //logger_ = new Binlog(options_.log_path, 1024);
  logger_ = new Binlog(options_.log_path);

  // TODO rm
  //LOG(INFO) << "local_host " << options_.local_ip << ":" << options.local_port;
}

ZPDataServer::~ZPDataServer() {
  //DLOG(INFO) << "~ZPDataServer dstor ";
  delete bgsave_engine_;

  delete zp_dispatch_thread_;
  for (int i = 0; i < worker_num_; i++) {
    delete zp_worker_thread_[i];
  }

  {
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();

    while (iter != slaves_.end()) {
      delete static_cast<ZPBinlogSenderThread*>(iter->sender);
      iter =  slaves_.erase(iter);
      LOG(INFO) << "Delete BinlogSender from slaves success";
    }
  }

  delete zp_ping_thread_;
  delete zp_binlog_receiver_thread_;
  delete zp_trysync_thread_;
  delete zp_metacmd_worker_thread_;

  // TODO 
  pthread_rwlock_destroy(&state_rw_);
  pthread_rwlock_destroy(&meta_state_rw_);
  pthread_rwlock_destroy(&server_rw_);

  db_.reset();
  delete logger_;
  LOG(INFO) << "ZPDataServerThread " << pthread_self() << " exit!!!";
}

Status ZPDataServer::Start() {
  zp_dispatch_thread_->StartThread();
  zp_binlog_receiver_thread_->StartThread();

  zp_ping_thread_->StartThread();
  zp_trysync_thread_->StartThread();
  zp_metacmd_worker_thread_->StartThread();

  // TEST 
  LOG(INFO) << "ZPDataServer started on port:" <<  options_.local_port;
  auto iter = options_.meta_addr.begin();
  while (iter != options_.meta_addr.end()) {
    LOG(INFO) << "seed is: " << *iter;
    iter++;
  }
//  if (options_.local_port != options_.seed_port || options_.local_ip != options_.seed_ip) {
//    repl_state_ = ReplState::kShouldConnect;
//  }

  server_mutex_.Lock();
  server_mutex_.Lock();

  server_mutex_.Unlock();
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


bool ZPDataServer::ShouldJoinMeta() {
  slash::RWLock l(&meta_state_rw_, false);
  DLOG(INFO) <<  "meta_state: " << meta_state_;
  return meta_state_ == MetaState::kMetaConnect;
}

////// Sync related //////
bool ZPDataServer::ShouldSync() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) <<  "repl_state: " << repl_state_;
  return repl_state_ == ReplState::kShouldConnect;
}

void ZPDataServer::SyncDone() {
  slash::RWLock l(&state_rw_, true);
  if (repl_state_ == ReplState::kShouldConnect) {
    repl_state_ = ReplState::kConnected;
  }
}

bool ZPDataServer::ShouldWaitDBSync() {
  slash::RWLock l(&state_rw_, false);
  DLOG(INFO) << "ShouldWaitDBSync repl_state: " << repl_state_ << " role: " << role_;
  return repl_state_ == ReplState::kWaitDBSync;
}

void ZPDataServer::SetWaitDBSync() {
  slash::RWLock l(&state_rw_, true);
  repl_state_ = ReplState::kWaitDBSync;
}

void ZPDataServer::WaitDBSyncDone() {
  slash::RWLock l(&state_rw_, true);
  if (repl_state_ == ReplState::kWaitDBSync) {
    repl_state_ = ReplState::kShouldConnect;
  }
}

bool ZPDataServer::ChangeDb(const std::string& new_path) {
  nemo::Options option;
  // TODO set options
  //option.write_buffer_size = g_pika_conf->write_buffer_size();
  //option.target_file_size_base = g_pika_conf->target_file_size_base();

  std::string db_path = options_.data_path;
  std::string tmp_path(db_path);
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  tmp_path += "_bak";
  slash::DeleteDirIfExist(tmp_path);

  slash::RWLock l(&server_rw_, true);
  LOG(INFO) << "Prepare change db from: " << tmp_path;
  db_.reset();
  if (0 != slash::RenameFile(db_path.c_str(), tmp_path)) {
    LOG(WARNING) << "Failed to rename db path when change db, error: " << strerror(errno);
    return false;
  }
 
  if (0 != slash::RenameFile(new_path.c_str(), db_path.c_str())) {
    DLOG(INFO) << "Rename (" << new_path.c_str() << ", " << db_path.c_str();
    LOG(WARNING) << "Failed to rename new db path when change db, error: " << strerror(errno);
    return false;
  }
  db_.reset(new nemo::Nemo(db_path, option));
  assert(db_);
  slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Change db success";
  return true;
}

bool ZPDataServer::FlushAll() {
 // TODO 
 // {
 //   slash::MutexLock l(&bgsave_protector_);
 //   if (bgsave_info_.bgsaving) {
 //     return false;
 //   }
 // }
  std::string dbpath = options_.data_path;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  int pos = dbpath.find_last_of('/');
  dbpath = dbpath.substr(0, pos);
  dbpath.append("/deleting");

  LOG(INFO) << "Delete old db...";
  db_.reset();
  if (slash::RenameFile(options_.data_path, dbpath.c_str()) != 0) {
    LOG(WARNING) << "Failed to rename db path when flushall, error: " << strerror(errno);
    return false;
  }
 
  LOG(INFO) << "Prepare open new db...";
  nemo::Options option;
  db_.reset(new nemo::Nemo(options_.data_path, option));
  assert(db_);
  LOG(INFO) << "open new db success";
  PurgeDir(dbpath);
  return true; 
}

void ZPDataServer::PurgeDir(std::string& path) {
  std::string *dir_path = new std::string(path);
  // Start new thread if needed
  purge_thread_.StartIfNeed();
  purge_thread_.Schedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void ZPDataServer::DoPurgeDir(void* arg) {
  std::string path = *(static_cast<std::string*>(arg));
  DLOG(INFO) << "Delete dir: " << path << " start";
  slash::DeleteDir(path);
  DLOG(INFO) << "Delete dir: " << path << " done";
  delete static_cast<std::string*>(arg);
}

void ZPDataServer::TryDBSync(const std::string& ip, int port, int32_t top) {
  std::string bg_path;
  uint32_t bg_filenum = 0;
  {
    slash::MutexLock l(&bgsave_protector_);
    bg_path = bgsave_info_.path;
    bg_filenum = bgsave_info_.filenum;
  }

  DLOG(INFO) << "TryDBSync " << ip << ":" << port << ", top filenum " << top << ", bg_filenum=" << bg_filenum << ", bg_path=" << bg_path;
  if (bg_path.empty() || 0 != slash::IsDir(bg_path) ||                    // Bgsaving dir exist
      !slash::FileExists(NewFileName(logger_->filename, bg_filenum)) ||   // filenum can be found in binglog
      top - bg_filenum > kDBSyncMaxGap) {                                 // master is beyond too many files  
    // Need Bgsave first
    Bgsave();
  }
  DBSync(ip, port);
}


void ZPDataServer::DBSync(const std::string& ip, int port) {
  // Only one DBSync task for every ip_port

  std::string ip_port = slash::IpPortString(ip, port);
  {
    slash::MutexLock l(&db_sync_protector_);
    if (db_sync_slaves_.find(ip_port) != db_sync_slaves_.end()) {
      return;
    }
    db_sync_slaves_.insert(ip_port);
  }

  // Reuse the bgsave_thread_
  // Since we expect Bgsave and DBSync execute serially
  bgsave_thread_.StartIfNeed();
  DBSyncArg *arg = new DBSyncArg(this, ip, port);
  bgsave_thread_.Schedule(&DoDBSync, static_cast<void*>(arg));
}

void ZPDataServer::DoDBSync(void* arg) {
  DBSyncArg *ppurge = static_cast<DBSyncArg*>(arg);
  ZPDataServer* server = ppurge->p;

  //sleep(3);
  DLOG(INFO) << "DBSync begin sendfile " << ppurge->ip << ":" << ppurge->port;
  server->DBSyncSendFile(ppurge->ip, ppurge->port);
  
  delete (DBSyncArg*)arg;
}

void ZPDataServer::DBSyncSendFile(const std::string& ip, int port) {
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
  std::string module = kDBSyncModule + "_" + slash::IpPortString(local_ip(), local_port());
  std::vector<std::string>::iterator it = descendant.begin();
  slash::RsyncRemote remote(ip, port, module, db_sync_speed_ * 1024);
  for (; it != descendant.end(); ++it) {
    target_path = (*it).substr(bg_path.size() + 1);
    DLOG(INFO) << "--- descendant: " << target_path;
    if (target_path == kBgsaveInfoFile) {
      continue;
    }
    // We need specify the speed limit for every single file
    ret = slash::RsyncSendFile(*it, target_path, remote);
    if (0 != ret) {
      LOG(WARNING) << "rsync send file failed! From: " << *it
        << ", To: " << target_path
        << ", At: " << ip << ":" << port
        << ", Error: " << ret;
      break;
    }
  }
 
  // Clear target path
  slash::RsyncSendClearTarget(bg_path + "/kv", "kv", remote);
  slash::RsyncSendClearTarget(bg_path + "/hash", "hash", remote);
  slash::RsyncSendClearTarget(bg_path + "/list", "list", remote);
  slash::RsyncSendClearTarget(bg_path + "/set", "set", remote);
  slash::RsyncSendClearTarget(bg_path + "/zset", "zset", remote);

  // Send info file at last
  if (0 == ret) {
    if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, kBgsaveInfoFile, remote))) {
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
    TryDBSync(slave.node.ip, slave.node.port + kPortShiftRsync, cur_filenum);
    return Status::Incomplete("Bgsaving and DBSync first");
    //return Status::InvalidArgument("AddBinlogSender invalid binlog filenum");
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

    LOG(INFO) << "AddBinlogSender ok, tid is " << slave.sender_tid;
    // Add sender
//    slash::MutexLock l(&slave_mutex_);
    slaves_.push_back(slave);
    DLOG(INFO) << " slaves_ size=" << slaves_.size();

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
    readonly_ = false;
  }
}

void ZPDataServer::BecomeSlave(const std::string& master_ip, int master_port) {
  LOG(INFO) << "BecomeSlave, master ip: " << master_ip << " master port: " << master_port;
  {
    slash::MutexLock l(&slave_mutex_);
    // Note: after erase, need not move iter
    for (auto iter = slaves_.begin(); iter != slaves_.end(); ) {
      //DLOG(INFO) << "before delete iter: " << iter->node.ip << ":" << iter->node.port << "   tid=" << iter->sender_tid;
      delete static_cast<ZPBinlogSenderThread*>(iter->sender);
      //DLOG(INFO) << "iter: " << iter->node.ip << ":" << iter->node.port << "   tid=" << iter->sender_tid;
      iter = slaves_.erase(iter);
      //LOG(INFO) << "Delete slave success";
    }

    // TODO brute clear the sync point
    logger_->SetProducerStatus(0, 0);
    FlushAll();
    LOG(INFO) << "Reset logger to (0,0)";
  }
  {
    slash::RWLock l(&state_rw_, true);
    role_ = Role::kNodeSlave;
    repl_state_ = ReplState::kShouldConnect;
    master_ip_ = master_ip;
    master_port_ = master_port;
    readonly_ = true;
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

void ZPDataServer::PickMeta() {
  slash::RWLock l(&meta_state_rw_, true);
  if (options_.meta_addr.empty()) {
    return;
  }

  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(0, options_.meta_addr.size()-1);
  int index = di(mt);

  auto addr = options_.meta_addr[index];
  auto pos = addr.find(":");
  if (pos != std::string::npos) {
    meta_ip_ = addr.substr(0, pos);
    auto str_port = addr.substr(pos+1);
    slash::string2l(str_port.data(), str_port.size(), &meta_port_); 
  }
  LOG(INFO) << "PickMeta ip: " << meta_ip_ << " port: " << meta_port_;
}

////// BGSave //////

// Prepare engine, need bgsave_protector protect
bool ZPDataServer::InitBgsaveEnv() {
  {
    slash::MutexLock l(&bgsave_protector_);
    // Prepare for bgsave dir
    bgsave_info_.start_time = time(NULL);
    char s_time[32];
    int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
    bgsave_info_.s_start_time.assign(s_time, len);
    bgsave_info_.path = bgsave_path() + bgsave_prefix() + std::string(s_time, 8);
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
bool ZPDataServer::InitBgsaveEngine() {
  delete bgsave_engine_;
  nemo::Status result = nemo::BackupEngine::Open(db_.get(), &bgsave_engine_);
  if (!result.ok()) {
    LOG(WARNING) << "open backup engine failed " << result.ToString();
    return false;
  }

  {
    slash::RWLock l(&server_rw_, true);
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

bool ZPDataServer::RunBgsaveEngine(const std::string path) {
  // Backup to tmp dir
  nemo::Status result = bgsave_engine_->CreateNewBackup(path);
  DLOG(INFO) << "Create new backup finished.";
  
  if (!result.ok()) {
    LOG(WARNING) << "backup failed :" << result.ToString();
    return false;
  }
  return true;
}

void ZPDataServer::Bgsave() {
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

void ZPDataServer::DoBgsave(void* arg) {
  ZPDataServer* p = static_cast<ZPDataServer*>(arg);
  BGSaveInfo info = p->bgsave_info();

  // Do bgsave
  bool ok = p->RunBgsaveEngine(info.path);

  DLOG(INFO) << "DoBGSave info file " << p->local_ip() << ":" << p->local_port() << " filenum=" << info.filenum << " offset=" << info.offset;
  // Some output
  std::ofstream out;
  out.open(info.path + "/info", std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(NULL) - info.start_time) << "s\n"
      << p->local_ip() << "\n" 
      << p->local_port() << "\n"
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

bool ZPDataServer::Bgsaveoff() {
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
