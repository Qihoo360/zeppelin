#include "zp_data_server.h"

#include <fstream>
#include <random>
#include <glog/logging.h>

#include "zp_data_worker_thread.h"
#include "zp_data_dispatch_thread.h"

#include "rsync.h"

ZPDataServer::ZPDataServer()
  : table_count_(0),
   should_exit_(false),
   meta_epoch_(-1),
   should_pull_meta_(false) {
    pthread_rwlock_init(&meta_state_rw_, NULL);
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&table_rw_, NULL);
    
    // Command table
    cmds_.reserve(300);
    InitClientCmdTable();

    // Create thread
    worker_num_ = 4;
    for (int i = 0; i < worker_num_; i++) {
      zp_worker_thread_[i] = new ZPDataWorkerThread(kWorkerCronInterval);
    }

    zp_dispatch_thread_ = new ZPDataDispatchThread(g_zp_conf->local_port(), worker_num_, zp_worker_thread_, kDispatchCronInterval);
    zp_ping_thread_ = new ZPPingThread();
    //zp_metacmd_worker_thread_ = new ZPMetacmdWorkerThread(options_.local_port + kPortShiftDataCmd, kMetaCmdCronInterval);
    zp_metacmd_thread_ = new ZPMetacmdThread();
    zp_trysync_thread_ = new ZPTrySyncThread();

    for (int j = 0; j < kBinlogReceiveBgWorkerCount; j++) {
      zp_binlog_receive_bgworkers_.push_back(
          new ZPBinlogReceiveBgWorker(kBinlogReceiveBgWorkerFull));
    }
    zp_binlog_receiver_thread_ = new ZPBinlogReceiverThread(g_zp_conf->local_port() + kPortShiftSync, kBinlogReceiverCronInterval);

    // TODO rm
    //LOG(INFO) << "local_host " << options_.local_ip << ":" << options.local_port;
    DLOG(INFO) << "ZPDataServer constructed";
  }

ZPDataServer::~ZPDataServer() {
  DLOG(INFO) << "~ZPDataServer destoryed";
  // Order:
  // 1, Meta thread should before trysunc thread
  // 2, Worker thread should before bgsave_thread
  // 3, binlog reciever should before recieve bgworker
  delete zp_ping_thread_;
  delete zp_dispatch_thread_;
  for (int i = 0; i < worker_num_; i++) {
    delete zp_worker_thread_[i];
  }

  {
    slash::MutexLock l(&mutex_peers_);
    std::unordered_map<std::string, ZPPbCli*>::iterator iter = peers_.begin();
    while (iter != peers_.end()) {
      iter->second->Close();
      delete iter->second;
      iter++;
    }
  }

  delete zp_metacmd_thread_;
  delete zp_trysync_thread_;
  delete zp_binlog_receiver_thread_;
  std::vector<ZPBinlogReceiveBgWorker*>::iterator binlogbg_iter = zp_binlog_receive_bgworkers_.begin();
  while(binlogbg_iter != zp_binlog_receive_bgworkers_.end()){
    delete (*binlogbg_iter);
    ++binlogbg_iter;
  }

  {
    slash::RWLock l(&table_rw_, true);
    // TODO
    for (auto iter = tables_.begin(); iter != tables_.end(); iter++) {
      delete iter->second;
    }
  }

  bgsave_thread_.Stop();
  bgpurge_thread_.Stop();

  DestoryCmdTable(cmds_);
  // TODO 
  pthread_rwlock_destroy(&meta_state_rw_);
  pthread_rwlock_destroy(&table_rw_);

  LOG(INFO) << "ZPDataServerThread " << pthread_self() << " exit!!!";
}

Status ZPDataServer::Start() {
  zp_dispatch_thread_->StartThread();
  zp_binlog_receiver_thread_->StartThread();
  zp_ping_thread_->StartThread();

  // TEST 
  LOG(INFO) << "ZPDataServer started on port:" <<  g_zp_conf->local_port();
  auto iter = g_zp_conf->meta_addr().begin();
  while (iter != g_zp_conf->meta_addr().end()) {
    LOG(INFO) << "seed is: " << *iter;
    iter++;
  }

  while (!should_exit_) {
    DoTimingTask();
    int sleep_count = 600;
    while (!should_exit_ && --sleep_count > 0){
      sleep(1);
    }
  }
  return Status::OK();
}

void ZPDataServer::TryUpdateEpoch(int64_t epoch) {
  slash::MutexLock l(&mutex_epoch_);
  if (epoch != meta_epoch_) {
    LOG(INFO) <<  "Meta epoch changed: " << meta_epoch_ << " to " << epoch;
    should_pull_meta_ = true;
    AddMetacmdTask();
  }
}

void ZPDataServer::FinishPullMeta(int64_t epoch) {
  slash::MutexLock l(&mutex_epoch_);
  DLOG(INFO) <<  "UpdateEpoch (" << meta_epoch_ << "->" << epoch << ") ok...";
  meta_epoch_ = epoch;
  should_pull_meta_ = false;
}

void ZPDataServer::PickMeta() {
  slash::RWLock l(&meta_state_rw_, true);
  if (g_zp_conf->meta_addr().empty()) {
    return;
  }

  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(0, g_zp_conf->meta_addr().size()-1);
  int index = di(mt);

  auto addr = g_zp_conf->meta_addr()[index];
  auto pos = addr.find("/");
  if (pos != std::string::npos) {
    meta_ip_ = addr.substr(0, pos);
    auto str_port = addr.substr(pos+1);
    slash::string2l(str_port.data(), str_port.size(), &meta_port_); 
  }
  LOG(INFO) << "PickMeta ip: " << meta_ip_ << " port: " << meta_port_;
}

void ZPDataServer::DumpTablePartitions() {
  slash::RWLock l(&table_rw_, false);

  DLOG(INFO) << "==========================";
  for (auto iter = tables_.begin(); iter != tables_.end(); iter++) {
    iter->second->Dump();
  }
  DLOG(INFO) << "--------------------------";
}

bool ZPDataServer::UpdateOrAddTablePartition(const std::string &table_name, const int partition_id, const Node& master, const std::vector<Node>& slaves) {
  Table* table = NULL;
  {
    slash::RWLock l(&table_rw_, true);
    auto it = tables_.find(table_name);

    if (it == tables_.end()) {
      table = NewTable(table_name, g_zp_conf->log_path(), g_zp_conf->data_path());
      tables_[table_name] = table;
    }
  }
  assert(table != NULL);

  return table->UpdateOrAddPartition(partition_id, master, slaves);
}

Status ZPDataServer::SendToPeer(const std::string &peer_ip, int peer_port, const std::string &data) {
  pink::Status res;
  std::string ip_port = slash::IpPortString(peer_ip, peer_port);

  slash::MutexLock pl(&mutex_peers_);
  std::unordered_map<std::string, ZPPbCli*>::iterator iter = peers_.find(ip_port);
  if (iter == peers_.end()) {
    ZPPbCli *cli = new ZPPbCli();
    res = cli->Connect(peer_ip, peer_port);
    if (!res.ok()) {
      delete cli;
      return Status::Corruption(res.ToString());
    }
    iter = (peers_.insert(std::pair<std::string, ZPPbCli*>(ip_port, cli))).first;
  }

  res = iter->second->SendRaw(data.data(), data.size());
  if (!res.ok()) {
    // Remove when second Failed, retry outside
    iter->second->Close();
    delete iter->second;
    peers_.erase(iter);
    return Status::Corruption(res.ToString());
  }
  return Status::OK();
}

//Partition* ZPDataServer::GetPartition(const std::string &key) {
//  uint32_t id = KeyToPartition(key);
//  return GetPartitionById(id);
//}


bool ZPDataServer::SetTablePartitionCount(const std::string &table_name, const int count) {
  Table* table = GetTable(table_name);
  
  return table == NULL ? false : table->SetPartitionCount(count);
}

// Note: table pointer 
Table* ZPDataServer::GetTable(const std::string &table_name) {
  slash::RWLock l(&table_rw_, false);
  auto it = tables_.find(table_name);
  //std::unordered_map<std::string, Table*>::iterator it = tables_.find(table_name);
  if (it != tables_.end()) {
    return it->second;
  }
  return NULL;
}

Partition* ZPDataServer::GetTablePartition(const std::string &table_name, const std::string &key) {
  Table* table = GetTable(table_name);
  return table == NULL ? NULL : table->GetPartition(key);
}

Partition* ZPDataServer::GetTablePartitionById(const std::string &table_name, const int partition_id) {
  Table* table = GetTable(table_name);
  return table == NULL ? NULL : table->GetPartitionById(partition_id);
}

//inline uint32_t ZPDataServer::KeyToPartition(const std::string &key) {
//  assert(partition_count_ != 0);
//  return std::hash<std::string>()(key) % partition_count_;
//}

void ZPDataServer::BGSaveTaskSchedule(void (*function)(void*), void* arg) {
  slash::MutexLock l(&bgsave_thread_protector_);
  bgsave_thread_.StartIfNeed();
  bgsave_thread_.Schedule(function, arg);
}

void ZPDataServer::BGPurgeTaskSchedule(void (*function)(void*), void* arg) {
  slash::MutexLock l(&bgpurge_thread_protector_);
  bgpurge_thread_.StartIfNeed();
  bgpurge_thread_.Schedule(function, arg);
}

void ZPDataServer::AddSyncTask(const std::string &table_name, int parititon_id) {
  zp_trysync_thread_->TrySyncTaskSchedule(table_name, parititon_id);
}

void ZPDataServer::AddMetacmdTask() {
  zp_metacmd_thread_->MetacmdTaskSchedule();
}

// Here, we dispatch task base on its partition id
// So that the task within same partition will be located on same thread
// So there could be no lock in DoBinlogReceiveTask to keep binlogs order
void ZPDataServer::DispatchBinlogBGWorker(const std::string &table_name, const std::string& key, ZPBinlogReceiveArg *arg) {
  Table* table = GetTable(table_name);
  assert(table != NULL);

  if (table != NULL) {
    uint32_t id = table->KeyToPartition(key);
    arg->partition_id = id;
    size_t index = id % zp_binlog_receive_bgworkers_.size();
    zp_binlog_receive_bgworkers_[index]->StartIfNeed();
    zp_binlog_receive_bgworkers_[index]->Schedule(
      &ZPBinlogReceiveBgWorker::DoBinlogReceiveTask, static_cast<void*>(arg));
  }
}

void ZPDataServer::InitClientCmdTable() {
  // SetCmd
  Cmd* setptr = new SetCmd(kCmdFlagsKv | kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::SET), setptr));
  // GetCmd
  Cmd* getptr = new GetCmd(kCmdFlagsKv | kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::GET), getptr));
  // SyncCmd
  Cmd* syncptr = new SyncCmd(kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSuspend);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::SYNC), syncptr));
}

void ZPDataServer::DoTimingTask() {
  slash::RWLock l(&table_rw_, false);
  for (auto pair : tables_) {
    pair.second->DoTimingTask();
  }
}
