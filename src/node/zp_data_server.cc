#include "zp_data_server.h"

#include <fstream>
#include <random>
#include <glog/logging.h>

#include "zp_data_worker_thread.h"
#include "zp_data_dispatch_thread.h"

#include "rsync.h"

ZPDataServer::ZPDataServer(const ZPOptions& options)
  : options_(options),
  should_exit_(false),
  should_rejoin_(false),
  meta_state_(MetaState::kMetaConnect),
  meta_epoch_(-1),
  should_pull_meta_(false) {
    pthread_rwlock_init(&meta_state_rw_, NULL);
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&partition_rw_, NULL);


    // Create thread
    worker_num_ = 2; 
    for (int i = 0; i < worker_num_; i++) {
      zp_worker_thread_[i] = new ZPDataWorkerThread(kWorkerCronInterval);
    }

    zp_dispatch_thread_ = new ZPDataDispatchThread(options_.local_port, worker_num_, zp_worker_thread_, kDispatchCronInterval);
    zp_ping_thread_ = new ZPPingThread();
    //zp_metacmd_worker_thread_ = new ZPMetacmdWorkerThread(options_.local_port + kPortShiftDataCmd, kMetaCmdCronInterval);
    zp_metacmd_thread_ = new ZPMetacmdThread();
    zp_binlog_receiver_thread_ = new ZPBinlogReceiverThread(options_.local_port + kPortShiftSync, kBinlogReceiverCronInterval);
    zp_trysync_thread_ = new ZPTrySyncThread();

    // TODO rm
    //LOG(INFO) << "local_host " << options_.local_ip << ":" << options.local_port;
    DLOG(INFO) << "ZPDataServer cstor";
  }

ZPDataServer::~ZPDataServer() {
  DLOG(INFO) << "~ZPDataServer dstor";

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

  delete zp_ping_thread_;
  delete zp_binlog_receiver_thread_;
  delete zp_trysync_thread_;
  delete zp_metacmd_thread_;

  {
    slash::RWLock l(&partition_rw_, true);
    for (auto iter = partitions_.begin(); iter != partitions_.end(); iter++) {
      delete iter->second;
    }
  }

  bgsave_thread_.Stop();
  bgpurge_thread_.Stop();

  // TODO 
  pthread_rwlock_destroy(&meta_state_rw_);
  pthread_rwlock_destroy(&partition_rw_);

  LOG(INFO) << "ZPDataServerThread " << pthread_self() << " exit!!!";
}

Status ZPDataServer::Start() {
  zp_dispatch_thread_->StartThread();
  zp_binlog_receiver_thread_->StartThread();

  // TODO test
  zp_ping_thread_->StartThread();

  zp_trysync_thread_->StartThread();
  zp_metacmd_thread_->StartThread();

  // TEST 
  LOG(INFO) << "ZPDataServer started on port:" <<  options_.local_port;
  auto iter = options_.meta_addr.begin();
  while (iter != options_.meta_addr.end()) {
    LOG(INFO) << "seed is: " << *iter;
    iter++;
  }

  while (!should_exit_) {
    DoTimingTask();
    sleep(30);
  }
  return Status::OK();
}

bool ZPDataServer::ShouldJoinMeta() {
  slash::RWLock l(&meta_state_rw_, false);
  DLOG(INFO) <<  "ShouldJoinMeta meta_state: " << meta_state_;
  return meta_state_ == MetaState::kMetaConnect;
}

void ZPDataServer::UpdateEpoch(int64_t epoch) {
  slash::MutexLock l(&mutex_epoch_);
  if (epoch > meta_epoch_) {
    DLOG(INFO) <<  "UpdateEpoch (" << meta_epoch_ << "->" << epoch << ") ok...";
    meta_epoch_ = epoch;
    should_pull_meta_ = true;
  }
}

// Now we only need to keep one connection for Meta Node;
void ZPDataServer::PlusMetaServerConns() {
  slash::RWLock l(&meta_state_rw_, true);
  if (meta_server_conns_ == 0) {
    meta_state_ = MetaState::kMetaConnected;
    meta_server_conns_ = 1;
  }

  //if (meta_server_conns_ < 2) {
  //  if ((++meta_server_conns_) >= 2) {
  //    meta_state_ = MetaState::kMetaConnected;
  //    meta_server_conns_ = 2;
  //  }
  //}
}

void ZPDataServer::MinusMetaServerConns() {
  slash::RWLock l(&meta_state_rw_, true);
  if (meta_server_conns_ == 1) {
    meta_state_ = MetaState::kMetaConnect;
    meta_server_conns_ = 0;
  }

  //if (meta_server_conns_ > 0) {
  //  if ((--meta_server_conns_) <= 0) {
  //    meta_state_ = MetaState::kMetaConnect;
  //    meta_server_conns_ = 0;
  //  }
  //}
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

void ZPDataServer::DumpPartitions() {
  slash::RWLock l(&partition_rw_, false);

  DLOG(INFO) << "==========================";
  for (auto iter = partitions_.begin(); iter != partitions_.end(); iter++) {
    iter->second->Dump();
  }
  DLOG(INFO) << "--------------------------";
}

bool ZPDataServer::UpdateOrAddPartition(const int partition_id, const std::vector<Node>& nodes) {
  slash::RWLock l(&partition_rw_, true);
  auto iter = partitions_.find(partition_id);
  if (iter != partitions_.end()) {
    //TODO exist partition: update it
    // BecomeMaster BecomeSlave
    Partition* partition = iter->second;
    Node current_master = partition->master_node();

    if ((nodes[0].ip != options_.local_ip || nodes[0].port != options_.local_port)  // I'm not the told master
        && (nodes[0] != current_master)) {          // and there's a new master
      partition->Update(nodes);
      partition->BecomeSlave();
    }
    if ((nodes[0].ip == options_.local_ip && nodes[0].port == options_.local_port)  // I'm the told master and
        && !partition->is_master()) {
      partition->Update(nodes);
      partition->BecomeMaster();
    }

    return true;
  }

  // New Partition
  Partition* partition = NewPartition(options_.log_path, options_.data_path, partition_id, nodes);
  assert(partition != NULL);
  partitions_[partition_id] = partition;

  return true;
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

Partition* ZPDataServer::GetPartition(const std::string &key) {
  uint32_t id = KeyToPartition(key);
  return GetPartitionById(id);
}

Partition* ZPDataServer::GetPartitionById(const int partition_id) {
  slash::RWLock l(&partition_rw_, false);
  if (partitions_.find(partition_id) != partitions_.end()) {
    return partitions_[partition_id];
  }
  return NULL;
}

inline uint32_t ZPDataServer::KeyToPartition(const std::string &key) {
  slash::RWLock l(&partition_rw_, false);
  assert(partitions_.size() != 0);
  return std::hash<std::string>()(key) % partitions_.size();
}

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

void ZPDataServer::DoTimingTask() {
  slash::RWLock l(&partition_rw_, false);
  for (auto pair : partitions_) {
    pair.second->AutoPurge();
  }
}
