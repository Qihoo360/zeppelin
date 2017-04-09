#include "zp_data_server.h"

#include <fstream>
#include <random>
#include <glog/logging.h>

#include "zp_data_worker_thread.h"
#include "zp_data_dispatch_thread.h"
#include <google/protobuf/text_format.h>

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
    pthread_rwlock_init(&table_rw_, &attr);

    // Command table
    cmds_.reserve(300);
    InitClientCmdTable();

    // Create thread
    zp_metacmd_bgworker_= new ZPMetacmdBGWorker();
    zp_trysync_thread_ = new ZPTrySyncThread();

    // Binlog related
    for (int j = 0; j < g_zp_conf->sync_recv_thread_num(); j++) {
      zp_binlog_receive_bgworkers_.push_back(
          new ZPBinlogReceiveBgWorker(kBinlogReceiveBgWorkerFull));
    }
    zp_binlog_receiver_thread_ = new ZPBinlogReceiverThread(g_zp_conf->local_port() + kPortShiftSync,
        kBinlogReceiverCronInterval);
    for (int i = 0; i < g_zp_conf->sync_send_thread_num(); ++i) {
      ZPBinlogSendThread *thread = new ZPBinlogSendThread(&binlog_send_pool_);
      binlog_send_workers_.push_back(thread);
    }

    for (int i = 0; i < g_zp_conf->data_thread_num(); i++) {
      zp_worker_thread_[i] = new ZPDataWorkerThread(kWorkerCronInterval);
    }
    zp_dispatch_thread_ = new ZPDataDispatchThread(g_zp_conf->local_port(),
        g_zp_conf->data_thread_num(), zp_worker_thread_, kDispatchCronInterval);

    zp_ping_thread_ = new ZPPingThread();

    InitDBOptions();

    // TODO rm
    //LOG(INFO) << "local_host " << options_.local_ip << ":" << options.local_port;
    DLOG(INFO) << "ZPDataServer constructed";
  }

ZPDataServer::~ZPDataServer() {
  DLOG(INFO) << "~ZPDataServer destoryed";
  // Order:
  // 1, Meta thread should before trysync thread
  // 2, Worker thread should before bgsave_thread
  // 3, binlog reciever should before recieve bgworker
  // 4, binlog send thread should before binlog send pool
  delete zp_ping_thread_;
  delete zp_dispatch_thread_;
  for (int i = 0; i < g_zp_conf->data_thread_num(); i++) {
    delete zp_worker_thread_[i];
  }


  std::vector<ZPBinlogSendThread*>::iterator it = binlog_send_workers_.begin();
  for (; it != binlog_send_workers_.end(); ++it) {
    delete *it;
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

  //delete binlog_send_pool_;
  delete zp_binlog_receiver_thread_;
  std::vector<ZPBinlogReceiveBgWorker*>::iterator binlogbg_iter = zp_binlog_receive_bgworkers_.begin();
  while(binlogbg_iter != zp_binlog_receive_bgworkers_.end()){
    delete (*binlogbg_iter);
    ++binlogbg_iter;
  }

  delete zp_trysync_thread_;
  delete zp_metacmd_bgworker_;

  {
    slash::RWLock l(&table_rw_, true);
    // TODO
    for (auto iter = tables_.begin(); iter != tables_.end(); iter++) {
      delete iter->second;
    }
  }
  LOG(INFO) << " All Tables exit!!!";

  bgsave_thread_.Stop();
  bgpurge_thread_.Stop();

  DestoryCmdTable(cmds_);
  // TODO 
  pthread_rwlock_destroy(&meta_state_rw_);
  pthread_rwlock_destroy(&table_rw_);

  LOG(INFO) << "ZPDataServerThread " << pthread_self() << " exit!!!";
}

void ZPDataServer::InitDBOptions() {
  // memtable大小256M，48个实例最多占用256M*2*48 = 24.6G
  db_options_.write_buffer_size = 256 * 1024 * 1024;
  // sst大小256M，这样可以减少sst文件数，从而减少fd个数
  db_options_.target_file_size_base = 256 * 1024 * 1024;
  /*
   * level 1触发compaction的大小为128M*4 = 512M
   * （这里假设从memtable到level 0会有50%的压缩）
   */
  db_options_.max_bytes_for_level_base = 512 * 1024 * 1024;
  // 加快db打开的速度
  db_options_.skip_stats_update_on_db_open = true;
  /*
   * compaction对需要的sst单独打开新的句柄，与Get()互不干扰
   * 并且使用2M的readhead来加速read，这里分配2M会带来额外的内存
   * 开销，默认单次compaction涉及的最大bytes为
   * target_file_size_base * 25，即25个sst文件，则每个rocksdb
   * 实例会额外消耗25*2M = 50M，48个实例一共消耗50M*48 = 2.4G
   */
  db_options_.compaction_readahead_size = 2 * 1024 * 1024;
  /*
   * 24T数据大约有198304个sst(256M)文件，则48个rocksdb实例
   * 每一个实例差不多有2048个，所以配置table_cache的capacity
   * 为2048
   */
  //db_options_.max_open_files = 2048;

  rocksdb::BlockBasedTableOptions block_based_table_options;
  /*
   * 使用512K的block size，修改block_size主要是为了减少index block的大小
   * 但鉴于本例中单条value很大，其实效果不明显，所以这个可改可不改
 */
  block_based_table_options.block_size = 16 * 1024;

  db_options_.table_factory.reset(
     NewBlockBasedTableFactory(block_based_table_options));
  
  db_options_.max_background_flushes = g_zp_conf->max_background_flushes();
  db_options_.max_background_compactions = g_zp_conf->max_background_compactions();
  
  db_options_.create_if_missing = true;
}

Status ZPDataServer::Start() {
  if (pink::RetCode::kSuccess != zp_dispatch_thread_->StartThread()) {
    LOG(INFO) << "Dispatch thread start failed";
    return Status::Corruption("Dispatch thread start failed!");
  }

  if (pink::RetCode::kSuccess != zp_binlog_receiver_thread_->StartThread()) {
    LOG(INFO) << "Binlog receiver thread start failed";
    return Status::Corruption("Binlog receiver thread start failed!");
  }

  if (pink::RetCode::kSuccess != zp_ping_thread_->StartThread()) {
    LOG(INFO) << "Ping thread start failed";
    return Status::Corruption("Ping thread start failed!");
  }

  std::vector<ZPBinlogSendThread*>::iterator bsit = binlog_send_workers_.begin();
  for (; bsit != binlog_send_workers_.end(); ++bsit) {
    LOG(INFO) << "Start one binlog send worker thread";
    if (pink::RetCode::kSuccess != (*bsit)->StartThread()) {
      LOG(INFO) << "Binlog send worker start failed";
      return Status::Corruption("Binlog send worker start failed!");
    }
  }

  // TEST 
  LOG(INFO) << "ZPDataServer started on port:" <<  g_zp_conf->local_port();
  auto iter = g_zp_conf->meta_addr().begin();
  while (iter != g_zp_conf->meta_addr().end()) {
    LOG(INFO) << "seed is: " << *iter;
    iter++;
  }

  while (!should_exit_) {
    DoTimingTask();
    int sleep_count = kNodeCronWaitCount;
    while (!should_exit_ && sleep_count-- > 0){
      usleep(kNodeCronInterval * 1000);
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
  LOG(INFO) <<  "UpdateEpoch (" << meta_epoch_ << "->" << epoch << ") ok...";
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

  LOG(INFO) << "TablePartition==========================";
  for (auto iter = tables_.begin(); iter != tables_.end(); iter++) {
    iter->second->Dump();
  }
  LOG(INFO) << "TablePartition--------------------------";
}

Status ZPDataServer::SendToPeer(const Node &node, const client::SyncRequest &msg) {
  pink::Status res;
  std::string ip_port = slash::IpPortString(node.ip, node.port);

  slash::MutexLock pl(&mutex_peers_);
  std::unordered_map<std::string, ZPPbCli*>::iterator iter = peers_.find(ip_port);
  if (iter == peers_.end()) {
    ZPPbCli *cli = new ZPPbCli();
    res = cli->Connect(node.ip, node.port);
    if (!res.ok()) {
      delete cli;
      return Status::Corruption(res.ToString());
    }
    cli->set_send_timeout(1000);
    cli->set_recv_timeout(1000);
    iter = (peers_.insert(std::pair<std::string, ZPPbCli*>(ip_port, cli))).first;
  }
  
  res = iter->second->Send(const_cast<client::SyncRequest*>(&msg));
  if (!res.ok()) {
    // Remove when second Failed, retry outside
    iter->second->Close();
    delete iter->second;
    peers_.erase(iter);
    return Status::Corruption(res.ToString());
  }
  return Status::OK();
}

Table* ZPDataServer::GetOrAddTable(const std::string &table_name) {
  slash::RWLock l(&table_rw_, true);
  auto it = tables_.find(table_name);
  if (it != tables_.end()) {
    return it->second;
  }

  Table *table = NewTable(table_name, g_zp_conf->log_path(), g_zp_conf->data_path());
  tables_[table_name] = table;
  return table;
}

void ZPDataServer::DeleteTable(const std::string &table_name) {
  // TODO wangkang delete table
  // Before which all partition point outside should become invalid
  return;
  slash::RWLock l(&table_rw_, true);
  auto it = tables_.find(table_name);
  if (it != tables_.end()) {
    delete it->second;
  }
  tables_.erase(table_name);
}

// Note: table pointer 
Table* ZPDataServer::GetTable(const std::string &table_name) {
  slash::RWLock l(&table_rw_, false);
  auto it = tables_.find(table_name);
  if (it != tables_.end()) {
    return it->second;
  }
  return NULL;
}

// We will dump all tables when table_name is empty.
void ZPDataServer::DumpTableBinlogOffsets(const std::string &table_name,
                                          std::unordered_map<std::string, std::vector<PartitionBinlogOffset>> &all_offset) {
  slash::RWLock l(&table_rw_, false);
  if (table_name.empty()) {
    for (auto& item : tables_) {
      std::vector<PartitionBinlogOffset> poffset;
      (item.second)->DumpPartitionBinlogOffsets(poffset);
      all_offset.insert(std::pair<std::string,
                        std::vector<PartitionBinlogOffset>>(item.first, poffset));
    }
  } else {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
      std::vector<PartitionBinlogOffset> poffset;
      it->second->DumpPartitionBinlogOffsets(poffset);
      all_offset.insert(std::pair<std::string,
                        std::vector<PartitionBinlogOffset>>(it->first, poffset));
    }
  }
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

// Add Task, remove first if already exist
// Return Status::InvalidArgument means the filenum and offset is Invalid
Status ZPDataServer::AddBinlogSendTask(const std::string &table, int partition_id, const Node& node,
    int32_t filenum, int64_t offset) {
  return binlog_send_pool_.AddNewTask(table, partition_id, node, filenum, offset, true);
}

Status ZPDataServer::RemoveBinlogSendTask(const std::string &table, int partition_id, const Node& node) {
  std::string task_name = ZPBinlogSendTaskName(table, partition_id, node);
  return binlog_send_pool_.RemoveTask(task_name);
}

// Return the task filenum indicated by id and node
// -1 when the task is not exist
// -2 when the task is exist but is processing now
int32_t ZPDataServer::GetBinlogSendFilenum(const std::string &table, int partition_id, const Node& node) {
  std::string task_name = ZPBinlogSendTaskName(table, partition_id, node);
  return binlog_send_pool_.TaskFilenum(task_name);
}

void ZPDataServer::DumpBinlogSendTask() {
  LOG(INFO) << "BinlogSendTask==========================";
  binlog_send_pool_.Dump();
  LOG(INFO) << "BinlogSendTask--------------------------";
}

void ZPDataServer::AddSyncTask(Partition* partition) {
  zp_trysync_thread_->TrySyncTaskSchedule(partition);
}

void ZPDataServer::AddMetacmdTask() {
  zp_metacmd_bgworker_->AddTask();
}

// Here, we dispatch task base on its partition id
// So that the task within same partition will be located on same thread
// So there could be no lock in DoBinlogReceiveTask to keep binlogs order
void ZPDataServer::DispatchBinlogBGWorker(ZPBinlogReceiveTask *task) {
    size_t index = task->option.partition_id % zp_binlog_receive_bgworkers_.size();
    zp_binlog_receive_bgworkers_[index]->AddTask(task);
}

// Statistic related
bool ZPDataServer::GetAllTableName(std::set<std::string>& table_names) {
  slash::RWLock l(&table_rw_, false);
  for (auto iter = tables_.begin(); iter != tables_.end(); iter++) {
    table_names.insert(iter->first);
  }
  return true;
}

bool ZPDataServer::GetTableStat(const std::string& table_name, std::vector<Statistic>& stats) {
  std::set<std::string> stat_tables;
  if (table_name.empty()) {
    GetAllTableName(stat_tables);
  } else {
    stat_tables.insert(table_name);
  }

  for (auto it = stat_tables.begin(); it != stat_tables.end(); it++) {
    Statistic sum;
    sum.table_name = *it;
    for (int i = 0; i < g_zp_conf->data_thread_num(); i++) {
      Statistic tmp;
      zp_worker_thread_[i]->GetStat(*it, tmp);
      sum.Add(tmp);
    }
    stats.push_back(sum);
  }
  return true;
}

bool ZPDataServer::GetTableCapacity(const std::string& table_name,
    std::vector<Statistic>& capacity_stats) {
  slash::RWLock l(&table_rw_, false);
  if (table_name.empty()) {
    for (auto& item : tables_) {
      Statistic tmp;
      tmp.table_name = item.first;
      (item.second)->GetCapacity(&tmp);
      capacity_stats.push_back(tmp);
    }
  } else {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
      Statistic tmp;
      tmp.table_name = it->first;
      it->second->GetCapacity(&tmp);
      capacity_stats.push_back(tmp);
    }
  }
  return true;
}

void ZPDataServer::InitClientCmdTable() {
  // SetCmd
  Cmd* setptr = new SetCmd(kCmdFlagsKv | kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::SET), setptr));
  // GetCmd
  Cmd* getptr = new GetCmd(kCmdFlagsKv | kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::GET), getptr));
  // DelCmd
  Cmd* delptr = new DelCmd(kCmdFlagsKv | kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::DEL), delptr));
  // One InfoCmd handle many type queries;
  Cmd* infostatsptr = new InfoCmd(kCmdFlagsAdmin | kCmdFlagsRead | kCmdFlagsMultiPartition);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::INFOSTATS), infostatsptr));
  Cmd* infocapacityptr = new InfoCmd(kCmdFlagsAdmin | kCmdFlagsRead | kCmdFlagsMultiPartition);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::INFOCAPACITY), infocapacityptr));
  Cmd* infopartitionptr = new InfoCmd(kCmdFlagsAdmin | kCmdFlagsRead | kCmdFlagsMultiPartition);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::INFOPARTITION), infopartitionptr));
  // SyncCmd
  Cmd* syncptr = new SyncCmd(kCmdFlagsAdmin | kCmdFlagsRead | kCmdFlagsSuspend);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::SYNC), syncptr));
  // MgetCmd
  Cmd* mgetptr = new MgetCmd(kCmdFlagsKv | kCmdFlagsRead | kCmdFlagsMultiPartition);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::MGET), mgetptr));
}

void ZPDataServer::DoTimingTask() {
  slash::RWLock l(&table_rw_, false);
  for (auto& pair : tables_) {
    pair.second->DoTimingTask();
  }
}
