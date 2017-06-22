#include "src/node/zp_data_server.h"

#include <fstream>
#include <random>
#include <glog/logging.h>
#include <sys/resource.h>
#include <google/protobuf/text_format.h>

#include "slash/include/rsync.h"

#include "src/node/zp_sync_conn.h"
#include "src/node/zp_data_client_conn.h"

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

    LOG(INFO) << "ZPMetaServer start initialization";

    // Try to raise the file descriptor
    struct  rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) != -1) {
      if (limit.rlim_cur < (rlim_t)g_zp_conf->max_file_descriptor_num()) {
        // rlim_cur could be set by any user while rlim_max are
        // changeable only by root.
        rlim_t previous_limit = limit.rlim_cur;
        limit.rlim_cur = g_zp_conf->max_file_descriptor_num();
        if(limit.rlim_cur > limit.rlim_max) {
          limit.rlim_max = g_zp_conf->max_file_descriptor_num();
        }
        if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
          LOG(WARNING) << "your 'limit -n ' of " << previous_limit << " is not enough for zeppelin to start, zeppelin have successfully reconfig it to " << limit.rlim_cur;
        } else {
          LOG(FATAL) << "your 'limit -n ' of " << previous_limit << " is not enough for zeppelin to start, but zeppelin can not reconfig it: " << strerror(errno) <<" do it by yourself";
        };
      }
    } else {
      LOG(WARNING) << "getrlimir error: " << strerror(errno);
    }

    // Command table
    cmds_.reserve(300);
    InitClientCmdTable();

    // Create thread
    zp_metacmd_bgworker_= new ZPMetacmdBGWorker();
    zp_trysync_thread_ = new ZPTrySyncThread();
    
    // Binlog receive
    for (int j = 0; j < g_zp_conf->sync_recv_thread_num(); j++) {
      zp_binlog_receive_bgworkers_.push_back(
          new ZPBinlogReceiveBgWorker(kBinlogReceiveBgWorkerFull));
    }
    sync_factory_ = new ZPSyncConnFactory();
    sync_handle_ = new ZPSyncConnHandle();
    zp_binlog_receiver_thread_ = pink::NewHolyThread(
        g_zp_conf->local_port() + kPortShiftSync,
        sync_factory_,
        kBinlogReceiverCronInterval,
        sync_handle_);

    // binlog Reiver don't check keepalive 
    zp_binlog_receiver_thread_->set_keepalive_timeout(0);
    zp_binlog_receiver_thread_->set_thread_name("ZPDataSyncDispatch");

    // Binlog send
    for (int i = 0; i < g_zp_conf->sync_send_thread_num(); ++i) {
      ZPBinlogSendThread *thread = new ZPBinlogSendThread(&binlog_send_pool_);
      binlog_send_workers_.push_back(thread);
    }

    // Client command
    client_factory_ = new ZPDataClientConnFactory();
    client_handle_ = new ZPDataClientConnHandle();
    zp_dispatch_thread_ = pink::NewDispatchThread(
        g_zp_conf->local_port(),
        g_zp_conf->data_thread_num(),
        client_factory_,
        kDispatchCronInterval,
        client_handle_);

    // KeepAlive in seconds
    zp_dispatch_thread_->set_keepalive_timeout(kKeepAlive);
    zp_dispatch_thread_->set_thread_name("ZPDataDispatch");

    // Ping
    zp_ping_thread_ = new ZPPingThread();

    InitDBOptions();
    DLOG(INFO) << "ZPDataServer constructed";
  }

ZPDataServer::~ZPDataServer() {
  DLOG(INFO) << "~ZPDataServer destoryed";
  // Order:
  // 1, Meta thread should before trysync thread
  // 2, binlog reciever should before recieve bgworker
  // 3, binlog send thread should before binlog send pool
  delete zp_ping_thread_;

  // We call StopThread first
  zp_dispatch_thread_->StopThread();
  delete zp_dispatch_thread_;
  delete client_factory_;
  delete client_handle_;
  LOG(INFO) << "Dispatch thread exit!";

  auto it = binlog_send_workers_.begin();
  for (; it != binlog_send_workers_.end(); ++it) {
    delete *it;
  }

  {
    slash::MutexLock l(&mutex_peers_);
    auto iter = peers_.begin();
    while (iter != peers_.end()) {
      iter->second->Close();
      delete iter->second;
      iter++;
    }
  }
  LOG(INFO) << "Peers client exit!";

  zp_binlog_receiver_thread_->StopThread();
  delete zp_binlog_receiver_thread_;
  LOG(INFO) << "Binlig receiver thread exit!";
  auto binlogbg_iter = zp_binlog_receive_bgworkers_.begin();
  while(binlogbg_iter != zp_binlog_receive_bgworkers_.end()){
    delete (*binlogbg_iter);
    ++binlogbg_iter;
  }
  delete sync_factory_;
  delete sync_handle_;

  // Statistic result
  for (int i = 0; i < 2; i++) {
    slash::MutexLock l(&(stats_[i].mu));
    for (auto& item : stats_[i].table_stats) {
      delete item.second;
    }
  }

  delete zp_trysync_thread_;
  delete zp_metacmd_bgworker_;

  LOG(INFO) << " All Tables exit!!!";
  bgsave_thread_.StopThread();
  bgpurge_thread_.StopThread();

  DestoryCmdTable(cmds_);
  pthread_rwlock_destroy(&meta_state_rw_);
  pthread_rwlock_destroy(&table_rw_);
  LOG(INFO) << "ZPDataServerThread " << pthread_self() << " exit!!!";
}

void ZPDataServer::InitDBOptions() {
  // Assume 48 rocksdb install totally

  // memtable each
  db_options_.write_buffer_size = g_zp_conf->db_write_buffer_size() * 1024;

  // memtable max
  db_options_.write_buffer_manager.reset(
      new rocksdb::WriteBufferManager(g_zp_conf->db_max_write_buffer() * 1024));
  
  // sst file size
  db_options_.target_file_size_base = g_zp_conf->db_target_file_size_base() * 1024;
 
  // suppose 50% compression radio
  db_options_.max_bytes_for_level_base = 2 * db_options_.write_buffer_size;
  
  // speed up db open 
  db_options_.skip_stats_update_on_db_open = true;
  
  db_options_.compaction_readahead_size = 2 * 1024 * 1024;
  
  db_options_.max_open_files = g_zp_conf->db_max_open_files();

  rocksdb::BlockBasedTableOptions block_based_table_options;
  
  block_based_table_options.block_size = g_zp_conf->db_block_size() * 1024;

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
  std::unordered_map<std::string, pink::PinkCli*>::iterator iter = peers_.find(ip_port);
  if (iter == peers_.end()) {
    pink::PinkCli *cli = pink::NewPbCli();
    res = cli->Connect(node.ip, node.port);
    if (!res.ok()) {
      cli->Close();
      delete cli;
      return Status::Corruption(res.ToString());
    }
    cli->set_send_timeout(1000);
    cli->set_recv_timeout(1000);
    iter = (peers_.insert(std::pair<std::string, pink::PinkCli*>(ip_port, cli))).first;
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

std::shared_ptr<Table> ZPDataServer::GetOrAddTable(const std::string &table_name) {
  slash::RWLock l(&table_rw_, true);
  auto it = tables_.find(table_name);
  if (it != tables_.end()) {
    return it->second;
  }

  std::shared_ptr<Table> table = NewTable(table_name,
      g_zp_conf->log_path(), g_zp_conf->data_path());
  tables_[table_name] = table;
  return table;
}

void ZPDataServer::DeleteTable(const std::string &table_name) {
  slash::RWLock l(&table_rw_, true);
  auto it = tables_.find(table_name);
  if (it != tables_.end()) {
    it->second->LeaveAllPartition();
  }
  tables_.erase(table_name);
}

// Required: hold table_rw_
std::shared_ptr<Table> ZPDataServer::GetTable(const std::string &table_name) {
  auto it = tables_.find(table_name);
  if (it != tables_.end()) {
    return it->second;
  }
  return NULL;
}

// We will dump all tables when table_name is empty.
void ZPDataServer::DumpTableBinlogOffsets(const std::string &table_name,
    TablePartitionOffsets *all_offset) {
  slash::RWLock l(&table_rw_, false);
  if (table_name.empty()) {
    for (auto& item : tables_) {
      std::map<int, BinlogOffset> poffset;
      (item.second)->DumpPartitionBinlogOffsets(&poffset);
      all_offset->insert(std::pair<std::string,
          std::map<int, BinlogOffset>>(item.first, poffset));
    }
  } else {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
      std::map<int, BinlogOffset> poffset;
      it->second->DumpPartitionBinlogOffsets(&poffset);
      all_offset->insert(std::pair<std::string,
          std::map<int, BinlogOffset>>(it->first, poffset));
    }
  }
}

std::shared_ptr<Partition> ZPDataServer::GetTablePartition(
    const std::string &table_name, const std::string &key) {
  slash::RWLock l(&table_rw_, false);
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->GetPartition(key) : NULL;
}

std::shared_ptr<Partition> ZPDataServer::GetTablePartitionById(
    const std::string &table_name, const int partition_id) {
  slash::RWLock l(&table_rw_, false);
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->GetPartitionById(partition_id) : NULL;
}

int ZPDataServer::KeyToPartition(const std::string& table_name,
    const std::string &key) {
  slash::RWLock l(&table_rw_, false);
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->KeyToPartition(key) : -1;
}

void ZPDataServer::BGSaveTaskSchedule(void (*function)(void*), void* arg) {
  slash::MutexLock l(&bgsave_thread_protector_);
  bgsave_thread_.StartThread();
  bgsave_thread_.Schedule(function, arg);
}

void ZPDataServer::BGPurgeTaskSchedule(void (*function)(void*), void* arg) {
  slash::MutexLock l(&bgpurge_thread_protector_);
  bgpurge_thread_.StartThread();
  bgpurge_thread_.Schedule(function, arg);
}

// Add Task, remove first if already exist
// Return Status::InvalidArgument means the filenum and offset is Invalid
Status ZPDataServer::AddBinlogSendTask(const std::string &table, int partition_id,
    const std::string& binlog_filename, const Node& node, int32_t filenum,
    int64_t offset) {
  return binlog_send_pool_.AddNewTask(table, partition_id, binlog_filename,
      node, filenum, offset, true);
}

Status ZPDataServer::RemoveBinlogSendTask(const std::string &table,
    int partition_id, const Node& node) {
  std::string task_name = ZPBinlogSendTaskName(table, partition_id, node);
  return binlog_send_pool_.RemoveTask(task_name);
}

// Return the task filenum indicated by id and node
// -1 when the task is not exist
// -2 when the task is exist but is processing now
int32_t ZPDataServer::GetBinlogSendFilenum(const std::string &table,
    int partition_id, const Node& node) {
  std::string task_name = ZPBinlogSendTaskName(table, partition_id, node);
  return binlog_send_pool_.TaskFilenum(task_name);
}

void ZPDataServer::DumpBinlogSendTask() {
  LOG(INFO) << "BinlogSendTask==========================";
  binlog_send_pool_.Dump();
  LOG(INFO) << "BinlogSendTask--------------------------";
}

void ZPDataServer::AddSyncTask(const std::string& table,
    int partition_id, uint64_t delay) {
  zp_trysync_thread_->TrySyncTaskSchedule(table, partition_id, delay);
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

//
// Statistic related
//
bool ZPDataServer::GetStat(const StatType type, const std::string &table, Statistic& stat) {
  stat.Reset();

  slash::MutexLock l(&(stats_[type].mu));
  auto it = stats_[type].table_stats.find(table);
  if (it == stats_[type].table_stats.end()) {
    return false;
  }
  stat = *(it->second);
  return true;
}

void ZPDataServer::PlusStat(const StatType type, const std::string &table) {
  slash::MutexLock l(&(stats_[type].mu));
  if (table.empty()) {
    stats_[type].other_stat.querys++;
  } else {
    auto it = stats_[type].table_stats.find(table);
    if (it == stats_[type].table_stats.end()) {
      Statistic* pstat = new Statistic;
      pstat->table_name = table;
      pstat->querys++;
      stats_[type].table_stats[table] = pstat;
    } else {
      (it->second)->querys++;
    }
  }
}

void ZPDataServer::ResetLastStat(const StatType type) {
  uint64_t cur_time_us = slash::NowMicros();
  slash::MutexLock l(&(stats_[type].mu));
  // TODO(anan) debug;
  for (auto it = stats_[type].table_stats.begin();
       it != stats_[type].table_stats.end(); it++) {
    auto stat = it->second;
    stat->last_qps = ((stat->querys - stat->last_querys) * 1000000
                      / (cur_time_us - stats_[type].last_time_us + 1));
    stat->last_querys = stat->querys;
    //stat->Dump();
  }
  stats_[type].other_stat.last_qps =
      ((stats_[type].other_stat.querys - stats_[type].other_stat.last_querys)
       * 1000000 / (cur_time_us - stats_[type].last_time_us + 1));
  stats_[type].other_stat.last_querys = stats_[type].other_stat.querys;
  stats_[type].last_time_us = cur_time_us;
}

bool ZPDataServer::GetAllTableName(std::set<std::string>* table_names) {
  slash::RWLock l(&table_rw_, false);
  for (auto iter = tables_.begin(); iter != tables_.end(); iter++) {
    table_names->insert(iter->first);
  }
  return true;
}

bool ZPDataServer::GetTotalStat(const StatType type, Statistic& stat) {
  stat.Reset();
  slash::MutexLock l(&(stats_[type].mu));
  for (auto it = stats_[type].table_stats.begin();
       it != stats_[type].table_stats.end(); it++) {
    stat.Add(*(it->second));
  }
  stat.Add(stats_[type].other_stat);
  return true;
}

bool ZPDataServer::GetTableStat(const StatType type,
    const std::string& table_name, std::vector<Statistic>& stats) {
  std::set<std::string> stat_tables;
  if (table_name.empty()) {
    GetAllTableName(&stat_tables);
  } else {
    stat_tables.insert(table_name);
  }

  for (auto it = stat_tables.begin(); it != stat_tables.end(); it++) {
    Statistic sum;
    GetStat(type, *it, sum);
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

bool ZPDataServer::GetTableReplInfo(const std::string& table_name,
    std::unordered_map<std::string, client::CmdResponse_InfoRepl>* info_repls) {
  slash::RWLock l(&table_rw_, false);
  client::CmdResponse_InfoRepl info_repl;
  if (!table_name.empty()) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
      return false;
    }
    it->second->GetReplInfo(&info_repl);
    info_repls->insert(std::pair<std::string, client::CmdResponse_InfoRepl>(
          table_name, info_repl));
    return true;
  }
  
  // All table
  for (auto& table : tables_) {
    table.second->GetReplInfo(&info_repl);
    info_repls->insert(std::pair<std::string, client::CmdResponse_InfoRepl>(
          table.first, info_repl));
  }
  return true;
}

bool ZPDataServer::GetServerInfo(client::CmdResponse_InfoServer* info_server) {
  info_server->set_epoch(meta_epoch());
  std::set<std::string> table_names;
  GetAllTableName(&table_names);
  for (auto& name : table_names) {
    info_server->add_table_names(name);
  }

  {
  slash::RWLock l(&meta_state_rw_, false);
  info_server->mutable_cur_meta()->set_ip(meta_ip_);
  info_server->mutable_cur_meta()->set_port(meta_port_);
  }

  info_server->set_meta_renewing(ShouldPullMeta());
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
  Cmd* inforepl = new InfoCmd(kCmdFlagsAdmin | kCmdFlagsRead | kCmdFlagsMultiPartition);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::INFOREPL), inforepl));
  Cmd* infoserver = new InfoCmd(kCmdFlagsAdmin | kCmdFlagsRead | kCmdFlagsMultiPartition);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::INFOSERVER), infoserver));
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

