#ifndef ZP_DATA_SERVER_H
#define ZP_DATA_SERVER_H

#include <string>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

#include "bg_thread.h"
#include "pb_conn.h"
#include "zp_pb_cli.h"
#include "holy_thread.h"

#include "slash_status.h"
#include "slash_mutex.h"

#include "nemo.h"
#include "nemo_backupable.h"

#include "zp_data_command.h"
#include "zp_conf.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_const.h"
#include "zp_metacmd_bgworker.h"
#include "zp_ping_thread.h"
#include "zp_trysync_thread.h"
#include "zp_binlog_sender.h"
#include "zp_binlog_receiver_thread.h"
#include "zp_binlog_receive_bgworker.h"
#include "zp_data_table.h"
#include "zp_data_partition.h"


using slash::Status;

class ZPDataServer;
class ZPDataServerConn;
class ZPDataServerThread;

class ZPDataWorkerThread;
class ZPDataDispatchThread;

//class Table;

extern ZpConf* g_zp_conf;
class ZPDataServer {
 public:

  explicit ZPDataServer();
  virtual ~ZPDataServer();
  Status Start();

  std::string meta_ip() {
    slash::RWLock l(&meta_state_rw_, false);
    return meta_ip_;
  }
  int meta_port() {
    slash::RWLock l(&meta_state_rw_, false);
    return meta_port_;
  }
  std::string local_ip() {
    return g_zp_conf->local_ip();
  }
  int local_port() {
    return g_zp_conf->local_port();
  }

  bool IsSelf(const Node& node) {
    return (g_zp_conf->local_ip() == node.ip && g_zp_conf->local_port() == node.port);
  }

  std::string db_sync_path() {
    return "./sync_" + std::to_string(g_zp_conf->local_port()) + "/";
  }

  std::string bgsave_path() {
    return g_zp_conf->data_path() + "/dump/";
  }

  void Exit() {
    should_exit_ = true;
  }

  //ZPMetacmdWorkerThread* zp_metacmd_worker_thread() {
  //  return zp_metacmd_worker_thread_;
  //};

  ZPBinlogReceiverThread* zp_binlog_receiver_thread() {
    return zp_binlog_receiver_thread_;
  };

  // Meta related
  bool ShouldJoinMeta();
  void MetaConnected();
  void MetaDisconnect();
  void PickMeta();
  int64_t meta_epoch() {
    slash::MutexLock l(&mutex_epoch_);
    return meta_epoch_;
  }
  void TryUpdateEpoch(int64_t epoch);
  void FinishPullMeta(int64_t epoch);
  bool ShouldPullMeta() {
    slash::MutexLock l(&mutex_epoch_);
    return should_pull_meta_;
  }
  bool Availible() {
    slash::MutexLock l(&mutex_epoch_);
    return meta_epoch_ >= 0;
  }
  
  // Table related
  //bool UpdateOrAddPartition(const int partition_id, const Node& master, const std::vector<Node>& nodes);
  //Partition* GetPartition(const std::string &key);
  //Partition* GetPartitionById(const int partition_id);

  Table* GetOrAddTable(const std::string &table_name);

  Table* GetTable(const std::string &table_name);
  Partition* GetTablePartition(const std::string &table_name, const std::string &key);
  Partition* GetTablePartitionById(const std::string &table_name, const int partition_id);

  // TODO 
 // template <class VisitorFunction>
 // void WalkPartitions(VisitorFunction vfn) {
 //   slash::RWLock rl(&table_rw_, false);
 //   for_each(partitions_.begin(), partitions_.end(), vfn);
 // }

  void DumpTablePartitions();
  void DumpBinlogSendTask();
  
  // Peer Client
  Status SendToPeer(const Node &node, const client::SyncRequest &msg);
  
  // Backgroud thread
  void BGSaveTaskSchedule(void (*function)(void*), void* arg);
  void BGPurgeTaskSchedule(void (*function)(void*), void* arg);
  void AddSyncTask(Partition* partition);
  void AddMetacmdTask();
  Status AddBinlogSendTask(const std::string &table, int parititon_id, const Node& node,
      int32_t filenum, int64_t offset);
  Status RemoveBinlogSendTask(const std::string &table, int parititon_id, const Node& node);
  int32_t GetBinlogSendFilenum(const std::string &table, int partition_id, const Node& node);
  void DispatchBinlogBGWorker(ZPBinlogReceiveTask *task);


  // Command related
  Cmd* CmdGet(const int op) {
    return GetCmdFromTable(op, cmds_);
  }
  void DumpTableBinlogOffsets(const std::string &table_name,
                              std::unordered_map<std::string, std::vector<PartitionBinlogOffset>> &all_offset);

  // Statistic related
  bool GetAllTableName(std::vector<std::string>& table_names);
  bool GetTableStat(const std::string& table_name, std::vector<Statistic>& stats);
  bool GetTableCapacity(const std::string& table_name, std::vector<Statistic>& capacity_stats);

 private:

  slash::Mutex server_mutex_;
  std::unordered_map<int, Cmd*> cmds_;

  // Table and Partition
  //Note: this lock only protect table map, rather than certain partiton which should keep thread safety itself
  pthread_rwlock_t table_rw_;
  std::atomic<int> table_count_;
  std::unordered_map<std::string, Table*> tables_;

  // Binlog Send related
  slash::Mutex mutex_peers_;
  std::unordered_map<std::string, ZPPbCli*> peers_;
  ZPBinlogSendTaskPool binlog_send_pool_;
  std::vector<ZPBinlogSendThread*> binlog_send_workers_;

  // Server related
  int worker_num_;
  ZPDataWorkerThread* zp_worker_thread_[kMaxWorkerThread];
  ZPDataDispatchThread* zp_dispatch_thread_;
  ZPPingThread* zp_ping_thread_;
  ZPMetacmdBGWorker* zp_metacmd_bgworker_;
  ZPTrySyncThread* zp_trysync_thread_;
  ZPBinlogReceiverThread* zp_binlog_receiver_thread_;
  std::vector<ZPBinlogReceiveBgWorker*> zp_binlog_receive_bgworkers_;

  std::atomic<bool> should_exit_;

  // Meta State related
  pthread_rwlock_t meta_state_rw_;
  std::string meta_ip_;
  long meta_port_;
  
  slash::Mutex mutex_epoch_;
  int64_t meta_epoch_;
  bool should_pull_meta_;

  // Cmd related
  void InitClientCmdTable();

  // Background thread
  slash::Mutex bgsave_thread_protector_;
  pink::BGThread bgsave_thread_;
  slash::Mutex bgpurge_thread_protector_;
  pink::BGThread bgpurge_thread_;
  void DoTimingTask();
};

#endif
