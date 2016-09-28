#ifndef ZP_DATA_SERVER_H
#define ZP_DATA_SERVER_H

#include <string>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_const.h"
#include "zp_metacmd_thread.h"
#include "zp_ping_thread.h"
#include "zp_trysync_thread.h"
#include "zp_binlog_sender_thread.h"
#include "zp_binlog_receiver_thread.h"
#include "zp_data_partition.h"

#include "bg_thread.h"
#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

#include "slash_status.h"
#include "slash_mutex.h"

#include "nemo.h"
#include "nemo_backupable.h"

using slash::Status;

class ZPDataServer;
class ZPDataServerConn;
class ZPDataServerThread;

class ZPDataWorkerThread;
class ZPDataDispatchThread;


class ZPDataServer {
 public:

  explicit ZPDataServer(const ZPOptions& option);
  virtual ~ZPDataServer();
  Status Start();
  
  std::string meta_ip() {
    return meta_ip_;
  }
  int meta_port() {
    return meta_port_;
  }
  std::string local_ip() {
    return options_.local_ip;
  }
  int local_port() {
    return options_.local_port;
  }

  std::string db_sync_path() {
    return "./sync_" + std::to_string(options_.local_port) + "/";
  }

  std::string bgsave_path() {
    return options_.data_path + "/dump/";
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


  bool ShouldJoinMeta();
  void PlusMetaServerConns();
  void MinusMetaServerConns();
  void PickMeta();
  int64_t meta_epoch() {
    slash::MutexLock l(&mutex_epoch_);
    return meta_epoch_;
  }
  void UpdateEpoch(int64_t epoch);
  bool ShouldPullMeta() {
    slash::MutexLock l(&mutex_epoch_);
    return should_pull_meta_;
  }
  void FinishPullMeta() {
    slash::MutexLock l(&mutex_epoch_);
    should_pull_meta_ = false;
  }


  slash::Mutex server_mutex_;

  pthread_rwlock_t server_rw_;
  
  // Partition
  bool UpdateOrAddPartition(const int partition_id, const std::vector<Node>& nodes);
  Partition* GetPartition(const std::string &key);
  Partition* GetPartitionById(const int partition_id);
  template <class VisitorFunction>
  void WalkPartitions(VisitorFunction vfn) {
    slash::RWLock rl(&partition_rw_, false);
    for_each(partitions_.begin(), partitions_.end(), vfn);
  }

  void DumpPartitions();
  
  // Peer Client
  Status SendToPeer(const std::string &peer_ip, int peer_port, const std::string &data);

 private:

  ZPOptions options_;

  // Partitions
  pthread_rwlock_t partition_rw_;
  std::map<int, Partition*> partitions_;
  uint32_t KeyToPartition(const std::string &key);

  // Peer Client
  slash::Mutex mutex_peers_;
  std::unordered_map<std::string, ZPPbCli*> peers_;

  // Server related
  int worker_num_;
  ZPDataWorkerThread* zp_worker_thread_[kMaxWorkerThread];
  ZPDataDispatchThread* zp_dispatch_thread_;
  ZPPingThread* zp_ping_thread_;
  ZPMetacmdThread* zp_metacmd_thread_;
  //ZPMetacmdWorkerThread* zp_metacmd_worker_thread_;
  ZPBinlogReceiverThread* zp_binlog_receiver_thread_;
  ZPTrySyncThread* zp_trysync_thread_;

  std::atomic<bool> should_exit_;

  // Meta State related
  pthread_rwlock_t meta_state_rw_;
  std::atomic<bool> should_rejoin_;
  int meta_state_;
  int meta_server_conns_;
  std::string meta_ip_;
  long meta_port_;
  
  slash::Mutex mutex_epoch_;
  int64_t meta_epoch_;
  bool should_pull_meta_;

  // Purgelogs use
  std::atomic<bool> purging_;
  pink::BGThread purge_thread_;
  static void DoPurgeDir(void* arg);
  void PurgeDir(std::string& path);
  bool FlushAll();
};

#endif
