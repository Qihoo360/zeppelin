#ifndef ZP_DATA_SERVER_H
#define ZP_DATA_SERVER_H

#include <string>
#include <memory>
#include <unordered_set>

#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_const.h"
#include "zp_metacmd_worker_thread.h"
#include "zp_ping_thread.h"
#include "zp_trysync_thread.h"
#include "zp_binlog_sender_thread.h"
#include "zp_binlog_receiver_thread.h"

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
  
  //Status Set(const std::string &key, const std::string &value);

  const std::shared_ptr<nemo::Nemo> db() {
    return db_;
  }

  bool is_master() {
    slash::RWLock l(&state_rw_, true);
    return role_ == Role::kNodeMaster;
  }
  std::string master_ip() {
    slash::RWLock l(&state_rw_, false);
    return master_ip_;
  }
  int master_port() {
    slash::RWLock l(&state_rw_, false);
    return master_port_;
  }
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
  std::string bgsave_prefix() {
    return "";
  }

  bool readonly() {
    return readonly_;
  }
  void Exit() {
    should_exit_ = true;
  }

  ZPMetacmdWorkerThread* zp_metacmd_worker_thread() {
    return zp_metacmd_worker_thread_;
  };

  ZPBinlogReceiverThread* zp_binlog_receiver_thread() {
    return zp_binlog_receiver_thread_;
  };

  bool FindSlave(const Node& node);
  Status AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t con_offset);
  void DeleteSlave(int fd);
  void BecomeMaster();
  void BecomeSlave(const std::string& master_ip, int port);
  bool ShouldSync();
  void SyncDone();

  // DBSync used
  struct DBSyncArg {
    ZPDataServer *p;
    std::string ip;
    int port;
    DBSyncArg(ZPDataServer *_p, const std::string& _ip, int &_port)
      : p(_p), ip(_ip), port(_port) {}
  };
  void DBSyncSendFile(const std::string& ip, int port);

  // BGSave used
  bool ShouldWaitDBSync();
  void SetWaitDBSync();
  void WaitDBSyncDone();
  bool ChangeDb(const std::string& new_path);
  struct BGSaveInfo {
    bool bgsaving;
    time_t start_time;
    std::string s_start_time;
    std::string path;
    uint32_t filenum;
    uint64_t offset;
    BGSaveInfo() : bgsaving(false), filenum(0), offset(0){}
    void Clear() {
      bgsaving = false;
      path.clear();
      filenum = 0;
      offset = 0;
    }
  };
  BGSaveInfo bgsave_info() {
    slash::MutexLock l(&bgsave_protector_);
    return bgsave_info_;
  }
  bool bgsaving() {
    slash::MutexLock l(&bgsave_protector_);
    return bgsave_info_.bgsaving;
  }
  void Bgsave();
  bool Bgsaveoff();
  bool RunBgsaveEngine(const std::string path);
  void FinishBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.bgsaving = false;
  }

  bool ShouldJoinMeta();
  void PlusMetaServerConns();
  void MinusMetaServerConns();
  void PickMeta();

  Binlog* logger_;

  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;

  slash::RecordMutex mutex_record_;

  slash::Mutex server_mutex_;

  pthread_rwlock_t server_rw_;

 private:

  ZPOptions options_;


  // DB
  std::shared_ptr<nemo::Nemo> db_;

  // Server related
  int worker_num_;
  ZPDataWorkerThread* zp_worker_thread_[kMaxWorkerThread];
  ZPDataDispatchThread* zp_dispatch_thread_;
  ZPPingThread* zp_ping_thread_;
  ZPMetacmdWorkerThread* zp_metacmd_worker_thread_;
  ZPBinlogReceiverThread* zp_binlog_receiver_thread_;
  ZPTrySyncThread* zp_trysync_thread_;

  // State related
  pthread_rwlock_t state_rw_;
  int role_;
  int repl_state_;  
  std::string master_ip_;
  int master_port_;
  std::atomic<bool> readonly_;
  std::atomic<bool> should_exit_;

  // Meta State related
  pthread_rwlock_t meta_state_rw_;
  std::atomic<bool> should_rejoin_;
  int meta_state_;
  int meta_server_conns_;
  std::string meta_ip_;
  long meta_port_;

  // BGSave related
  slash::Mutex bgsave_protector_;
  pink::BGThread bgsave_thread_;
  nemo::BackupEngine *bgsave_engine_;
  BGSaveInfo bgsave_info_;
  
  static void DoBgsave(void* arg);
  bool InitBgsaveEnv();
  bool InitBgsaveEngine();
  void ClearBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.Clear();
  }

  // Purgelogs use
  std::atomic<bool> purging_;
  pink::BGThread purge_thread_;
  static void DoPurgeDir(void* arg);
  void PurgeDir(std::string& path);

  bool FlushAll();

  // DBSync use
  int db_sync_speed_ = 126;
  slash::Mutex db_sync_protector_;
  std::unordered_set<std::string> db_sync_slaves_;
  void TryDBSync(const std::string& ip, int port, int32_t top);
  void DBSync(const std::string& ip, int port);
  static void DoDBSync(void* arg);

};

#endif
