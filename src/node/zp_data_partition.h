#ifndef ZP_DATA_PARTITION_H
#define ZP_DATA_PARTITION_H

#include <vector>
#include <memory>
#include <functional>
#include <unordered_set>

#include "zp_const.h"
#include "client.pb.h"
#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_command.h"

#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

#include "nemo.h"
#include "nemo_backupable.h"

class Partition;
std::string NewPartitionPath(const std::string& name, const uint32_t current);
Partition* NewPartition(const std::string log_path, const std::string data_path, const int partition_id, const Node& master, const std::vector<Node> &slaves);

// Slave item
struct SlaveItem {
  Node node;
  pthread_t sender_tid;
  int sync_fd;
  void* sender;
  struct timeval create_time;

  SlaveItem()
    : node(),
    sender(NULL) {}

  SlaveItem(const SlaveItem& item)
    : node(item.node),
    sender_tid(item.sender_tid),
    sender(item.sender),
    create_time(item.create_time) {
    }
};

class Partition {
  friend class ZPBinlogSenderThread;
  public:
  Partition(const int partition_id, const std::string &log_path, const std::string &data_path);
  ~Partition();

  bool readonly() {
    return readonly_;
  }
  int partition_id() {
    return partition_id_;
  }
  std::string sync_path() {
    return sync_path_;
  }
  const std::shared_ptr<nemo::Nemo> db() {
    return db_;
  }
  Node master_node() {
    slash::RWLock l(&state_rw_, false);
    return master_node_;
  }
  Role role() {
    slash::RWLock l(&state_rw_, false);
    return role_;
  }

  // Command related
  void DoBinlogCommand(const Cmd* cmd, const client::CmdRequest &req,
      const std::string &raw_msg);
  void DoCommand(const Cmd* cmd, const client::CmdRequest &req,
      client::CmdResponse &res, const std::string &raw_msg);

  // Status related
  bool ShouldTrySync();
  void TrySyncDone();
  bool TryUpdateMasterOffset();
  bool ShouldWaitDBSync();
  void SetWaitDBSync();
  void WaitDBSyncDone();

  // Partition node related
  void Update(const Node& master, const std::vector<Node> &slaves);

  // Binlog related
  Status AddBinlogSender(const Node &node, uint32_t filenum, uint64_t offset);
  void GetBinlogOffset(uint32_t* filenum, uint64_t* pro_offset) {
    logger_->GetProducerStatus(filenum, pro_offset);
  }
  std::string GetBinlogFilename() {
    return logger_->filename;
  }
  bool CheckBinlogFiles(); // Check binlog availible and update purge_index_

  // BGSave related
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
  bool RunBgsaveEngine(const std::string path);
  void FinishBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.bgsaving = false;
  }
  BGSaveInfo bgsave_info() {
    slash::MutexLock l(&bgsave_protector_);
    return bgsave_info_;
  }

  // DBSync related
  struct DBSyncArg {
    Partition *p;
    std::string ip;
    int port;
    DBSyncArg(Partition *_p, const std::string& _ip, int &_port)
      : p(_p), ip(_ip), port(_port) {}
  };
  void DBSyncSendFile(const std::string& ip, int port);
  
  // Purge binlog related
  struct PurgeArg {
    Partition *p;
    uint32_t to;
    bool manual;
  };
  bool PurgeLogs(uint32_t to, bool manual);
  bool PurgeFiles(uint32_t to, bool manual);
  void ClearPurge() {
    purging_ = false;
  }
  void AutoPurge();
  void Dump();

 private:
  //TODO define PartitionOption if needed
  int partition_id_;
  std::string log_path_;
  std::string data_path_;
  std::string sync_path_;
  std::string bgsave_path_;
  Node master_node_;
  std::vector<Node> slave_nodes_;
  std::atomic<bool> readonly_;
  
  // State related
  pthread_rwlock_t state_rw_;
  Role role_;
  int repl_state_;
  void CleanRoleEnv(Role role);
  void BecomeSingle();
  void BecomeMaster();
  void BecomeSlave();

  // DB related
  std::shared_ptr<nemo::Nemo> db_;
  bool FlushAll();

  // Binlog related
  Binlog* logger_;
  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;
  bool FindSlave(const Node& node);
  void DeleteSlave(const Node& node);
  void WriteBinlog(const std::string &content);

  // DoCommand related
  slash::RecordMutex mutex_record_;
  pthread_rwlock_t partition_rw_;

  // BGSave related
  slash::Mutex bgsave_protector_;
  nemo::BackupEngine *bgsave_engine_;
  BGSaveInfo bgsave_info_;
  void Bgsave();
  bool Bgsaveoff();
  static void DoBgsave(void* arg);
  bool InitBgsaveEnv();
  bool InitBgsaveEngine();
  void ClearBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.Clear();
  }
  bool bgsaving() {
    slash::MutexLock l(&bgsave_protector_);
    return bgsave_info_.bgsaving;
  }
  std::string bgsave_prefix() {
    return "";
  }

  // DBSync related
  slash::Mutex db_sync_protector_;
  std::unordered_set<std::string> db_sync_slaves_;
  void TryDBSync(const std::string& ip, int port, int32_t top);
  void DBSync(const std::string& ip, int port);
  static void DoDBSync(void* arg);
  bool ChangeDb(const std::string& new_path);

  // Purge binlog related
  std::atomic<bool> purging_;
  uint32_t purged_index_; // binlog before which has or will be purged
  bool GetBinlogFiles(std::map<uint32_t, std::string>& binlogs);
  static void DoPurgeLogs(void* arg);
  bool CouldPurge(uint32_t index);

  Partition(const Partition&);
  void operator=(const Partition&);
};

#endif
