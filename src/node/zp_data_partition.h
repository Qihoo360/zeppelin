#ifndef ZP_DATA_PARTITION_H
#define ZP_DATA_PARTITION_H

#include <memory>
#include <functional>
#include <unordered_set>
#include <set>

#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"
#include "nemo.h"
#include "nemo_backupable.h"


#include "zp_const.h"
#include "client.pb.h"
#include "zp_conf.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_command.h"


class Partition;
std::string NewPartitionPath(const std::string& name, const uint32_t current);
Partition* NewPartition(const std::string &table_name, const std::string& log_path, const std::string& data_path,
                        const int partition_id, const Node& master, const std::set<Node> &slaves);

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

struct PartitionBinlogOffset {
  int partition_id;
  uint32_t filenum;
  uint64_t offset;
};

struct PartitionSyncOption {
  client::SyncType type;
  std::string table_name;
  uint32_t partition_id;
  std::string from_node;
  uint32_t filenum;
  uint64_t offset;
  PartitionSyncOption(
      client::SyncType t,
      std::string table,
      uint32_t id,
      const std::string& from,
      uint32_t arg_filenum,
      uint64_t arg_offset)
    : type(t),
    table_name(table),
    partition_id(id),
    from_node(from),
    filenum(arg_filenum),
    offset(arg_offset) {}
};

class Partition {
  public:
  Partition(const std::string &table_name, const int partition_id,
      const std::string &log_path, const std::string &data_path);
  ~Partition();

  int partition_id() const {
    return partition_id_;
  }
  std::string sync_path() const {
    return sync_path_;
  }
  std::string table_name() const {
    return table_name_;
  }

  const std::shared_ptr<nemo::Nemo> db() const {
    return db_;
  }
  Node master_node() {
    slash::RWLock l(&state_rw_, false);
    return master_node_;
  }

  Status Init();

  // Command related
  void DoBinlogCommand(const PartitionSyncOption& option,
      const Cmd* cmd, const client::CmdRequest &req);
  void DoCommand(const Cmd* cmd, const client::CmdRequest &req,
      client::CmdResponse &res);
  void DoBinlogSkip(const PartitionSyncOption& option, uint64_t gap);

  // Status related
  bool ShouldTrySync();
  void TrySyncDone();
  bool TryUpdateMasterOffset();
  bool ShouldWaitDBSync();
  void SetWaitDBSync();
  void WaitDBSyncDone();

  // Partition node related
  void Update(ZPMeta::PState state, const Node& master, const std::set<Node> &slaves);
  void Leave();

  // Binlog related
  Status SlaveAskSync(const Node &node, uint32_t filenum, uint64_t offset);
  void GetBinlogOffset(uint32_t* filenum, uint64_t* pro_offset) const {
    logger_->GetProducerStatus(filenum, pro_offset);
  }
  Status SetBinlogOffset(uint32_t filenum, uint64_t offset);
  std::string GetBinlogFilename() {
    return logger_->filename();
  }

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
  bool RunBgsaveEngine();
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
  void Dump();

  // State related
  bool GetWinBinlogOffset(uint32_t* filenum, uint64_t* offset);
  
  void DoTimingTask();

 private:
  std::string table_name_;
  int partition_id_;
  std::string log_path_;
  std::string data_path_;
  std::string sync_path_;
  std::string bgsave_path_;

  
  // State related
  pthread_rwlock_t state_rw_; //protect partition status below
  Node master_node_;
  std::set<Node> slave_nodes_;
  std::atomic<bool> readonly_;
  ZPMeta::PState pstate_;
  Role role_;
  int repl_state_;
  uint32_t win_filenum_;
  uint64_t win_offset_;
  void CleanSlaves(const std::set<Node> &old_slaves);
  void BecomeSingle();
  void BecomeMaster();
  void BecomeSlave();
  ZPMeta::PState UpdateState(ZPMeta::PState state);
  bool CheckSyncOption(const PartitionSyncOption& option);

  // DB related
  std::shared_ptr<nemo::Nemo> db_;
  bool FlushAll();

  // Binlog related
  Binlog* logger_;
  bool CheckBinlogFiles(); // Check binlog availible and update purge_index_

  // DoCommand related
  slash::RecordMutex mutex_record_;
  pthread_rwlock_t partition_rw_; // Some command use partition_rw to suspend others

  // Recover sync related
  std::atomic<bool> do_recovery_sync_;
  std::atomic<int> recover_sync_flag_;
  void TryRecoverSync();
  void CancelRecoverSync();
  void MaybeRecoverSync();

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
  // protect purge index between purge thread and trysync command
  // Notice purged_index_rw_ should lock after state_rw_
  pthread_rwlock_t purged_index_rw_; 
  uint32_t purged_index_; // binlog before which has or will be purged
  bool GetBinlogFiles(std::map<uint32_t, std::string>& binlogs);
  static void DoPurgeLogs(void* arg);
  bool CouldPurge(uint32_t index);

  Partition(const Partition&);
  void operator=(const Partition&);
};

#endif
