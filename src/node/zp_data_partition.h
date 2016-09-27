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
Partition* NewPartition(const std::string log_path, const std::string data_path, const int partition_id, const std::vector<Node> &nodes);

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
  bool is_master() {
    slash::RWLock l(&state_rw_, false);
    return role_ == Role::kNodeMaster;
  }

  // Command related
  void DoBinlogCommand(Cmd* cmd, client::CmdRequest &req, client::CmdResponse &res, const std::string &raw_msg);
  void DoCommand(Cmd* cmd, client::CmdRequest &req, client::CmdResponse &res,
      const std::string &raw_msg, bool is_from_binlog = false);

  // Status related
  bool FindSlave(const Node& node);
  void DeleteSlave(int fd);
  void BecomeMaster();
  void BecomeSlave();
  bool ShouldTrySync();
  void TrySyncDone();
  bool TryUpdateMasterOffset();
  bool ShouldWaitDBSync();
  void SetWaitDBSync();
  void WaitDBSyncDone();

  // Partition node related
  void Init(const std::vector<Node> &nodes);
  void Update(const std::vector<Node> &nodes);

  // Binlog related
  Status AddBinlogSender(const Node &node, uint32_t filenum, uint64_t offset);
  void GetBinlogOffset(uint32_t* filenum, uint64_t* pro_offset) {
    logger_->GetProducerStatus(filenum, pro_offset);
  }
  std::string GetBinlogFilename() {
    return logger_->filename;
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
  int role_;
  int repl_state_;

  // DB related
  std::shared_ptr<nemo::Nemo> db_;
  bool FlushAll();

  // Binlog related
  Binlog* logger_;
  void WriteBinlog(const std::string &content);
  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;

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

  Partition(const Partition&);
  void operator=(const Partition&);
};

#endif
