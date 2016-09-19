#ifndef ZP_DATA_PARTITION_H
#define ZP_DATA_PARTITION_H

#include <vector>
#include <memory>
#include <functional>

#include "zp_const.h"
#include "client.pb.h"
#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_command.h"

#include "bg_thread.h"
#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

#include "nemo.h"
#include "nemo_backupable.h"

// TODO maybe need replica
// class Replica;
class Partition;

Partition* NewPartition(const std::string log_path, const std::string data_path, const int partition_id, const std::vector<Node> &nodes);

class Partition {
  friend class ZPBinlogSenderThread;
 public:
  Partition(const int partition_id, const std::string &log_path, const std::string &data_path);
  ~Partition();
  
  bool readonly() {
    return readonly_;
  }

  bool FindSlave(const Node& node);
  Status AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t con_offset);
  void DeleteSlave(int fd);
  void BecomeMaster();
  void BecomeSlave();
  bool ShouldTrySync();
  void TrySyncDone();
  bool TryUpdateMasterOffset();

  bool ShouldWaitDBSync();
  void SetWaitDBSync();
  void WaitDBSyncDone();

  // TODO combine Update and Init
  void Init(const std::vector<Node> &nodes);
  void Update(const std::vector<Node> &nodes);

  void DoBinlogCommand(Cmd* cmd, client::CmdRequest &req, client::CmdResponse &res, const std::string &raw_msg);
  void DoCommand(Cmd* cmd, client::CmdRequest &req, client::CmdResponse &res,
     const std::string &raw_msg, bool is_from_binlog = false);

  int partition_id() {
    slash::RWLock l(&state_rw_, false);
    return partition_id_;
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

  // BGSave used
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
  std::string bgsave_prefix() {
    return "";
  }
  void Bgsave();
  bool Bgsaveoff();
  bool RunBgsaveEngine(const std::string path);
  void FinishBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.bgsaving = false;
  }

  Binlog* logger_;

  // Binlog senders
  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;

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


  // State related
  pthread_rwlock_t state_rw_;
  int role_;
  int repl_state_;  

  std::shared_ptr<nemo::Nemo> db_;

  slash::RecordMutex mutex_record_;
  pthread_rwlock_t partition_rw_;
  

  std::string NewPartitionPath(const std::string& name, const uint32_t current);
  void WriteBinlog(const std::string &content);
  
  Partition(const Partition&);
  void operator=(const Partition&);
};

#endif
