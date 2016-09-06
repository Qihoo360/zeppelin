#ifndef ZP_DATA_PARTITION_H
#define ZP_DATA_PARTITION_H

#include <vector>
#include <memory>
#include <functional>

#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_command.h"

#include "nemo.h"

// TODO maybe need replica
// class Replica;
class Partition;

Partition* NewPartition(const std::string log_path, const std::string data_path, const int partition_id, const std::vector<Node> &nodes);

class Partition {
  friend class ZPBinlogSenderThread;
 public:
  Partition(const int partition_id, const std::string &log_path, const std::string &data_path);
  ~Partition();

  bool FindSlave(const Node& node);
  Status AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t con_offset);
  void DeleteSlave(int fd);
  void BecomeMaster();
  void BecomeSlave();
  bool ShouldTrySync();
  void TrySyncDone();
  void Init(const std::vector<Node> &nodes);
  void DoCommand(Cmd* cmd, google::protobuf::Message &req, google::protobuf::Message &res,
     const std::string &raw_msg);

 private:
  //TODO define PartitionOption if needed
  int partition_id_;
  std::string log_path_;
  std::string data_path_;
  Node master_node_;
  std::vector<Node> slave_nodes_;

  // State related
  pthread_rwlock_t state_rw_;
  int role_;
  int repl_state_;  

  Binlog* logger_;
  std::shared_ptr<nemo::Nemo> db_;

  slash::RecordMutex mutex_record_;
  pthread_rwlock_t partition_rw_;
  
  // Binlog senders
  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;

  std::string NewPartitionPath(const std::string& name, const uint32_t current);
  void WriteBinlog(const std::string &content);
  
  Partition(const Partition&);
  void operator=(const Partition&);
};

#endif
