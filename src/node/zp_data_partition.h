#ifndef ZP_DATA_PARTITION_H
#define ZP_DATA_PARTITION_H

#include <vector>
#include <memory>

#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"

#include "nemo.h"

// TODO maybe need replica
// class Replica;
class Partition;

//class PartitionManager {
// public:
//
//  PartitionManager(const ZPOptions& options)
//      : options_(options) {}
//
//  ~PartitionManager();
//
//  // Add partition; the first node is master node;
//  bool AddPartition(const int partition_id, const std::vector<Node>& nodes);
//
//
// private:
//
//  ZPOptions options_;
//  std::vector<Partition*> partitions_;
//  std::map<int, Partition*> partition_index_;
//
//  // No copying allowed
//  PartitionManager(const PartitionManager&);
//  void operator=(const PartitionManager&);
//};

Partition* NewPartition(const std::string log_path, const std::string data_path, const int partition_id, const std::vector<Node> &nodes);

class Partition {
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


 private:
  //TODO define PartitionOption if needed
  int partition_id_;
  std::string log_path_;
  std::string data_path_;
  Node master_node_;
  Node slave_nodes_[kReplicaNum - 1];

  // State related
  pthread_rwlock_t state_rw_;
  int role_;
  int repl_state_;  

  Binlog* logger_;
  std::shared_ptr<nemo::Nemo> db_;
  
  // Binlog senders
  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;

  std::string NewPartitionPath(const std::string& name, const uint32_t current);
  //slash::RecordMutex mutex_record_;
  Partition(const Partition&);
  void operator=(const Partition&);
};

#endif
