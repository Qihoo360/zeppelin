#ifndef ZP_DATA_TABLE_H
#define ZP_DATA_TABLE_H

#include <atomic>
#include <vector>
#include <memory>
//#include <functional>
#include <unordered_set>

#include "zp_const.h"
#include "client.pb.h"
#include "zp_meta.pb.h"
//#include "zp_conf.h"
#include "zp_meta_utils.h"
//#include "zp_command.h"


class Partition;
class Table;

Table* NewTable(const std::string &table_name, const std::string log_path, const std::string data_path);

class Table {
 public:
  Table(const std::string& table_name,
      const std::string &log_path, const std::string &data_path);
  ~Table();

  bool SetPartitionCount(int count);
  Partition* GetPartition(const std::string &key);
  Partition* GetPartitionById(const int partition_id);
  bool UpdateOrAddPartition(int partition_id, ZPMeta::PState state,
      const Node& master, const std::vector<Node>& slaves);
  
  uint32_t KeyToPartition(const std::string &key);

  void Dump();
  void DoTimingTask();

 private:
  std::string table_name_;
  std::string log_path_;
  std::string data_path_;

  //std::string sync_path_;
  //std::string bgsave_path_;
  //std::atomic<bool> readonly_;

  pthread_rwlock_t partition_rw_;
  std::atomic<int> partition_cnt_;
  std::map<int, Partition*> partitions_;

  Table(const Table&);
  void operator=(const Table&);
};

#endif
