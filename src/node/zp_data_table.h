// Copyright 2017 Qihoo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef SRC_NODE_ZP_DATA_TABLE_H_
#define SRC_NODE_ZP_DATA_TABLE_H_

#include <set>
#include <map>
#include <atomic>
#include <memory>
#include <string>

#include "include/zp_util.h"
#include "include/zp_const.h"
#include "src/meta/zp_meta.pb.h"
#include "src/node/client.pb.h"
#include "src/node/zp_data_entity.h"

class Table;
class Partition;
class BinlogOffset;

std::shared_ptr<Table> NewTable(const std::string& table_name,
    const std::string& log_path, const std::string& data_path,
    const std::string& trash_path);

class Table  {
 public:
  Table(const std::string& table_name, const std::string& log_path,
      const std::string& data_path, const std::string& trash_path);
  ~Table();
  int partition_cnt() {
    return partition_cnt_;
  }

  bool SetPartitionCount(int count);
  std::shared_ptr<Partition> GetPartition(const std::string &key);
  std::shared_ptr<Partition> GetPartitionById(const int partition_id);
  bool UpdateOrAddPartition(int partition_id, ZPMeta::PState state,
      const Node& master, const std::set<Node>& slaves);
  void LeaveAllPartition();
  void LeavePartition(int pid);

  uint32_t KeyToPartition(const std::string &key);

  void Dump();
  void DoTimingTask();
  void DumpPartitionBinlogOffsets(std::map<int, BinlogOffset> *offset);
  void GetCapacity(Statistic *stat);
  void GetReplInfo(client::CmdResponse_InfoRepl* repl_info);

 private:
  std::string table_name_;
  std::string log_path_;
  std::string data_path_;
  std::string trash_path_;

  std::atomic<int> partition_cnt_;
  pthread_rwlock_t partition_rw_;
  std::map<int, std::shared_ptr<Partition>> partitions_;

  Table(const Table&);
  void operator=(const Table&);
};

#endif  // SRC_NODE_ZP_DATA_TABLE_H_
