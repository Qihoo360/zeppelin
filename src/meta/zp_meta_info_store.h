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
#ifndef SRC_META_ZP_META_INFO_STORE_H_
#define SRC_META_ZP_META_INFO_STORE_H_
#include <set>
#include <string>
#include <atomic>
#include <unordered_map>

#include "slash/include/env.h"
#include "slash/include/slash_status.h"
#include "floyd/include/floyd.h"
#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_node_offset.h"

using slash::Status;

extern std::string NodeOffsetKey(const std::string& table,
    int partition_id);

struct NodeInfo {
  uint64_t last_alive_time;
  // table_partition -> offset
  std::map<std::string, NodeOffset> offsets;
  
  NodeInfo()
    : last_alive_time(0) {}
  
  explicit NodeInfo(bool up)
    : last_alive_time(0) {
    if (up) {
      last_alive_time = slash::NowMicros();
    }
  }

  Status GetOffset(const std::string& table, int partition_id,
      NodeOffset* noffset) const;
};

class ZPMetaInfoStoreSnap {
 public:
   ZPMetaInfoStoreSnap();
   Status UpNode(const std::string& ip_port);
   Status DownNode(const std::string& ip_port);
   Status AddSlave(const std::string& table, int partition,
       const std::string& ip_port);
   Status DeleteSlave(const std::string& table, int partition,
       const std::string& ip_port);
   Status DeleteDup(const std::string& table, int partition,
       const std::string& ip_port);
   Status SetMaster(const std::string& table, int partition,
       const std::string& ip_port);
   Status SetStuck(const std::string& table, int partition);
   Status AddTable(const std::string& table, int num);
   Status RemoveTable(const std::string& table);
   void RefreshTableWithNodeAlive();

 private:
   friend class ZPMetaInfoStore;
   int snap_epoch_;
   std::unordered_map<std::string, ZPMeta::Table> tables_;
   std::unordered_map<std::string, NodeInfo> nodes_;
   bool node_changed_;
   std::unordered_map<std::string, bool> table_changed_;
   void SerializeNodes(ZPMeta::Nodes* nodes_ptr) const;

   bool IsNodeUp(const ZPMeta::Node& node) const;
   Status GetNodeOffset(const ZPMeta::Node& node,
       const std::string& table, int partition_id,
       NodeOffset* noffset) const;
};

class ZPMetaInfoStore {
 public:
   explicit ZPMetaInfoStore(floyd::Floyd* floyd);

   int epoch() {
     return epoch_;
   }
   
   Status Refresh();
   Status RestoreNodeInfos();
   bool UpdateNodeInfo(const ZPMeta::MetaCmd_Ping &ping);
   void FetchExpiredNode(std::set<std::string>* nodes);

   Status GetTableList(std::set<std::string>* table_list) const;
   Status GetTablesForNode(const std::string& ip_port,
       std::set<std::string>* table_list) const;

   Status GetTableMeta(const std::string& table,
       ZPMeta::Table* table_meta) const;

   void GetSnapshot(ZPMetaInfoStoreSnap* snap);
   Status Apply(const ZPMetaInfoStoreSnap& snap);
   
   void GetAllNodes(
       std::unordered_map<std::string, NodeInfo>* all_nodes) const;
   
   Status GetPartitionMaster(const std::string& table,
       int partition, ZPMeta::Node* master);

   Status GetNodeOffset(const ZPMeta::Node& node,
       const std::string& table, int partition_id,
       NodeOffset* noffset) const;

 private:
   floyd::Floyd* floyd_;
   std::atomic<int> epoch_;
   // table => ZPMeta::Table set
   std::unordered_map<std::string, ZPMeta::Table> table_info_;
   // node => tables
   std::unordered_map<std::string, std::set<std::string> > node_table_;
   // node => alive time + offset set, 0 means already down node
   std::unordered_map<std::string, NodeInfo> node_infos_;
   
   void AddNodeTable(const std::string& ip_port,
       const std::string& table);
   void NodesDebug();
   
   void GetAllTables(
       std::unordered_map<std::string, ZPMeta::Table>* all_tables) const;


   // No copying allowed
   ZPMetaInfoStore (const ZPMetaInfoStore&);
   void operator=(const ZPMetaInfoStore&);
};

extern std::string DiffKey(const ZPMeta::RelationCmdUnit& diff);

#endif  // SRC_META_ZP_META_INFO_STORE_H_
