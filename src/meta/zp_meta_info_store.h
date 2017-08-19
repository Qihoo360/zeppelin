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
#include <unordered_map>

#include "slash/include/slash_status.h"
#include "floyd/include/floyd.h"
#include "include/zp_meta.pb.h"

using slash::Status;

class ZPMetaInfoStoreSnap() {
 public:
   ZPMetaInfoStoreSnap()
   void UpNode(const std::string& ip_port);
   void DownNode(const std::string& ip_port);
   Status AddSlave(const std::string& table, int partition,
       const std::string& ip_port);
   Status DeleteSlave(const std::string& table, int partition,
       const std::string& ip_port);
   Status SetMaster(const std::string& table, int partition,
       const std::string& ip_port);
   void RefreshTableWithNodeAlive();

 private:
   friend class ZPMetaInfoStore;
   int snap_epoch_;
   std::unordered_map<std::string, ZPMeta::Table> tables_;
   std::unordered_map<std::string, ZPMeta::NodeStatus> nodes_;
   bool node_changed_;
   bool table_changed_;

   bool IsNodeUp(const ZPMeta::Node& node) const;
};

class ZPMetaInfoStore {
 public:
   explicit ZPMetaInfoStore(floyd::Floyd* floyd);

   int epoch() {
     return epoch_;
   }
   
   Status Refresh();
   Status RestoreNodeAlive();
   void FetchExpiredNode(std::set<std::string>* nodes);

   Status GetTablesForNode(const std::string& ip_port,
       std::set<std::string> *table_list) const;

   Status GetTableMeta(const std::string& table,
       ZPMeta::Table* table_meta) const;

   void GetSnapshot(ZPMetaInfoStoreSnap* snap);
   Status Apply(const ZPMetaInfoStoreSnap& snap)

 private:
   floyd::Floyd* floyd_;
   std::atomic<int> epoch_;
   // table => ZPMeta::Table set
   std::unordered_map<std::string, ZPMeta::Table> table_info_;
   // node => tables
   std::unordered_map<std::string, std::set<std::string> > node_table_;
   // node => alive time, 0 means already down node
   std::unordered_map<std::string, uint64_t> node_alive_;
   
   void AddNodeTable(const std::string& ip_port,
       const std::string& table);
   void NodesDebug();
   
   Status GetAllTables(
       std::unordered_map<std::string, ZPMeta::Table>* all_tables) const;

   Status GetAllNodes(
       std::unordered_map<std::string, ZPMeta::NodeStatus>* all_nodes) const;

   // No copying allowed
   ZPMetaInfoStore (const ZPMetaInfoStore&);
   void operator=(const ZPMetaInfoStore&);
};

extern std::string DiffKey(const ZPMeta::RelationCmdUnit& diff);

#endif  // SRC_META_ZP_META_INFO_STORE_H_
