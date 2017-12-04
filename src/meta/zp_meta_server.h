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
#ifndef SRC_META_ZP_META_SERVER_H_
#define SRC_META_ZP_META_SERVER_H_
#include <stdio.h>
#include <string>
#include <unordered_map>
#include <set>
#include <atomic>
#include <vector>

#include "pink/include/server_thread.h"
#include "pink/include/pink_cli.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "floyd/include/floyd.h"

#include "include/zp_conf.h"
#include "include/zp_const.h"
#include "src/meta/zp_meta_command.h"
#include "src/meta/zp_meta_client_conn.h"
#include "src/meta/zp_meta_info_store.h"

extern ZpConf* g_zp_conf;

using slash::Status;
typedef std::unordered_map<std::string, struct timeval> NodeAliveMap;

class ZPMetaUpdateThread;
class ZPMetaConditionCron;
class ZPMetaElection;
class ZPMetaInfoStore;
class ZPMetaMigrateRegister;

enum MetaRole {
  kNone = 0,
  kLeader,
  kFollower,
};

const std::string MetaRoleMsg[] {
  "kMetaNone",
  "kMetaLeader",
  "kNodeFollower"
};

struct LeaderJoint {
  slash::Mutex mutex;
  pink::PinkCli* cli;
  std::string ip;
  int port;
  LeaderJoint()
    : cli(NULL),
    port(0) {
    }

  bool NoLeader() {
    return ip.empty() || port == 0;
  }

  void Disconnect() {
    if (cli) {
      cli->Close();
      delete cli;
      cli = NULL;
    }
  }

  void CleanLeader() {
    Disconnect();
    ip.clear();
    port = 0;
  }
};

struct QueryStatistic {
  std::atomic<uint64_t> last_query_num;
  std::atomic<uint64_t> query_num;
  std::atomic<uint64_t> last_qps;
  std::atomic<uint64_t> last_time_us;

  QueryStatistic()
    : last_query_num(0),
    query_num(0),
    last_qps(0),
    last_time_us(0) {}
};

class ZPMetaServer  {
 public:
  ZPMetaServer();
  virtual ~ZPMetaServer();

  // Server related
  void Start();
  void Stop() {
    should_exit_ = true;
  }

  // Required info_store_ be initialized
  int epoch() {
    return info_store_->epoch();
  }

  void PlusQueryNum() {
    statistic.query_num++;
  }

  // Cmd related
  Cmd* GetCmd(const int op);

  // Node alive related
  void UpdateNodeInfo(const ZPMeta::MetaCmd_Ping &ping);

  // Node info related
  Status GetMetaInfoByTable(const std::string& table,
      ZPMeta::MetaCmdResponse_Pull *ms_info);
  Status GetMetaInfoByNode(const std::string& ip_port,
      ZPMeta::MetaCmdResponse_Pull *ms_info);
  Status GetTableList(std::set<std::string>* table_list);
  Status GetNodeStatusList(
      std::unordered_map<std::string, NodeInfo>* node_infos);
  Status WaitSetMaster(const ZPMeta::Node& node,
      const std::string table, int partition);

  // Meta info related
  Status CreateTable(const ZPMeta::Table& table);
  Status DropTable(const std::string& table);
  Status AddPartitionSlave(const std::string& table, int pnum,
      const ZPMeta::Node& node);
  Status RemovePartitionSlave(const std::string& table, int pnum,
      const ZPMeta::Node& node);
  Status GetAllMetaNodes(ZPMeta::MetaCmdResponse_ListMeta *nodes);
  Status GetMetaStatus(ZPMeta::MetaCmdResponse_MetaStatus* ms);
  bool IsCharged(const std::string& table, int pnum, const ZPMeta::Node& node);
  Status RemoveNodes(const ZPMeta::MetaCmd_RemoveNodes& remove_nodes_cmd);

  // Migrate related
  Status Migrate(int epoch, const std::vector<ZPMeta::RelationCmdUnit>& diffs);
  Status CancelMigrate();
 
  // Leader related
  Status RedirectToLeader(const ZPMeta::MetaCmd &request,
      ZPMeta::MetaCmdResponse *response);
  bool IsLeader() {
    return role_ == MetaRole::kLeader;
  }

  bool Available() {
    return role_ != MetaRole::kNone;
  }


 private:
  // Server related
  std::atomic<bool> should_exit_;
  pink::ServerThread* server_thread_;
  ZPMetaClientConnFactory* conn_factory_;
  ZPMetaUpdateThread* update_thread_;
  ZPMetaConditionCron* condition_cron_;
  void DoTimingTask();

  // Floyd related
  floyd::Floyd* floyd_;
  Status OpenFloyd();

  // Leader related
  ZPMetaElection* election_;
  std::atomic<int> role_;
  slash::Mutex leader_mutex_;
  LeaderJoint leader_joint_;
  bool GetLeader(std::string *ip, int *port, bool is_retry = false);
  Status RefreshLeader();

  // Cmd related
  std::unordered_map<int, Cmd*> cmds_;
  void InitClientCmdTable();

  // Info related
  ZPMetaInfoStore* info_store_;
  void CheckNodeAlive();
  bool TableExist(const std::string& table);
  Status SlowdownAndStuck(const std::string table, int partition,
      const ZPMeta::Node& left, const ZPMeta::Node& right);
  Status ActiveAllPartition();

  // Migrate related
  ZPMetaMigrateRegister* migrate_register_;
  void ProcessMigrateIfNeed();

  // Statistic related
  QueryStatistic statistic;
  void ResetLastSecQueryNum();
};

#endif  // SRC_META_ZP_META_SERVER_H_
