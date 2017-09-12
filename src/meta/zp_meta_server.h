#ifndef ZP_META_SERVER_H
#define ZP_META_SERVER_H

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
#include "src/meta/zp_meta_update_thread.h"
#include "src/meta/zp_meta_client_conn.h"
#include "src/meta/zp_meta_migrate_register.h"
#include "src/meta/zp_meta_condition_cron.h"

using slash::Status;
extern ZpConf* g_zp_conf;
typedef std::unordered_map<std::string, struct timeval> NodeAliveMap;

struct LeaderJoint {
  pink::PinkCli* cli;
  std::string ip;
  int port;
  LeaderJoint()
    : cli(NULL),
    port(0) {
    }

  void CleanLeader() {
    if (cli) {
      cli->Close();
      delete cli;
      cli = NULL;
    }
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

class ZPMetaServer {
 public:
  explicit ZPMetaServer();
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

  //Cmd related
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
  Status CreateTable(const std::string& table, int pnum);
  Status DropTable(const std::string& table);
  Status AddPartitionSlave(const std::string& table, int pnum,
      const ZPMeta::Node& node);
  Status RemovePartitionSlave(const std::string& table, int pnum,
      const ZPMeta::Node& node);
  Status GetAllMetaNodes(ZPMeta::MetaCmdResponse_ListMeta *nodes);
  Status GetMetaStatus(std::string *result);

  // Migrate related
  Status Migrate(int epoch, const std::vector<ZPMeta::RelationCmdUnit>& diffs);
  Status CancelMigrate() {
    return migrate_register_->Cancel();
  }

  // Leader related
  Status RedirectToLeader(ZPMeta::MetaCmd &request,
      ZPMeta::MetaCmdResponse *response);
  // Required hold mutex of leader_mutex
  bool IsLeader() {
    return is_leader_;
  }

  slash::Mutex leader_mutex;

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
  bool is_leader_;
  LeaderJoint leader_joint_;
  bool GetLeader(std::string *ip, int *port);
  Status RefreshLeader();
  
  // Cmd related
  std::unordered_map<int, Cmd*> cmds_;
  void InitClientCmdTable();
  
  // Info related
  ZPMetaInfoStore* info_store_;
  void CheckNodeAlive();
  bool TableExist(const std::string& table);
  
  // Migrate related
  ZPMetaMigrateRegister* migrate_register_;
  void ProcessMigrateIfNeed();
  
  // Statistic related
  QueryStatistic statistic;
  void ResetLastSecQueryNum();
};

#endif
