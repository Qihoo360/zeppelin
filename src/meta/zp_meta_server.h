#ifndef ZP_META_SERVER_H
#define ZP_META_SERVER_H

#include <stdio.h>
#include <string>
#include <unordered_map>
#include <set>
#include <atomic>

#include "pink/include/server_thread.h"
#include "pink/include/pink_cli.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "floyd/include/floyd.h"

#include "include/zp_conf.h"
#include "include/zp_const.h"
#include "src/meta/zp_meta_command.h"
#include "src/meta/zp_meta_offset_map.h"
#include "src/meta/zp_meta_update_thread.h"
#include "src/meta/zp_meta_client_conn.h"
#include "src/meta/zp_meta_migrate_register.h"
#include "src/meta/zp_meta_condition_cron.h"

using slash::Status;
extern ZpConf* g_zp_conf;
typedef std::unordered_map<std::string, struct timeval> NodeAliveMap;

struct LeaderJoint {
  slash::Mutex mutex;
  pink::PinkCli* cli;
  std::string ip;
  int port;
  LeaderJoint()
    : cli(NULL),
    port(0) {
    }

  // Required: hold the lock of mutex
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

extern std::string NodeOffsetKey(const std::string& table, int partition_id,
    const std::string& ip, int port);

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

  // ZPMetaServer should keep availible during all its life cycle
  ZPMetaUpdateThread* update_thread() {
    return update_thread_;
  }

  //Cmd related
  Cmd* GetCmd(const int op);
  
  // Node alive related
  void UpdateNodeOffset(const ZPMeta::MetaCmd_Ping &ping);
  void UpdateNodeAlive(const std::string& ip_port);
  
  // Node info related
  Status GetMetaInfoByTable(const std::string& table,
      ZPMeta::MetaCmdResponse_Pull *ms_info);
  Status GetMetaInfoByNode(const std::string& ip_port,
      ZPMeta::MetaCmdResponse_Pull *ms_info);
  Status GetTableList(std::set<std::string>* table_list);
  Status GetNodeStatusList(
      std::unordered_map<std::string, ZPMeta::NodeState>* node_list);
  Status WaitSetMaster(const ZPMeta::Node& node,
      const std::string table, int partition);

  // Meta info related
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
  bool IsLeader();

private:
  // Server related
  std::atomic<bool> should_exit_;
  pink::ServerThread* server_thread_;
  ZPMetaClientConnFactory* conn_factory_;
  ZPMetaUpdateThread* update_thread_;
  void DoTimingTask();
  
  // Floyd related
  floyd::Floyd* floyd_;
  Status OpenFloyd();

  // Leader related
  LeaderJoint leader_joint_;
  bool GetLeader(std::string *ip, int *port);
  Status RefreshLeader();
  
  // Cmd related
  std::unordered_map<int, Cmd*> cmds_;
  void InitClientCmdTable();
  
  // Info related
  ZPMetaInfoStore* info_store_;
  void CheckNodeAlive();

  // Migrate related
  ZPMetaMigrateRegister* migrate_register_;
  Status ProcessMigrate();
  ZPMetaConditionCron* condition_cron_;
  
  // Offset related
  NodeOffsetMap node_offsets_;
  bool GetSlaveOffset(const std::string &table, int partition,
      const std::string ip, int port, NodeOffset* node_offset);
  void DebugOffset();

  // Statistic related
  QueryStatistic statistic;
  void ResetLastSecQueryNum();
};

#endif
