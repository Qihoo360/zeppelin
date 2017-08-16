#ifndef ZP_META_SERVER_H
#define ZP_META_SERVER_H

#include <stdio.h>
#include <string>
#include <unordered_map>
#include <set>
#include <atomic>

#include "pink/include/server_thread.h"
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

struct StuckState {
  std::string table;
  int partition;

  std::string old_master_ip;
  int old_master_port;
  NodeOffset old_master_offset;

  std::string new_master_ip;
  int new_master_port;
  NodeOffset new_master_offset;
};

namespace pink {
class ServerThread;
class PinkCli;
}

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

class ZPMetaServer {
 public:

  explicit ZPMetaServer();
  virtual ~ZPMetaServer();
  // Server related
  void Start();
  void Stop() {
    should_exit_ = true;
  }
  std::string local_ip() {
    return local_ip_;
  }
  std::string seed_ip() {
    return seed_ip_;
  }
  int local_port() {
    return local_port_;
  }
  int seed_port() {
    return seed_port_;
  }
  int current_epoch() {
    return info_store_->epoch();
  }

  //Cmd related
  Cmd* GetCmd(const int op);
  
  // Node & Meta update related
  Status DoUpdate(ZPMetaUpdateTaskDeque task_deque);
  void UpdateNodeAlive(const std::string& ip_port);
  void CheckNodeAlive();
  
  // Meta related
  Status GetMetaInfoByTable(const std::string& table,
      ZPMeta::MetaCmdResponse_Pull *ms_info);
  Status GetMetaInfoByNode(const std::string& ip_port,
      ZPMeta::MetaCmdResponse_Pull *ms_info);

  Status RemoveSlave(const std::string &table, int partition, const ZPMeta::Node &node);
  Status SetMaster(const std::string &table, int partition, const ZPMeta::Node &node);
  Status AddSlave(const std::string &table, int partition, const ZPMeta::Node &node);
  Status GetAllMetaNodes(ZPMeta::MetaCmdResponse_ListMeta *nodes);
  Status GetMetaStatus(std::string *result);
  Status GetTableList(ZPMeta::MetaCmdResponse_ListTable *tables);
  Status GetAllNodes(ZPMeta::MetaCmdResponse_ListNode *nodes);
  Status Distribute(const std::string &table, int num);
  Status DropTable(const std::string &table);

  // Migrate related
  Status Migrate(int epoch, const std::vector<ZPMeta::RelationCmdUnit>& diffs);
  Status CancelMigrate() {
    return migrate_register_->Cancel();
  }

  // offset related
  void UpdateOffset(const ZPMeta::MetaCmd_Ping &ping);


  // Leader related
  Status RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse *response);
  bool IsLeader();

  // Statistic related
  uint64_t last_qps();
  uint64_t query_num();
  void PlusQueryNum();
  void ResetLastSecQueryNum();

private:

  // Debug
  void DebugNodes();

  std::string local_ip_;
  std::string seed_ip_;
  int local_port_;
  int seed_port_;

  // Server related
  pink::ServerThread* server_thread_;
  ZPMetaClientConnFactory* conn_factory_;
  ZPMetaServerHandle* server_handle_;

  std::atomic<bool> should_exit_;
  slash::Mutex server_mutex_;
  std::atomic<bool> started_;

  // Cmd related
  void InitClientCmdTable();

  std::unordered_map<int, Cmd*> cmds_;

  // Node & Meta update related
  bool ProcessUpdateTableInfo(const ZPMetaUpdateTaskDeque task_deque, const ZPMeta::Nodes &nodes, ZPMeta::Table *table_info, bool *should_update_version);
  void DoDownNodeForTableInfo(const ZPMeta::Nodes &nodes, ZPMeta::Table *table_info, const std::string ip, int port, bool *should_update_table_info);
  void DoRemoveSlaveForTableInfo(ZPMeta::Table *table_info, const std::string& table, int partition, const std::string &ip, int port, bool *should_update_table_info);
  void DoSetMasterForTableInfo(ZPMeta::Table *table_info, const std::string& table, int partition, const std::string &ip, int port, bool *should_update_table_info);
  void DoAddSlaveForTableInfo(ZPMeta::Table *table_info, const std::string& table, int partition, const std::string &ip, int port, bool *should_update_table_info);

  void DoUpNodeForTableInfo(ZPMeta::Table *table_info, const std::string ip, int port, bool *should_update_table_info);
  void DoClearStuckForTableInfo(ZPMeta::Table *table_info, const std::string& table, int partition, bool *should_update_table_info);
  bool ProcessUpdateNodes(const ZPMetaUpdateTaskDeque task_deque, ZPMeta::Nodes *nodes);
  void AddClearStuckTaskIfNeeded(const ZPMetaUpdateTaskDeque &task_deque);
  bool ShouldRetryAddVersion(const ZPMetaUpdateTaskDeque task_deque);

  ZPMetaUpdateThread* update_thread_;
  slash::Mutex alive_mutex_;
  NodeAliveMap node_alive_;

  // Meta related
  void Reorganize(const std::vector<ZPMeta::NodeStatus> &t_alive_nodes, std::vector<ZPMeta::NodeStatus> *alive_nodes);
  void SetNodeStatus(ZPMeta::Nodes *nodes, const std::string &ip, int port, int status, bool *should_update_node);
  void GetAllAliveNode(const ZPMeta::Nodes &nodes, std::vector<ZPMeta::NodeStatus> *alive_nodes);
  Status GetTableInfo(const std::string &table, ZPMeta::Table *table_info);
  bool FindNode(const ZPMeta::Nodes &nodes, const std::string &ip, int port);
  Status ExistInTableList(const std::string &name, bool *found);
  Status RemoveTableFromTableList(const std::string &name);
  Status GetTableList(std::vector<std::string> *tables);
  Status UpdateTableList(const std::string &name);
  Status SetTable(const ZPMeta::Table &table);
  Status DeleteTable(const std::string &name);
  Status SetNodes(const ZPMeta::Nodes &nodes);
  Status GetAllNodes(ZPMeta::Nodes *nodes);
  Status InitVersion();
  Status AddVersion();

  // InfoStore related
  ZPMetaInfoStore* info_store_;

  // Migrate related
  ZPMetaMigrateRegister* migrate_register_;
  Status ProcessMigrate();
  ZPMetaConditionCron* condition_cron_;

  // Offset related
  bool GetSlaveOffset(const std::string &table, int partition,
      const std::string ip, int port, NodeOffset* node_offset);
  void DebugOffset();

  //std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, NodeOffset> > > offset_;
  NodeOffsetMap node_offsets_;
  std::unordered_map<std::string, std::set<std::string> > nodes_;
  std::vector<StuckState> stuck_;
  slash::Mutex offset_mutex_; //protect offset_ & stuck_
  slash::Mutex node_mutex_;

  // Floyd related
  Status Set(const std::string &key, const std::string &value);
  Status Get(const std::string &key, std::string &value);
  Status Delete(const std::string &key);

  floyd::Floyd* floyd_;

  // Leader slave
  bool GetLeader(std::string *ip, int *port);

  LeaderJoint leader_joint_;


  // Statistic related
  std::atomic<uint64_t> last_query_num_;
  std::atomic<uint64_t> query_num_;
  std::atomic<uint64_t> last_qps_;
  std::atomic<uint64_t> last_time_us_;
};

#endif
