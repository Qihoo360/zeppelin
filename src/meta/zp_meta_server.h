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

  // Statistic related
  uint64_t last_qps() {
    return last_qps_.load();
  }
  uint64_t ZPMetaServer::query_num() {
    return query_num_.load();
  }
  void PlusQueryNum() {
    query_num_++;
  }
  void ResetLastSecQueryNum() {
    uint64_t cur_time_us = slash::NowMicros();
    last_qps_ = (query_num_ - last_query_num_) * 1000000 / (cur_time_us - last_time_us_ + 1);
    last_query_num_ = query_num_.load();
    last_time_us_ = cur_time_us;
  }

  // ZPMetaServer should keep the return point availible
  // during all its life cycle
  ZPMetaUpdateThread* update_thread() {
    return update_thread_;
  }

  //Cmd related
  Cmd* GetCmd(const int op);
  
  // Node & Meta update related
  void UpdateNodeAlive(const std::string& ip_port);
  void CheckNodeAlive();
  
  // Meta related
  Status GetMetaInfoByTable(const std::string& table,
      ZPMeta::MetaCmdResponse_Pull *ms_info);
  Status GetMetaInfoByNode(const std::string& ip_port,
      ZPMeta::MetaCmdResponse_Pull *ms_info);
  Status WaitSetMaster(const ZPMeta::Node& node,
      const std::string table, int partition);

  Status GetAllMetaNodes(ZPMeta::MetaCmdResponse_ListMeta *nodes);
  Status GetMetaStatus(std::string *result);
  Status GetAllNodes(ZPMeta::MetaCmdResponse_ListNode *nodes);
  Status GetTableList(std::set<std::string>* table_list);
  Status GetNodeStatusList(
      std::unordered_map<std::string, ZPMeta::NodeState>* node_list);

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
  ZPMetaUpdateThread* update_thread_;
  slash::Mutex alive_mutex_;
  NodeAliveMap node_alive_;

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
