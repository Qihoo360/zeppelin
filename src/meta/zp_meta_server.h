#ifndef ZP_META_SERVER_H
#define ZP_META_SERVER_H

#include <stdio.h>
#include <string>
#include <unordered_map>
#include <set>
#include <atomic>

#include "include/zp_conf.h"
#include "include/zp_const.h"
#include "src/meta/zp_meta_command.h"
#include "src/meta/zp_meta_update_thread.h"
#include "src/meta/zp_meta_client_conn.h"

#include "pink/include/server_thread.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "floyd/include/floyd.h"

using slash::Status;

extern ZpConf* g_zp_conf;

typedef std::unordered_map<std::string, struct timeval> NodeAliveMap;

struct NodeOffset {
  int32_t filenum;
  int64_t offset;
};

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

class ZPMetaServer {
 public:

  explicit ZPMetaServer();
  virtual ~ZPMetaServer();
  // Server related
  void Start();
  void Stop();
  void CleanUp();
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
  int version() {
    return version_;
  }

  //Cmd related
  Cmd* GetCmd(const int op);
  
  // Node & Meta update related
  void AddMetaUpdateTaskDequeFromFront(const ZPMetaUpdateTaskDeque &task_deque);
  Status DoUpdate(ZPMetaUpdateTaskDeque task_deque);
  Status AddNodeAlive(const std::string& ip_port);
  void AddMetaUpdateTask(const UpdateTask &task);
  void CheckNodeAlive();
  void ScheduleUpdate();
  
  // Meta related
  Status GetMSInfo(const std::set<std::string> &tables, ZPMeta::MetaCmdResponse_Pull *ms_info);
  Status GetTableListForNode(const std::string &ip_port, std::set<std::string> *tables);
  Status RemoveSlave(const std::string &table, int partition, const ZPMeta::Node &node);
  Status SetMaster(const std::string &table, int partition, const ZPMeta::Node &node);
  Status AddSlave(const std::string &table, int partition, const ZPMeta::Node &node);
  Status GetAllMetaNodes(ZPMeta::MetaCmdResponse_ListMeta *nodes);
  Status GetTableList(ZPMeta::MetaCmdResponse_ListTable *tables);
  Status GetAllNodes(ZPMeta::MetaCmdResponse_ListNode *nodes);
  Status Distribute(const std::string &table, int num);
  void UpdateOffset(const ZPMeta::MetaCmd_Ping &ping);
  Status DropTable(const std::string &table);
  Status InitVersionIfNeeded();

  // Leader related
  Status RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse *response);
  bool IsLeader();
  void DebugOffset();

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
  std::atomic<int> version_;

  // Cmd related
  void InitClientCmdTable();

  std::unordered_map<int, Cmd*> cmds_;

  // Node & Meta update related
  bool ProcessUpdateTableInfo(const ZPMetaUpdateTaskDeque task_deque, const ZPMeta::Nodes &nodes, ZPMeta::Table *table_info, bool *should_update_version);
  void DoDownNodeForTableInfo(const ZPMeta::Nodes &nodes, ZPMeta::Table *table_info, const std::string ip, int port, bool *should_update_table_info);
  void DoRemoveSlaveForTableInfo(ZPMeta::Table *table_info, int partition, const std::string &ip, int port, bool *should_update_table_info);
  void DoSetMasterForTableInfo(ZPMeta::Table *table_info, int partition, const std::string &ip, int port, bool *should_update_table_info);
  void DoAddSlaveForTableInfo(ZPMeta::Table *table_info, int partition, const std::string &ip, int port, bool *should_update_table_info);

  void DoUpNodeForTableInfo(ZPMeta::Table *table_info, const std::string ip, int port, bool *should_update_table_info);
  void DoClearStuckForTableInfo(ZPMeta::Table *table_info, int partition, bool *should_update_table_info);
  bool ProcessUpdateNodes(const ZPMetaUpdateTaskDeque task_deque, ZPMeta::Nodes *nodes);
  void AddClearStuckTaskIfNeeded(const ZPMetaUpdateTaskDeque &task_deque);
  bool ShouldRetryAddVersion(const ZPMetaUpdateTaskDeque task_deque);

  ZPMetaUpdateThread* update_thread_;
  ZPMetaUpdateTaskDeque task_deque_;
  slash::Mutex alive_mutex_;
  slash::Mutex task_mutex_;
  NodeAliveMap node_alive_;

  // Meta related
  bool GetSlaveOffset(const std::string &table, const std::string &ip_port, const int partition, int32_t *filenum, int64_t *offset);
  void Reorganize(const std::vector<ZPMeta::NodeStatus> &t_alive_nodes, std::vector<ZPMeta::NodeStatus> *alive_nodes);
  void SetNodeStatus(ZPMeta::Nodes *nodes, const std::string &ip, int port, int status, bool *should_update_node);
  void GetAllAliveNode(const ZPMeta::Nodes &nodes, std::vector<ZPMeta::NodeStatus> *alive_nodes);
  Status GetTableInfo(const std::string &table, ZPMeta::Table *table_info);
  bool FindNode(const ZPMeta::Nodes &nodes, const std::string &ip, int port);
  void RestoreNodeAlive(const std::vector<ZPMeta::NodeStatus> &alive_nodes);
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


  std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, NodeOffset> > > offset_;
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
  Status BecomeLeader();
  void CleanLeader();

  slash::Mutex leader_mutex_;
  pink::PinkCli* leader_cli_;
  bool leader_first_time_;
  std::string leader_ip_;
  int leader_cmd_port_;

  // Statistic related
  std::atomic<uint64_t> last_query_num_;
  std::atomic<uint64_t> query_num_;
  std::atomic<uint64_t> last_qps_;
  std::atomic<uint64_t> last_time_us_;
};

#endif
