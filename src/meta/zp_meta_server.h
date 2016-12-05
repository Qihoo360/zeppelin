#ifndef ZP_META_SERVER_H
#define ZP_META_SERVER_H

#include <stdio.h>
#include <string>

#include "slash_status.h"
#include "slash_mutex.h"
#include "floyd.h"

#include "zp_conf.h"
#include "zp_const.h"
#include "zp_meta_command.h"
#include "zp_meta_dispatch_thread.h"
#include "zp_meta_worker_thread.h"
#include "zp_meta_update_thread.h"

using slash::Status;

extern ZpConf* g_zp_conf;

typedef std::unordered_map<std::string, ZPMetaUpdateOP> ZPMetaUpdateTaskMap;
typedef std::unordered_map<std::string, struct timeval> NodeAliveMap;

class ZPMetaServer {
 public:

  explicit ZPMetaServer();
  virtual ~ZPMetaServer();
  // Server related
  void Start();
  void Stop();
  void CleanUp();
  std::string seed_ip() {
    return g_zp_conf->seed_ip();
  }
  int seed_port() {
    return g_zp_conf->seed_port();
  }
  std::string local_ip() {
    return g_zp_conf->local_ip();
  }
  int local_port() {
    return g_zp_conf->local_port();
  }
  int version() {
    return version_;
  }

  //Cmd related
  Cmd* GetCmd(const int op);
  
  // Node & Meta update related
  void AddMetaUpdateTask(const std::string& ip_port, ZPMetaUpdateOP);
  Status AddNodeAlive(const std::string& ip_port);
  Status DoUpdate(ZPMetaUpdateTaskMap task_map);
  void CheckNodeAlive();
  void ScheduleUpdate();
  
  // Meta related
  Status GetMSInfo(std::set<std::string> &tables, ZPMeta::MetaCmdResponse_Pull &ms_info);
  Status GetTableListForNode(const std::string &ip_port, std::set<std::string> &tables);
  Status Distribute(const std::string table, int num);
  Status InitVersionIfNeeded();

  // Leader related
  Status RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse &response);
  bool IsLeader();

private:

  // Server related
  ZPMetaWorkerThread* zp_meta_worker_thread_[kMaxMetaWorkerThread];
  ZPMetaDispatchThread* zp_meta_dispatch_thread_;
  std::atomic<bool> should_exit_;
  slash::Mutex server_mutex_;
  std::atomic<bool> started_;
  std::atomic<int> version_;
  int worker_num_;

  // Cmd related
  void InitClientCmdTable();

  std::unordered_map<int, Cmd*> cmds_;

  // Node & Meta update related
  bool ProcessUpdateTableInfo(ZPMetaUpdateTaskMap task_map, const ZPMeta::Nodes &nodes, ZPMeta::Table &table_info, bool &should_update_version);
  void DoDownNodeForTableInfo(const ZPMeta::Nodes &nodes, ZPMeta::Table &table_info, const std::string ip, int port, bool &should_update_table_info);
  void DoUpNodeForTableInfo(ZPMeta::Table &table_info, const std::string ip, int port, bool &should_update_table_info);
  bool ProcessUpdateNodes(ZPMetaUpdateTaskMap task_map, ZPMeta::Nodes &nodes);
  bool ShouldRetryAddVersion(ZPMetaUpdateTaskMap task_map);

  ZPMetaUpdateThread* update_thread_;
  ZPMetaUpdateTaskMap task_map_;
  slash::Mutex alive_mutex_;
  slash::Mutex task_mutex_;
  NodeAliveMap node_alive_;

  // Meta related
  void SetNodeStatus(ZPMeta::Nodes &nodes, const std::string &ip, int port, int status /*0-UP 1-DOWN*/, bool &should_update_node);
  void Reorganize(std::vector<ZPMeta::NodeStatus> &t_alive_nodes, std::vector<ZPMeta::NodeStatus> &alive_nodes);
  void GetAllAliveNode(const ZPMeta::Nodes &nodes, std::vector<ZPMeta::NodeStatus> &alive_nodes);
  Status GetTableInfo(const std::string &table, ZPMeta::Table &table_info);
  bool FindNode(ZPMeta::Nodes &nodes, const std::string &ip, int port);
  void RestoreNodeAlive(std::vector<ZPMeta::NodeStatus> &alive_nodes);
  Status GetTableList(std::vector<std::string>& tables);
  Status UpdateTableList(const std::string& name);
  Status SetTable(const ZPMeta::Table &table);
  Status SetNodes(const ZPMeta::Nodes &nodes);
  Status GetAllNodes(ZPMeta::Nodes &nodes);
  Status InitVersion();
  Status AddVersion();

  std::unordered_map<std::string, std::set<std::string> > nodes_;
  slash::Mutex node_mutex_;

  // Floyd related
  Status Set(const std::string &key, const std::string &value);
  Status Get(const std::string &key, std::string &value);
  Status Delete(const std::string &key);

  floyd::Floyd* floyd_;

  // Leader slave
  bool GetLeader(std::string& ip, int& port);
  Status BecomeLeader();
  void CleanLeader();

  slash::Mutex leader_mutex_;
  pink::PbCli* leader_cli_;
  bool leader_first_time_;
  std::string leader_ip_;
  int leader_cmd_port_;
};

#endif
