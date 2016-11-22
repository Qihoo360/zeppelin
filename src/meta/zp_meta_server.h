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

typedef std::unordered_map<std::string, struct timeval> NodeAliveMap;
typedef std::unordered_map<std::string, ZPMetaUpdateOP> ZPMetaUpdateTaskMap;

class ZPMetaServer {
 public:

  explicit ZPMetaServer();
  virtual ~ZPMetaServer();
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

  Cmd* GetCmd(const int op);
  
  // Node alive related
  Status AddNodeAlive(const std::string& ip_port);
  void CheckNodeAlive();
  void AddMetaUpdateTask(const std::string& ip_port, ZPMetaUpdateOP);
  void ClearMetaUpdateTask();
  void ScheduleUpdate();
//  bool UpdateNodeAlive(const std::string& ip_port);
  Status DoUpdate(ZPMetaUpdateTaskMap task_map);
  
  // Floyd related
  int version() {
    return version_;
  }
  void Reorganize(std::vector<ZPMeta::NodeStatus> &t_alive_nodes, std::vector<ZPMeta::NodeStatus> &alive_nodes);
  Status Distribute(std::string table, int num);
  Status GetTablesFromNode(std::string &ip_port, std::vector<std::string> &tables);
  Status GetTableInfo(std::string &table, ZPMeta::Table &table_info);
  Status GetMSInfo(std::vector<std::string> &tables, ZPMeta::MetaCmdResponse_Pull &ms_info);

  // Leader related
  bool IsLeader();
  Status RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse &response);

private:
  // Cmd related
  std::unordered_map<int, Cmd*> cmds_;
  void InitClientCmdTable();

  // Server related
  int worker_num_;
  ZPMetaWorkerThread* zp_meta_worker_thread_[kMaxMetaWorkerThread];
  ZPMetaDispatchThread* zp_meta_dispatch_thread_;
  slash::Mutex server_mutex_;
  std::atomic<int> version_;
  std::atomic<bool> should_exit_;
  std::atomic<bool> started_;
  std::unordered_map<std::string, std::vector<std::string> > nodes_;

  // Floyd related
  floyd::Floyd* floyd_;
  Status InitVersion();
  Status Set(const std::string &key, const std::string &value);
  Status Get(const std::string &key, std::string &value);
  Status Delete(const std::string &key);
  Status GetAllNode(ZPMeta::Nodes &nodes);
  void GetAllAliveNode(ZPMeta::Nodes &nodes, std::vector<ZPMeta::NodeStatus> &alive_nodes);
  bool FindNode(ZPMeta::Nodes &nodes, const std::string &ip, int port);
  Status SetNodeStatus(ZPMeta::Nodes &nodes, ZPMeta::Table &table_info, const std::string &ip, int port, int status /*0-UP 1-DOWN*/);
  Status AddNode(ZPMeta::Nodes &nodes, ZPMeta::Table &table_info, const std::string &ip, int port);
  Status SetTable(const ZPMeta::Table &table);
  Status GetTable(std::vector<std::string> tables);
  bool OnNode(ZPMeta::Table &table_info, const std::string &ip, int port);
  Status OffNode(ZPMeta::Nodes &nodes, ZPMeta::Table &table_info, const std::string &ip, int port);

  // Alive Check
  slash::Mutex alive_mutex_;
  slash::Mutex node_mutex_;
  slash::Mutex task_mutex_;
  NodeAliveMap node_alive_;
  ZPMetaUpdateTaskMap task_map_;
  ZPMetaUpdateThread* update_thread_;
  void RestoreNodeAlive(std::vector<ZPMeta::NodeStatus> &alive_nodes);

  // Leader slave
  bool leader_first_time_;
  slash::Mutex leader_mutex_;
  pink::PbCli* leader_cli_;
  std::string leader_ip_;
  int leader_cmd_port_;
  Status BecomeLeader();
  void CleanLeader();
  bool GetLeader(std::string& ip, int& port);
};

#endif
