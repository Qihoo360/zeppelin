#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/repeated_field.h>

#include "slash_string.h"
#include "zp_meta_server.h"
#include "zp_meta.pb.h"

enum ZPNodeStatus {
  kNodeUp,
  kNodeDown
};

ZPMetaServer::ZPMetaServer()
  : should_exit_(false), started_(false), version_(-1), worker_num_(6), leader_cli_(NULL), leader_first_time_(true), leader_ip_(""), leader_cmd_port_(0) {

  floyd::Options fy_options;
  fy_options.seed_ip = g_zp_conf->seed_ip();
  fy_options.seed_port = g_zp_conf->seed_port() + kMetaPortShiftFY;
  fy_options.local_ip = g_zp_conf->local_ip();
  fy_options.local_port = g_zp_conf->local_port() + kMetaPortShiftFY;
  fy_options.data_path = g_zp_conf->data_path();
  fy_options.log_path = g_zp_conf->log_path();
  fy_options.log_type = "FileLog";

  floyd_ = new floyd::Floyd(fy_options);

  cmds_.reserve(300);
  InitClientCmdTable();  

  for (int i = 0; i < worker_num_ ; ++i) {
    zp_meta_worker_thread_[i] = new ZPMetaWorkerThread(kMetaWorkerCronInterval);
  }
  zp_meta_dispatch_thread_ = new ZPMetaDispatchThread(g_zp_conf->local_port() + kMetaPortShiftCmd, worker_num_, zp_meta_worker_thread_, kMetaDispathCronInterval);
  update_thread_ = new ZPMetaUpdateThread();
}

ZPMetaServer::~ZPMetaServer() {
  delete zp_meta_dispatch_thread_;
  for (int i = 0; i < worker_num_; ++i) {
    delete zp_meta_worker_thread_[i];
  }
  DestoryCmdTable(cmds_);
  delete update_thread_;
  CleanLeader();
  delete floyd_;
  LOG(INFO) << "Delete Done";
}

void ZPMetaServer::Start() {
  LOG(INFO) << "ZPMetaServer started on port:" << g_zp_conf->local_port() << ", seed is " << g_zp_conf->seed_ip().c_str() << ":" <<g_zp_conf->seed_port();
  floyd::Status s = floyd_->Start();
  if (!s.ok()) {
    LOG(ERROR) << "Start floyd failed: " << s.ToString();
    return;
  }
  std::string leader_ip;
  int leader_port;
  while (!GetLeader(leader_ip, leader_port) && !should_exit_) {
    LOG(INFO) << "Wait leader ... ";
    // Wait leader election
    sleep(1);
  }
  if (!should_exit_) {
    LOG(INFO) << "Got Leader: " << leader_ip << ":" << leader_port;
    InitVersion();
    zp_meta_dispatch_thread_->StartThread();

    server_mutex_.Lock();
    started_ = true;
    server_mutex_.Lock();
    server_mutex_.Unlock();
  }
  CleanUp();
}

void ZPMetaServer::Stop() {
  if (started_) {
    server_mutex_.Unlock();
  }
  should_exit_ = true;
}

void ZPMetaServer::CleanUp() {
  if (g_zp_conf->daemonize()) {
    unlink(g_zp_conf->pid_file().c_str());
  }
  delete this;
  ::google::ShutdownGoogleLogging();
}

Cmd* ZPMetaServer::GetCmd(const int op) {
  return GetCmdFromTable(op, cmds_);
}

Status ZPMetaServer::ProcessUpdate(ZPMetaUpdateTaskMap task_map, ZPMeta::Nodes &nodes, ZPMeta::Table &table_info, bool &should_update_version) {
  bool should_rewrite_table_info = false;
  bool should_rewrite_nodes = false;
  std::string ip;
  int port;
  Status s;

  for (auto iter = task_map.begin(); iter != task_map.end(); iter++) {
    LOG(INFO) << "process task in DoUpdate: " << iter->first << ", " << iter->second;
    if (!slash::ParseIpPortString(iter->first, ip, port)) {
      return Status::Corruption("ParseIpPortString error");
    }
    if (iter->second == ZPMetaUpdateOP::kOpAdd) {
      s = AddNode(nodes, table_info, ip, port);
      LOG(INFO) << "AddNode ret:" << s.ToString();
      if (s.IsNotFound()) {
        should_rewrite_nodes = true;
        if (s.ToString() == "NotFound: 2") {
          should_rewrite_table_info = true;
          should_update_version = true;
        }
      }
    } else if (iter->second == ZPMetaUpdateOP::kOpRemove) {
      s = OffNode(nodes, table_info, ip, port);
      LOG(INFO) << "OffNode ret: " << s.ToString();
      if (s.IsNotFound()) {
        should_rewrite_nodes = true;
        if (s.ToString() == "NotFound: 2") {
          should_rewrite_table_info = true;
          should_update_version = true;
        }
      }
    } else if (iter->second == ZPMetaUpdateOP::kOpAddVersion) {
      s = Set(kMetaVersion, std::to_string(version_+1));
      if (s.ok()) {
        version_++; 
        LOG(INFO) << "Set version in Distribute : " << version_;
      } else {
        LOG(ERROR) << "Set version error in Distribute, error: " << s.ToString();
        return Status::Corruption("add version failed");
      }
    }
  }
  LOG(INFO) << "should_rewrite_table_info in DoUpdate: " << should_rewrite_table_info;
  LOG(INFO) << "should_rewrite_nodes in DoUpdate: " << should_rewrite_nodes;

  if (should_rewrite_table_info) {

    std::string text_format;
    google::protobuf::TextFormat::PrintToString(table_info, &text_format);
    LOG(INFO) << "table_info : [" << text_format << "]";

    s = SetTable(table_info);
    if (!s.ok()) {
      LOG(ERROR) << "SetTable error in DoUpdate, error: " << s.ToString();
    }
  }

  if (should_rewrite_nodes) {
    std::string new_value;
    if (!nodes.SerializeToString(&new_value)) {
      LOG(ERROR) << "Serialization new meta failed, new value: " <<  new_value;
      return Status::Corruption("Serialize error");
    }
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(nodes, &text_format);
    LOG(INFO) << "nodes : [" << text_format << "]";
    
    s = Set(kMetaNodes, new_value);
    if (!s.ok()) {
      LOG(ERROR) << "Set error in DoUpdate: " << s.ToString();
      return Status::Corruption("floyd set error!");
    }
  }

  return Status::OK();
}

void ZPMetaServer::AddMetaUpdateTask(const std::string& ip_port, ZPMetaUpdateOP op) {
  slash::MutexLock l(&task_mutex_);
  task_map_.insert(std::unordered_map<std::string, ZPMetaUpdateOP>::value_type(ip_port, op));
}

Status ZPMetaServer::AddNodeAlive(const std::string& ip_port) {
  bool should_add = false;
  {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);
  if (node_alive_.find(ip_port) == node_alive_.end()) {
    should_add = true;
  }
  gettimeofday(&now, NULL);
  node_alive_[ip_port] = now;
  }

  if (!should_add) {
    return Status::OK();
  }

  std::string ip;
  int port;
  if (!slash::ParseIpPortString(ip_port, ip, port)) {
    return Status::Corruption("parse ip_port error");
  }

  LOG(INFO) << "Add Node Alive " << ip_port;
  AddMetaUpdateTask(ip_port, ZPMetaUpdateOP::kOpAdd);
  return Status::OK();
}

Status ZPMetaServer::DoUpdate(ZPMetaUpdateTaskMap task_map) {
  ZPMeta::Table table_info;
  ZPMeta::Nodes nodes;

  slash::MutexLock l(&node_mutex_);
  Status s = GetAllNode(nodes);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetAllNode error in DoUpdate, error: " << s.ToString();
    return s;
  }

  bool should_update_version = false;
  std::vector<std::string> tables;
  s = GetTable(tables);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetTable error in DoUpdate, error: " << s.ToString();
    return s;
  }

  table_info.Clear();
  s = ProcessUpdate(task_map, nodes, table_info, should_update_version);
  if (!s.ok()) {
    LOG(ERROR) << "ProcessUpdate(Node) error in DoUpdate, error: " << s.ToString();
    return s;
  }

  bool sth_wrong = false;
  for (auto it = tables.begin(); it != tables.end(); it++) {

    table_info.Clear();
    s = GetTableInfo(*it, table_info);
    if (!s.ok() && !s.IsNotFound()) {
      LOG(ERROR) << "GetTableInfo error in DoUpdate, table: " << *it << " error: " << s.ToString();
      sth_wrong = true;
      continue;
    }
    s = ProcessUpdate(task_map, nodes, table_info, should_update_version);
    if (!s.ok()) {
      LOG(ERROR) << "ProcessUpdate(TableInfo) error in DoUpdate, error: " << s.ToString();
      sth_wrong = true;
      continue;
    }
  }

  if (should_update_version) {
    s = Set(kMetaVersion, std::to_string(version_+1));
    if (s.ok()) {
      version_++;
      LOG(INFO) << "Set version in Distribute : " << version_;
    } else {
      LOG(ERROR) << "Set version error in Distribute, error: " << s.ToString();
      return Status::Corruption("add version failed");
    }
  }

  if (sth_wrong) {
    return Status::Corruption("sth wrong");
  }
  return Status::OK();
}

void ZPMetaServer::CheckNodeAlive() {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);

  gettimeofday(&now, NULL);
  auto it = node_alive_.begin();
  while (it != node_alive_.end()) {
    if (now.tv_sec - (it->second).tv_sec > kNodeMetaTimeoutM) {
      AddMetaUpdateTask(it->first, ZPMetaUpdateOP::kOpRemove);
      it = node_alive_.erase(it);
    } else {
      it++;
    }
  }
}

void ZPMetaServer::ScheduleUpdate() {
  slash::MutexLock l(&task_mutex_);
  if (!task_map_.empty()) {
    update_thread_->ScheduleUpdate(task_map_);
    task_map_.clear();
  }
}

Status ZPMetaServer::GetMSInfo(std::vector<std::string> &tables, ZPMeta::MetaCmdResponse_Pull &ms_info) {
  ms_info.Clear();
  ZPMeta::Table table_info;
  ZPMeta::Table *t;
  Status s;
  ms_info.set_version(version_);
  for (auto it = tables.begin(); it != tables.end(); it++) {
    s = GetTableInfo(*it, table_info);
    if (s.ok()) {
      t = ms_info.add_info();
      t->CopyFrom(table_info);
    } else if (s.IsNotFound()) {
      LOG(WARNING) << "GetMSInfo, NotFound, table: " << *it;
    } else {
      LOG(WARNING) << "GetMSInfo error, table: " << *it << " error: " << s.ToString();
      return s;
    }
  }
  return s;
}

Status ZPMetaServer::GetTablesFromNode(const std::string &ip_port, std::vector<std::string> &tables) {
  tables.clear();
  slash::MutexLock l(&node_mutex_);
  auto iter = nodes_.find(ip_port);
  if (iter != nodes_.end()) {
    tables = iter->second;
  }
  return Status::OK();
}

Status ZPMetaServer::Distribute(const std::string name, int num) {
  slash::MutexLock l(&node_mutex_);
  std::string value;
  Status s;
  s = Get(name, value);
  if (s.ok()) {
    return Status::Corruption("Already Created");
  }
 
  ZPMeta::Nodes nodes;
  s = GetAllNode(nodes);
  if (!s.ok()) {
    return s;
  }

  if (num < nodes.nodes_size()) {
    return Status::Corruption("Too few partition num");
  }

  std::vector<ZPMeta::NodeStatus> t_alive_nodes;
  GetAllAliveNode(nodes, t_alive_nodes);
  if (t_alive_nodes.empty()) {
    return Status::Corruption("no nodes");
  }

  std::vector<ZPMeta::NodeStatus> alive_nodes;
  Reorganize(t_alive_nodes, alive_nodes);

  int an_num = alive_nodes.size();

  ZPMeta::Table table;

  table.set_name(name);

  for (int i = 0; i < num; i++) {
    ZPMeta::Partitions *p = table.add_partitions();
    p->set_id(i);
    p->mutable_master()->CopyFrom(alive_nodes[i % an_num].node());

    ZPMeta::Node *slave = p->add_slaves();
    slave->CopyFrom(alive_nodes[(i + 1) % an_num].node());

    slave = p->add_slaves();
    slave->CopyFrom(alive_nodes[(i + 2) % an_num].node());

  }

  s = SetTable(table);
  if (!s.ok()) {
    LOG(ERROR) << "SetTable error in Distribute, error: " << s.ToString();
    return s;
  }
  s = Set(kMetaVersion, std::to_string(version_+1));
  if (s.ok()) {
    version_++; 
    LOG(INFO) << "Set version in Distribute : " << version_;
  } else {
    LOG(ERROR) << "Set version error in Distribute, error: " << s.ToString();
    return s;
  }

  std::string ip_port;
  for (auto iter = alive_nodes.begin(); iter != alive_nodes.end(); iter++) {
    ip_port = slash::IpPortString(iter->node().ip(), iter->node().port());
    auto it = nodes_.find(ip_port);
    if (it != nodes_.end()) {
      it->second.push_back(name);
    } else {
      std::vector<std::string> ts;
      ts.push_back(name);
      nodes_.insert(std::unordered_map<std::string, std::vector<std::string> >::value_type(ip_port, ts));
    }
  }

  std::string text_format;
  google::protobuf::TextFormat::PrintToString(table, &text_format);
  LOG(INFO) << "ms_info : [" << text_format << "]";

  return Status::OK();
}

Status ZPMetaServer::RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse &response) {
  slash::MutexLock l(&leader_mutex_);
  if (leader_cli_ == NULL) {
    LOG(ERROR) << "Error in RedirectToLeader, leader_cli_ is NULL";
    return Status::Corruption("no leader connection");
  }
  pink::Status s = leader_cli_->Send(&request);
  if (!s.ok()) {
    CleanLeader();
    LOG(ERROR) << "Failed to redirect message to leader, " << s.ToString();
    return Status::Corruption(s.ToString());
  }
  s = leader_cli_->Recv(&response); 
  if (!s.ok()) {
    CleanLeader();
    LOG(ERROR) << "Failed to get redirect message response from leader" << s.ToString();
    return Status::Corruption(s.ToString());
  }
  //std::string text_format;
  //google::protobuf::TextFormat::PrintToString(response, &text_format);
  //LOG(INFO) << "recever redirect message response from leader: [" << text_format << "]";
  return Status::OK();
}

bool ZPMetaServer::IsLeader() {
  std::string leader_ip;
  int leader_port = 0, leader_cmd_port = 0;
  while (!should_exit_ && !GetLeader(leader_ip, leader_port)) {
    LOG(INFO) << "Wait leader ... ";
    // Wait leader election
    sleep(1);
  }
  if (should_exit_) {
    leader_cli_ = NULL;
    return false;
  }
  LOG(INFO) << "Leader: " << leader_ip << ":" << leader_port;

  slash::MutexLock l(&leader_mutex_);
  leader_cmd_port = leader_port + kMetaPortShiftCmd;
  if (leader_ip == leader_ip_ && leader_cmd_port == leader_cmd_port_) {
    // has connected to leader
    return false;
  }
  
  // Leader changed
  if (leader_ip == g_zp_conf->local_ip() && 
      leader_port == g_zp_conf->local_port()) {
    // I am Leader
    if (leader_first_time_) {
      leader_first_time_ = false;
      CleanLeader();
      LOG(INFO) << "Become to leader";
      BecomeLeader(); // Just become leader
      LOG(INFO) << "Become to leader success";
    }
    return true;
  }
  
  // Connect to new leader
  CleanLeader();
  leader_first_time_ = true;
  leader_cli_ = new pink::PbCli();
  leader_ip_ = leader_ip;
  leader_cmd_port_ = leader_cmd_port;
  pink::Status s = leader_cli_->Connect(leader_ip_, leader_cmd_port_);
  if (!s.ok()) {
    CleanLeader();
    LOG(ERROR) << "Connect to leader: " << leader_ip_ << ":" << leader_cmd_port_ << " failed";
  } else {
    LOG(INFO) << "Connect to leader: " << leader_ip_ << ":" << leader_cmd_port_ << " success";
    leader_cli_->set_send_timeout(1000);
    leader_cli_->set_recv_timeout(1000);
  }
  return false;
}

void ZPMetaServer::InitClientCmdTable() {

  // Ping Command
  Cmd* pingptr = new PingCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_PING), pingptr));

  //Pull Command
  Cmd* pullptr = new PullCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_PULL), pullptr));

  //Init Command
  Cmd* initptr = new InitCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_INIT), initptr));
}

Status ZPMetaServer::AddNode(ZPMeta::Nodes &nodes, ZPMeta::Table &table_info, const std::string &ip, int port) {
  if (FindNode(nodes, ip, port)) {
    return SetNodeStatus(nodes, table_info, ip, port, ZPNodeStatus::kNodeUp);
  } else {
    ZPMeta::NodeStatus *node_status = nodes.add_nodes();
    node_status->mutable_node()->set_ip(ip);
    node_status->mutable_node()->set_port(port);
    node_status->set_status(ZPNodeStatus::kNodeUp);
  }
  return Status::NotFound("1");
}

static bool IsAlive(std::vector<ZPMeta::NodeStatus> &alive_nodes, const std::string &ip, const int port) {
  for (auto iter = alive_nodes.begin(); iter != alive_nodes.end(); iter++) {
    if (iter->node().ip() == ip && iter->node().port() == port) {
      return true;
    }
  }
  return false;
}

Status ZPMetaServer::OffNode(ZPMeta::Nodes &nodes, ZPMeta::Table &table_info, const std::string &ip, int port) {

  std::vector<ZPMeta::NodeStatus> alive_nodes;

  SetNodeStatus(nodes, table_info, ip, port, ZPNodeStatus::kNodeDown);
  GetAllAliveNode(nodes, alive_nodes);

  ZPMeta::Node tmp;
  bool should_rewrite_table_info = false;

  for (int i = 0; i < table_info.partitions_size(); ++i) {
    ZPMeta::Partitions* p = table_info.mutable_partitions(i);
    if (ip != p->master().ip() || port != p->master().port()) {
      continue;
    }

    should_rewrite_table_info = true;
    tmp.CopyFrom(p->master());
    ZPMeta::Node* master = p->mutable_master();
    int slaves_size = p->slaves_size();
    LOG(INFO) << "slaves_size:" << slaves_size;
    int j = 0;
    for (j; j < slaves_size; j++) {
      if (IsAlive(alive_nodes, p->slaves(j).ip(), p->slaves(j).port())) {
        LOG(INFO) << "Use Slave " << j << " " << p->slaves(j).ip() << " " << p->slaves(j).port();
        master->CopyFrom(p->slaves(j));
        ZPMeta::Node* first = p->mutable_slaves(j);
        first->CopyFrom(tmp);
        break;
      }
    }
    if (j == slaves_size) {
      LOG(INFO) << "No Slave to use";
      ZPMeta::Node *slave = p->add_slaves();
      slave->CopyFrom(tmp);

      master->set_ip("");
      master->set_port(0);
    }
    tmp.Clear();
  }

  if (should_rewrite_table_info) {
    LOG(INFO) << "should rewrite tableinfo in offnode";
    return Status::NotFound("2");
  }
  return Status::NotFound("1");
}

bool ZPMetaServer::OnNode(ZPMeta::Table &table_info, const std::string &ip, int port) {

  bool should_rewrite = false;
  int slaves_size = 0;
  for (int i = 0; i < table_info.partitions_size(); ++i) {
    ZPMeta::Partitions* p = table_info.mutable_partitions(i);
    if (p->master().ip() == "" && p->master().port() == 0) {
      slaves_size = p->slaves_size();
      for(int j = 0; j < slaves_size; j++) {
        if (p->slaves(j).ip() == ip && p->slaves(j).port() == port) {
          should_rewrite = true;
          ZPMeta::Node* master = p->mutable_master();
          master->CopyFrom(p->slaves(j));
          ZPMeta::Node* slave = p->mutable_slaves(j);
          slave->CopyFrom(p->slaves(slaves_size-1));
          p->mutable_slaves()->RemoveLast();
          break;
        }
      }
    }
  }
  LOG(INFO) << "should_rewrite in OnNode: " << should_rewrite;
  return should_rewrite;
}

Status ZPMetaServer::SetNodeStatus(ZPMeta::Nodes& nodes, ZPMeta::Table &table_info, const std::string &ip, int port,
    int status /*0-kNodeUp 1-kNodeDown*/) {
  int update_nodes = false;
  bool update_table_info = false;
  for (int i = 0; i < nodes.nodes_size(); ++i) {
    ZPMeta::NodeStatus* node_status = nodes.mutable_nodes(i);
    if (ip == node_status->node().ip() && port == node_status->node().port()) {
      if (node_status->status() == status) {
      } else {
        update_nodes = true;
        node_status->set_status(status);
        if (status == ZPNodeStatus::kNodeUp) {
          if(OnNode(table_info, ip, port)) {
            update_table_info =true;
          }
        }
      }
    }
  }

  if (update_table_info && update_nodes) {
    return Status::NotFound("2");
  } else if (!update_table_info && !update_nodes) {
    return Status::OK();
  } else {
    return Status::NotFound("1");
  }
}

void ZPMetaServer::Reorganize(std::vector<ZPMeta::NodeStatus> &t_alive_nodes, std::vector<ZPMeta::NodeStatus> &alive_nodes) {
  std::map<std::string, std::vector<ZPMeta::NodeStatus> >m;

  for (auto iter_v = t_alive_nodes.begin(); iter_v != t_alive_nodes.end(); iter_v++) {
    auto iter_m = m.find(iter_v->node().ip());
    if (iter_m != m.end()) {
      iter_m->second.push_back(*iter_v);
    } else {
      std::vector<ZPMeta::NodeStatus> n;
      n.push_back(*iter_v);
      m.insert(std::map<std::string, std::vector<ZPMeta::NodeStatus> >::value_type(iter_v->node().ip(), n));
    }
  }

  int msize = m.size();
  int empty_count = 0;
  while (true) {
    if (empty_count == msize) {
      break;
    }
    for (auto iter_m = m.begin(); iter_m != m.end(); iter_m++) {
      if (iter_m->second.empty()) {
        empty_count++;
        continue;
      } else {
        LOG(INFO) << "PUSH " << iter_m->second.back().node().ip() << ":" << iter_m->second.back().node().port();
        alive_nodes.push_back(iter_m->second.back());
        iter_m->second.pop_back();
      }
    }
  }
}

void ZPMetaServer::GetAllAliveNode(ZPMeta::Nodes &nodes, std::vector<ZPMeta::NodeStatus> &alive_nodes) {
  for (int i = 0; i < nodes.nodes_size(); i++) {
    const ZPMeta::NodeStatus node_status = nodes.nodes(i);
    if (node_status.status() == 0) {
      alive_nodes.push_back(node_status);
    }
  }
}

Status ZPMetaServer::GetTableInfo(const std::string &table, ZPMeta::Table &table_info) {
  std::string value;
  floyd::Status fs = floyd_->DirtyRead(table, value);
  if (fs.ok()) {
    table_info.Clear();
    if (!table_info.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table_info failed, table: " << table << " value: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    return Status::OK();
  } else if (fs.IsNotFound()) {
    return Status::NotFound("table_info not found");
  } else {
    LOG(ERROR) << "Floyd read table_info failed: " << fs.ToString();
    return Status::Corruption("Read table_info failed!");
  }
}

bool ZPMetaServer::FindNode(ZPMeta::Nodes &nodes, const std::string &ip, int port) {
  for (int i = 0; i < nodes.nodes_size(); ++i) {
    const ZPMeta::NodeStatus& node_status = nodes.nodes(i);
    if (ip == node_status.node().ip() && port == node_status.node().port()) {
      return true;
    }
  }
  return false;
}

void ZPMetaServer::RestoreNodeAlive(std::vector<ZPMeta::NodeStatus> &alive_nodes) {
  struct timeval now;
  gettimeofday(&now, NULL);

  slash::MutexLock l(&alive_mutex_);
  node_alive_.clear();
  auto iter = alive_nodes.begin();
  while (iter != alive_nodes.end()) {
    node_alive_[slash::IpPortString(iter->node().ip(), iter->node().port())] = now;
    iter++;
  }
}

Status ZPMetaServer::GetTable(std::vector<std::string> &tables) {
  std::string value;
  ZPMeta::TableName table_name;
  Status s = Get(kMetaTables, value);
  if (s.ok()) {
    if (!table_name.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table failed, error: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    tables.clear();
    for (int i = 0; i< table_name.name_size(); i++) {
      tables.push_back(table_name.name(i));
    }
  }
  return s;
}

Status ZPMetaServer::SetTable(const ZPMeta::Table &table) {
  std::string new_value;
  if (!table.SerializeToString(&new_value)) {
    LOG(ERROR) << "Serialization table failed, new value: " <<  new_value;
    return Status::Corruption("Serialize error");
  }
  return Set(table.name(), new_value);
}

Status ZPMetaServer::GetAllNode(ZPMeta::Nodes &nodes) {
  // Load from Floyd
  std::string value;
  floyd::Status fs = floyd_->DirtyRead(kMetaNodes, value);
  nodes.Clear();
  if (fs.ok()) {
    // Deserialization
    if (!nodes.ParseFromString(value)) {
      LOG(ERROR) << "deserialization AllNodeInfo failed, value: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    return Status::OK();
  } else if (fs.IsNotFound()) {
    return Status::NotFound("not found from floyd");
  } else {
    LOG(ERROR) << "GetAllNode, floyd read failed: " << fs.ToString();
    return Status::Corruption("floyd get error!");
  }
}

Status ZPMetaServer::InitVersion() {
  std::string value;
  ZPMeta::TableName tables;
  ZPMeta::Table table_info;
  ZPMeta::Partitions partition;
  ZPMeta::Node node;
  floyd::Status fs;
  std::string ip_port;
  while(1) {
    fs = floyd_->Read(kMetaTables, value);
    if (fs.ok()) {
      if (value == "") {
        if (!tables.ParseFromString(value)) {
          LOG(ERROR) << "Deserialization table failed, error: " << value;
          return slash::Status::Corruption("Parse failed");
        }
        slash::MutexLock l(&node_mutex_);
        nodes_.clear();
        for (int i = 0; i < tables.name_size(); i++) {
          fs = floyd_->Read(tables.name(i), value);
          if (!table_info.ParseFromString(value)) {
            LOG(ERROR) << "Deserialization table_info failed, table: " << tables.name(i) << " value: " << value;
            return slash::Status::Corruption("Parse failed");
          }

          for (int i = 0; i < table_info.partitions_size(); i++) {
            partition = table_info.partitions(i);

            if (partition.master().ip() != "" && partition.master().port() != -1) {
              ip_port = slash::IpPortString(partition.master().ip(), partition.master().port());
              auto iter = nodes_.find(ip_port);
              if (iter != nodes_.end()) {
                iter->second.push_back(tables.name(i));
              } else {
                std::vector<std::string> ts;
                ts.push_back(tables.name(i));
                nodes_.insert(std::unordered_map<std::string, std::vector<std::string> >::value_type(ip_port, ts));
              }
            }

            for (int j = 0; j < partition.slaves_size(); j++) {
              ip_port = slash::IpPortString(partition.slaves(j).ip(), partition.slaves(j).port());
              auto iter = nodes_.find(ip_port);
              if (iter != nodes_.end()) {
                iter->second.push_back(tables.name(i));
              } else {
                std::vector<std::string> ts;
                ts.push_back(tables.name(i));
                nodes_.insert(std::unordered_map<std::string, std::vector<std::string> >::value_type(ip_port, ts));
              }
            }
          }
        }
      }
      return Status::OK();
    } else {
      LOG(ERROR) << "Read floyd tabls failed in InitVersion: " << fs.ToString() << ", try again";
      sleep(1);
    }
  
  }
  while(1) {
    fs = floyd_->Read(kMetaVersion, value);
    if (fs.ok()) {
      if (value == "") {
        version_ = -1;
      } else {
        version_ = std::stoi(value);
      }
      LOG(INFO) << "Got version " << version_;
      return Status::OK();
    } else {
      LOG(ERROR) << "Read floyd full_meta failed in InitVersion: " << fs.ToString() << ", try again";
      sleep(1);
    }
  }
}

Status ZPMetaServer::Set(const std::string &key, const std::string &value) {
  floyd::Status fs = floyd_->Write(key, value);
  if (fs.ok()) {
    return Status::OK();
  } else {
    LOG(ERROR) << "Floyd write failed: " << fs.ToString();
    return Status::Corruption("floyd set error!");
  }
}

Status ZPMetaServer::Get(const std::string &key, std::string &value) {
  floyd::Status fs = floyd_->DirtyRead(key, value);
  if (fs.ok()) {
    return Status::OK();
  } else if (fs.IsNotFound()) {
    return Status::NotFound("not found from floyd");
  } else {
    LOG(ERROR) << "Floyd read failed: " << fs.ToString();
    return Status::Corruption("floyd get error!");
  }
}

Status ZPMetaServer::Delete(const std::string &key) {
  floyd::Status fs = floyd_->Delete(key);
  if (fs.ok()) {
    return Status::OK();
  } else {
    LOG(ERROR) << "Floyd delete failed: " << fs.ToString();
    return Status::Corruption("floyd delete error!");
  }
}

inline bool ZPMetaServer::GetLeader(std::string& ip, int& port) {
  int fy_port = 0;
  bool res = floyd_->GetLeader(ip, fy_port);
  if (res) {
    port = fy_port - kMetaPortShiftFY;
  }
  return res;
}

Status ZPMetaServer::BecomeLeader() {
  ZPMeta::Nodes nodes;
  Status s = GetAllNode(nodes);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetAllNode error in BecomeLeader, error: " << s.ToString();
    return s;
  }
  std::vector<ZPMeta::NodeStatus> alive_nodes;
  GetAllAliveNode(nodes, alive_nodes);
  RestoreNodeAlive(alive_nodes);

  InitVersion();

  return s;
}

inline void ZPMetaServer::CleanLeader() {
  if (leader_cli_) {
    leader_cli_->Close();
    delete leader_cli_;
    leader_cli_ = NULL;
  }
  leader_ip_.clear();
  leader_cmd_port_ = 0;
}
