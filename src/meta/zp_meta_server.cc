#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/repeated_field.h>

#include "slash_string.h"
#include "zp_meta_server.h"
#include "zp_meta.pb.h"

ZPMetaServer::ZPMetaServer()
  : should_exit_(false), started_(false), version_(-1), worker_num_(6), leader_cli_(NULL), leader_first_time_(true), leader_ip_(""), leader_cmd_port_(0) {

  floyd::Options fy_options;
  local_ip_ = fy_options.local_ip = g_zp_conf->local_ip();
  seed_ip_ = fy_options.seed_ip = g_zp_conf->seed_ip();
  local_port_ = fy_options.local_port = g_zp_conf->local_port() + kMetaPortShiftFY;
  seed_port_ = fy_options.seed_port = g_zp_conf->seed_port() + kMetaPortShiftFY;
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
  floyd::Status fs = floyd_->Start();
  if (!fs.ok()) {
    LOG(ERROR) << "Start floyd failed: " << fs.ToString();
    return;
  }
  std::string leader_ip;
  int leader_port;
  while (!GetLeader(&leader_ip, &leader_port) && !should_exit_) {
    LOG(INFO) << "Wait leader ... ";
    // Wait leader election
    sleep(1);
  }
  Status s;
  if (!should_exit_) {
    LOG(INFO) << "Got Leader: " << leader_ip << ":" << leader_port;
    while (!should_exit_) {
      s = InitVersion();
      if (s.ok()) {
        break;
      }
      sleep(1);
    }
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

void ZPMetaServer::AddMetaUpdateTaskDequeFromFront(const ZPMetaUpdateTaskDeque &task_deque) {
  slash::MutexLock l(&task_mutex_);
  for (auto iter = task_deque.rbegin(); iter != task_deque.rend(); iter++) {
    task_deque_.push_front(*iter);
  }

}

Status ZPMetaServer::DoUpdate(ZPMetaUpdateTaskDeque task_deque) {
  ZPMeta::Table table_info;
  ZPMeta::Nodes nodes;

  slash::MutexLock l(&node_mutex_);
  Status s = GetAllNodes(&nodes);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetAllNodes error in DoUpdate, error: " << s.ToString();
    return s;
  }

  std::vector<std::string> tables;
  s = GetTableList(&tables);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetTableList error in DoUpdate, error: " << s.ToString();
    return s;
  }


  bool sth_wrong = false;

/*
 * Step 1. Apply every update task on Nodes
 */
  if (!ProcessUpdateNodes(task_deque, &nodes)) {
    sth_wrong = true;
    return Status::Corruption("sth wrong");
  }

/*
 * Step 2. Apply every update task on every Table
 */
  bool should_update_version = false;
//  bool should_update_table_set = false;

  for (auto it = tables.begin(); it != tables.end(); it++) {
    table_info.Clear();
    s = GetTableInfo(*it, &table_info);
    if (!s.ok() && !s.IsNotFound()) {
      LOG(ERROR) << "GetTableInfo error in DoUpdate, table: " << *it << " error: " << s.ToString();
      sth_wrong = true;
      continue;
    }
    if (!ProcessUpdateTableInfo(task_deque, nodes, &table_info, &should_update_version)) {
      sth_wrong = true;
      continue;
    }
  }

/*
 * Step 3. AddClearStuckTaskifNeeded
 */

  AddClearStuckTaskIfNeeded(task_deque);

/*
 * Step 4. Check whether should we add version after Step [1-2]
 * or is there kOpAddVersion task in task deque, if true,
 * add version
 */

  if (should_update_version || ShouldRetryAddVersion(task_deque)) {
    s = AddVersion();
    if (!s.ok()) {
      return s;
    }
  }

  if (sth_wrong) {
    return Status::Corruption("sth wrong");
  }

  return Status::OK();
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
  UpdateTask task = {ZPMetaUpdateOP::kOpAdd, ip_port, "", -1};
  AddMetaUpdateTask(task);
  return Status::OK();
}

void ZPMetaServer::AddMetaUpdateTask(const UpdateTask &task) {
  slash::MutexLock l(&task_mutex_);
  task_deque_.push_back(task);
}

void ZPMetaServer::CheckNodeAlive() {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);

  gettimeofday(&now, NULL);
  auto it = node_alive_.begin();
  while (it != node_alive_.end()) {
    if (now.tv_sec - (it->second).tv_sec > kNodeMetaTimeoutM) {
      UpdateTask task = {ZPMetaUpdateOP::kOpRemove, it->first, "", -1};
      AddMetaUpdateTask(task);
      it = node_alive_.erase(it);
    } else {
      it++;
    }
  }
}

void ZPMetaServer::ScheduleUpdate() {
  slash::MutexLock l(&task_mutex_);
  if (!task_deque_.empty()) {
    update_thread_->ScheduleUpdate(task_deque_);
    task_deque_.clear();
  }
}

Status ZPMetaServer::GetMSInfo(const std::set<std::string> &tables, ZPMeta::MetaCmdResponse_Pull *ms_info) {
  ms_info->Clear();
  ZPMeta::Table table_info;
  ZPMeta::Table *t;
  Status s;

  ms_info->set_version(version_);
  for (auto it = tables.begin(); it != tables.end(); it++) {
    s = GetTableInfo(*it, &table_info);
    if (s.ok()) {
      t = ms_info->add_info();
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

Status ZPMetaServer::GetTableListForNode(const std::string &ip_port, std::set<std::string> *tables) {
  tables->clear();
  slash::MutexLock l(&node_mutex_);
  auto iter = nodes_.find(ip_port);
  if (iter != nodes_.end()) {
    *tables = iter->second;
  }
  return Status::OK();
}

Status ZPMetaServer::RemoveSlave(const std::string &table, int partition, const ZPMeta::Node &node) {
  bool valid = false;
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  LOG(INFO) << "RemoveSlave " << table << " " << partition << " " << ip_port;

  ZPMeta::Nodes nodes;
  Status s = GetAllNodes(&nodes);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetAllNodes error in RemoveSlave, error: " << s.ToString();
    return s;
  }

  if (!FindNode(nodes, node.ip(), node.port())) {
    return Status::NotFound("not found");
  }

  ZPMeta::Table table_info;
  s = GetTableInfo(table, &table_info);
  if (!s.ok()) {
    return s;
  }
  
  if (partition < 0 || partition >= table_info.partitions_size()) {
    return Status::Corruption("invalid partition");
  }

  ZPMeta::Partitions p = table_info.partitions(partition);

  if (p.master().ip() == node.ip() && p.master().port() == node.port()) {
    LOG(INFO) << "RemoveSlave: can not remove master";
    return Status::Corruption("can not remove master");
  }

  int i = 0;
  for (i = 0; i < p.slaves_size(); i++) {
    if (p.slaves(i).ip() == node.ip() && p.slaves(i).port() == node.port()) {
      valid = true;
      break;
    }
  }

  if (valid) {
    UpdateTask task = {ZPMetaUpdateOP::kOpRemoveSlave, ip_port, table, partition};
    LOG(INFO) << "RemoveSlave PushTask" << task.op << " " << ip_port << " " << table << " " << partition;
    AddMetaUpdateTask(task);
    return Status::OK();
  } else {
    return Status::Corruption("RemoveSlave: node & partition dismatch");
  }

}

Status ZPMetaServer::SetMaster(const std::string &table, int partition, const ZPMeta::Node &node) {
  bool valid = false;
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  LOG(INFO) << "SetMaster " << table << " " << partition << " " << ip_port;

  {
  slash::MutexLock l(&node_mutex_);
  auto iter = nodes_.find(ip_port);
  if (iter == nodes_.end()) {
    return Status::Corruption("this node doesn't hold any table");
  }
  auto it = iter->second.find(table);
  if (it == iter->second.end()) {
    return Status::Corruption("table & node Dismatch");
  }
  }

  ZPMeta::Table table_info;
  Status s = GetTableInfo(table, &table_info);
  if (!s.ok()) {
    return s;
  }
  
  if (partition < 0 || partition >= table_info.partitions_size()) {
    return Status::Corruption("invalid partition");
  }

  ZPMeta::Partitions p = table_info.partitions(partition);

  if (p.master().ip() == node.ip() && p.master().port() == node.port()) {
    LOG(INFO) << "SetMaster: Already master";
    return Status::Corruption("already master");
  }

  for (int i = 0; i < p.slaves_size(); i++) {
    if (p.slaves(i).ip() == node.ip() && p.slaves(i).port() == node.port()) {
      valid = true;
    }
  }

  if (valid) {
    UpdateTask task = {ZPMetaUpdateOP::kOpSetMaster, ip_port, table, partition};
    LOG(INFO) << "SetMaster PushTask" << task.op << " " << ip_port << " " << table << " " << partition;
    AddMetaUpdateTask(task);
    return Status::OK();
  } else {
    return Status::Corruption("partition & node Dismatch");
  }

}

Status ZPMetaServer::AddSlave(const std::string &table, int partition, const ZPMeta::Node &node) {
  bool valid = false;
  std::string ip_port = slash::IpPortString(node.ip(), node.port());
  LOG(INFO) << "AddSlave " << table << " " << partition << " " << ip_port;

  ZPMeta::Nodes nodes;
  Status s = GetAllNodes(&nodes);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetAllNodes error in AddSlave, error: " << s.ToString();
    return s;
  }

  if (!FindNode(nodes, node.ip(), node.port())) {
    return Status::NotFound("not found");
  }

  ZPMeta::Table table_info;
  s = GetTableInfo(table, &table_info);
  if (!s.ok()) {
    return s;
  }
  
  if (partition < 0 || partition >= table_info.partitions_size()) {
    return Status::Corruption("invalid partition");
  }

  ZPMeta::Partitions p = table_info.partitions(partition);

  if (p.master().ip() == node.ip() && p.master().port() == node.port()) {
    LOG(INFO) << "AddSlave: Already master";
    return Status::OK();
  }

  int i = 0;
  for (i = 0; i < p.slaves_size(); i++) {
    if (p.slaves(i).ip() == node.ip() && p.slaves(i).port() == node.port()) {
      break;
    }
  }
  if (i == p.slaves_size()) {
    valid = true;
  }

  if (valid) {
    UpdateTask task = {ZPMetaUpdateOP::kOpAddSlave, ip_port, table, partition};
    LOG(INFO) << "AddSlave PushTask" << task.op << " " << ip_port << " " << table << " " << partition;
    AddMetaUpdateTask(task);
    return Status::OK();
  } else {
    return Status::Corruption("AddSlave: Already slave");
  }

}

Status ZPMetaServer::GetAllMetaNodes(ZPMeta::MetaCmdResponse_ListMeta *nodes) {
  std::string value;
  ZPMeta::Nodes allnodes;
  std::vector<std::string> meta_nodes;
  floyd_->GetAllNodes(meta_nodes);

  ZPMeta::MetaNodes *p = nodes->mutable_nodes();
  std::string leader_ip;
  int leader_port;
  bool ret = GetLeader(&leader_ip, &leader_port);
  if (ret) {
    ZPMeta::Node leader;
    ZPMeta::Node *np = p->mutable_leader();
    np->set_ip(leader_ip);
    np->set_port(leader_port);
  }

  std::string ip;
  int port;
  for (auto iter = meta_nodes.begin(); iter != meta_nodes.end(); iter++) {
    if (slash::ParseIpPortString(*iter, ip, port)) {
      if (ret && ip == leader_ip && port-kMetaPortShiftFY == leader_port) {
        continue;
      } else {
        ZPMeta::Node *np = p->add_followers();
        np->set_ip(ip);
        np->set_port(port-kMetaPortShiftFY);
      }
    } else {
      return Status::Corruption("parse ip port error");
    }
  }
  return Status::OK();
}

Status ZPMetaServer::GetTableList(ZPMeta::MetaCmdResponse_ListTable *tables) {
  std::string value;
  ZPMeta::TableName table_name;
  Status s = Get(kMetaTables, value);
  if (s.ok()) {
    if (!table_name.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table failed, error: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    ZPMeta::TableName *p = tables->mutable_tables();
    p->CopyFrom(table_name);
  }
  return s;
}

Status ZPMetaServer::GetAllNodes(ZPMeta::MetaCmdResponse_ListNode *nodes) {
  std::string value;
  ZPMeta::Nodes allnodes;
  Status s = Get(kMetaNodes, value);
  if (s.ok()) {
    if (!allnodes.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization nodes failed, error: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    ZPMeta::Nodes *p = nodes->mutable_nodes();
    p->CopyFrom(allnodes);
  }
  return s;
}

Status ZPMetaServer::Distribute(const std::string &name, int num) {
  slash::MutexLock l(&node_mutex_);
  std::string value;
  Status s;
  s = Get(name, value);
  if (s.ok()) {
    return Status::Corruption("Already Created");
  }
 
  ZPMeta::Nodes nodes;
  s = GetAllNodes(&nodes);
  if (!s.ok()) {
    return s;
  }

  std::vector<ZPMeta::NodeStatus> t_alive_nodes;
  GetAllAliveNode(nodes, &t_alive_nodes);
  if (t_alive_nodes.size() < 3) {
    return Status::Corruption("have no enough alive nodes to create replicats");
  }

  std::vector<ZPMeta::NodeStatus> alive_nodes;
  Reorganize(t_alive_nodes, &alive_nodes);

  int an_num = alive_nodes.size();

  ZPMeta::Table table;

  table.set_name(name);

  for (int i = 0; i < num; i++) {
    ZPMeta::Partitions *p = table.add_partitions();
    p->set_id(i);
    p->set_state(ZPMeta::PState::ACTIVE);
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

  s = UpdateTableList(name);
  if (!s.ok()) {
    LOG(ERROR) << "UpdateTableList error: " << s.ToString();
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
  int pnum = 3+num-1 ;
  for (auto iter = alive_nodes.begin(); pnum && iter != alive_nodes.end(); iter++) {
    ip_port = slash::IpPortString(iter->node().ip(), iter->node().port());
    auto it = nodes_.find(ip_port);
    if (it != nodes_.end()) {
      it->second.insert(name);
    } else {
      std::set<std::string> ts;
      ts.insert(name);
      nodes_.insert(std::unordered_map<std::string, std::set<std::string> >::value_type(ip_port, ts));
    }
    pnum--;
  }

  DebugNodes();
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(table, &text_format);
  LOG(INFO) << "table_info : [" << text_format << "]";

  return Status::OK();
}

void ZPMetaServer::UpdateOffset(const ZPMeta::MetaCmd_Ping &ping) {
  slash::MutexLock l(&offset_mutex_);
  std::string ip_port;
  std::string p;
  LOG(INFO) << "Size: " << ping.offset_size();
  for (int i = 0; i < ping.offset_size(); i++) {
//    LOG(INFO) << "process " << i;
    auto iter = offset_.find(ping.offset(i).table_name());
    if (iter == offset_.end()) {
      LOG(INFO) << "Table: Not Found " << ping.offset(i).table_name() << ", insert!";
      std::unordered_map<std::string, std::unordered_map<std::string, NodeOffset> > partition2node;
      offset_.insert(std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, NodeOffset> > >::value_type(ping.offset(i).table_name(), partition2node));
      iter = offset_.find(ping.offset(i).table_name());
    }
    p = std::to_string(ping.offset(i).partition());

    ip_port = slash::IpPortString(ping.node().ip(), ping.node().port()); 

    NodeOffset node_offset = {ping.offset(i).filenum(), ping.offset(i).offset()};
    auto it = iter->second.find(p);
    if (it == iter->second.end()) {
      LOG(INFO) << "Partition: Not Found " << p << ", insert!";
      std::unordered_map<std::string, NodeOffset> node2offset;
      node2offset.insert(std::unordered_map<std::string, NodeOffset>::value_type(ip_port, node_offset));
      iter->second.insert(std::unordered_map<std::string, std::unordered_map<std::string, NodeOffset> >::value_type(p, node2offset));
    } else {
      it->second[ip_port] = node_offset;
    }
//    LOG(INFO) << "Insert " << ping.offset(i).table_name() << ", " << p << " : " << ip_port << " -> " << node_offset.filenum << ":" << node_offset.offset;
  }
}

Status ZPMetaServer::DropTable(const std::string &name) {
  slash::MutexLock l(&node_mutex_);
  std::string value;
  Status s;

  s = DeleteTable(name);
  if (!s.ok()) {
    LOG(ERROR) << "DeleteTable error in DropTable, error: " << s.ToString();
    return s;
  }

  s = RemoveTableFromTableList(name);
  if (!s.ok()) {
    LOG(ERROR) << "RemoveTableFromTableList error: " << s.ToString();
    return s;
  }

  s = Set(kMetaVersion, std::to_string(version_+1));
  if (s.ok()) {
    version_++; 
    LOG(INFO) << "Set version in DropTable : " << version_;
  } else {
    LOG(ERROR) << "Set version error in DropTable, error: " << s.ToString();
    return s;
  }

  for (auto iter = nodes_.begin(); iter != nodes_.end(); iter++) {
    iter->second.erase(name);
  }

  DebugNodes();

  return Status::OK();
}

Status ZPMetaServer::InitVersionIfNeeded() {
  std::string value;
  int version = -1;
  floyd::Status fs = floyd_->DirtyRead(kMetaVersion, value);
  if (fs.ok()) {
    version = std::stoi(value);
  } else {
    LOG(ERROR) << "InitVersionIfNeeded error when get version key from floyd: " << fs.ToString();
  }

  slash::MutexLock l(&node_mutex_);
  if (version != version_) {
    return InitVersion();
  }
  return Status::OK();
}

Status ZPMetaServer::RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse *response) {
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
  s = leader_cli_->Recv(response); 
  if (!s.ok()) {
    CleanLeader();
    LOG(ERROR) << "Failed to get redirect message response from leader" << s.ToString();
    return Status::Corruption(s.ToString());
  }
  return Status::OK();
}

bool ZPMetaServer::IsLeader() {
  std::string leader_ip;
  int leader_port = 0, leader_cmd_port = 0;
  while (!should_exit_ && !GetLeader(&leader_ip, &leader_port)) {
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

void ZPMetaServer::DebugNodes() {
  for (auto iter = nodes_.begin(); iter != nodes_.end(); iter++) {
    std::string str = iter->first + " :";
    for (auto it = iter->second.begin(); it != iter->second.end(); it++) {
      str += (" " + *it);
    }
    LOG(INFO) << str;
  }
}

void ZPMetaServer::DebugOffset() {
  slash::MutexLock l(&offset_mutex_);
  for (auto iter = offset_.begin(); iter != offset_.end(); iter++) {
    std::string str = iter->first + " :\n";
    for (auto ite = iter->second.begin(); ite != iter->second.end(); ite++) {
      str += ("    " + ite->first + " : ");
      for (auto it = ite->second.begin(); it != ite->second.end(); it++) {
        str += (it->first + " -> " + std::to_string(it->second.filenum) + ":" + std::to_string(it->second.offset) + "; ");
      }
      str += "\n";
    }
    LOG(INFO) << str;
  } 
}

void ZPMetaServer::InitClientCmdTable() {

  // Ping Command
  Cmd* pingptr = new PingCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::PING), pingptr));

  //Pull Command
  Cmd* pullptr = new PullCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::PULL), pullptr));

  //Init Command
  Cmd* initptr = new InitCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::INIT), initptr));

  //SetMaster Command
  Cmd* setmasterptr = new SetMasterCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::SETMASTER), setmasterptr));

  //AddSlave Command
  Cmd* addslaveptr = new AddSlaveCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::ADDSLAVE), addslaveptr));

  //RemoveSlave Command
  Cmd* removeslaveptr = new RemoveSlaveCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::REMOVESLAVE), removeslaveptr));

  //ListTable Command
  Cmd* listtableptr = new ListTableCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::LISTTABLE), listtableptr));

  //ListNode Command
  Cmd* listnodeptr = new ListNodeCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::LISTNODE), listnodeptr));

  //ListNode Command
  Cmd* listmetaptr = new ListMetaCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::LISTMETA), listmetaptr));

  //DropTable Command
  Cmd* droptableptr = new DropTableCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::DROPTABLE), droptableptr));
}

bool ZPMetaServer::ProcessUpdateTableInfo(const ZPMetaUpdateTaskDeque task_deque, const ZPMeta::Nodes &nodes, ZPMeta::Table *table_info, bool *should_update_version) {

  bool should_update_table_info = false;
  *should_update_version = false;
  std::string ip;
  int port = 0;
  for (auto iter = task_deque.begin(); iter != task_deque.end(); iter++) {
    LOG(INFO) << "process task in ProcessUpdateTableInfo: " << iter->ip_port << ", " << iter->op;
    if (!slash::ParseIpPortString(iter->ip_port, ip, port)) {
      return false;
    }
    if (iter->op == ZPMetaUpdateOP::kOpAdd) {
      DoUpNodeForTableInfo(table_info, ip, port, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpRemove) {
      DoDownNodeForTableInfo(nodes, table_info, ip, port, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpSetMaster) {
      DoSetMasterForTableInfo(table_info, iter->partition, ip, port, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpClearStuck) {
      DoClearStuckForTableInfo(table_info, iter->partition, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpAddSlave) {
      DoAddSlaveForTableInfo(table_info, iter->partition, ip, port, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpRemoveSlave) {
      DoRemoveSlaveForTableInfo(table_info, iter->partition, ip, port, &should_update_table_info);
    }
  }

  if (should_update_table_info) {
    Status s = SetTable(*table_info);
    if (!s.ok()) {
      LOG(ERROR) << "SetTable in ProcessUpdateTableInfo error: " << s.ToString();
      return false;
    } else {
     *should_update_version = true;
    }
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(*table_info, &text_format);
    LOG(INFO) << "table_info : [" << text_format << "]";
  }
  return true;
}

static bool IsAlive(std::vector<ZPMeta::NodeStatus> &alive_nodes, const std::string &ip, const int port) {
  for (auto iter = alive_nodes.begin(); iter != alive_nodes.end(); iter++) {
    if (iter->node().ip() == ip && iter->node().port() == port) {
      return true;
    }
  }
  return false;
}

void ZPMetaServer::DoDownNodeForTableInfo(const ZPMeta::Nodes &nodes, ZPMeta::Table *table_info, const std::string ip, int port, bool *should_update_table_info) {

  std::vector<ZPMeta::NodeStatus> alive_nodes;
  GetAllAliveNode(nodes, &alive_nodes);

  ZPMeta::Node tmp;

  for (int i = 0; i < table_info->partitions_size(); ++i) {
    ZPMeta::Partitions* p = table_info->mutable_partitions(i);
    if (ip != p->master().ip() || port != p->master().port()) {
      continue;
    }

    *should_update_table_info = true;
    tmp.CopyFrom(p->master());
    ZPMeta::Node* master = p->mutable_master();
    int slaves_size = p->slaves_size();
    LOG(INFO) << "slaves_size:" << slaves_size;

    int32_t max_filenum = -1;
    int64_t max_offset = -1;
    int candidate = -1;

    int j = 0;
    int32_t filenum;
    int64_t offset;
    for (j = 0; j < slaves_size; j++) {
      if (IsAlive(alive_nodes, p->slaves(j).ip(), p->slaves(j).port())) {
        bool ret = GetSlaveOffset(table_info->name(), slash::IpPortString(p->slaves(j).ip(), p->slaves(j).port()), i, &filenum, &offset);
        if (ret && (filenum > max_filenum || (filenum == max_filenum && offset > max_offset))) {
          candidate = j;
          max_filenum = filenum;
          max_offset = offset;
        }
      }
    }
    if (candidate != -1) {
      LOG(INFO) << "Use Slave " << candidate << " " << p->slaves(candidate).ip() << " " << p->slaves(candidate).port();
      master->CopyFrom(p->slaves(candidate));
      ZPMeta::Node* first = p->mutable_slaves(candidate);
      first->CopyFrom(tmp);
    } else {
      LOG(INFO) << "No Slave to use";
      ZPMeta::Node *slave = p->add_slaves();
      slave->CopyFrom(tmp);

      master->set_ip("");
      master->set_port(0);
    }
    tmp.Clear();
  }
}

void ZPMetaServer::DoRemoveSlaveForTableInfo(ZPMeta::Table *table_info, int partition, const std::string &ip, int port, bool *should_update_table_info) {

  if (partition < 0 || partition >= table_info->partitions_size()) {
    LOG(ERROR) << "invalid partition num in DoRemoveSlaveForTableInfo for " << table_info->name() << " : " << partition;
    return;
  }

  ZPMeta::Partitions* p = table_info->mutable_partitions(partition);

  if (p->master().ip() == ip && p->master().port() == port) {
    return;
  }

  int slaves_size = p->slaves_size();
  for (int j = 0; j < slaves_size; j++) {
    if (p->slaves(j).ip() == ip && p->slaves(j).port() == port) {
      *should_update_table_info = true;
      ZPMeta::Node* slave = p->mutable_slaves(j);
      slave->CopyFrom(p->slaves(slaves_size-1));
      p->mutable_slaves()->RemoveLast();
    }
  }

  bool should_update_nodes = true;

  ZPMeta::Partitions part;
  for (int i = 0; i < table_info->partitions_size(); i++) {
    part = table_info->partitions(i);
    if (part.master().ip() == ip && part.master().port() == port) {
        should_update_nodes = false;
    }
    for (int j = 0; j < part.slaves_size(); j++) {
      if (part.slaves(j).ip() == ip && part.slaves(j).port() == port) {
        should_update_nodes = false;
        break;
      }
    }
    if (!should_update_nodes) {
      break;
    }
  }

  if (should_update_nodes) {
    auto iter = nodes_.find(slash::IpPortString(ip, port));
    if (iter != nodes_.end()) {
      auto it = iter->second.find(table_info->name());
      if (it != iter->second.end()) {
        LOG(INFO) << "DoRemoveSlaveForTableInfo: erase " << table_info->name() << " from nodes_[" << slash::IpPortString(ip, port) << "]";
        iter->second.erase(table_info->name());
        if (iter->second.empty()) {
          LOG(INFO) << "DoRemoveSlaveForTableInfo: erase " << slash::IpPortString(ip, port) << " from nodes_";
          nodes_.erase(slash::IpPortString(ip, port));
        }
      }
    }
  }
  DebugNodes();

}

void ZPMetaServer::DoSetMasterForTableInfo(ZPMeta::Table *table_info, int partition, const std::string &ip, int port, bool *should_update_table_info) {
  

  if (partition < 0 || partition >= table_info->partitions_size()) {
    LOG(ERROR) << "invalid partition num in DoSetMasterForTableInfo for " << table_info->name() << " : " << partition;
    return;
  }

  ZPMeta::Partitions* p = table_info->mutable_partitions(partition);
  if (p->master().ip() == ip && p->master().port() == port) {
    return;
  }
  int slaves_size = p->slaves_size();
  for(int j = 0; j < slaves_size; j++) {
    if (p->slaves(j).ip() == ip && p->slaves(j).port() == port) {
      *should_update_table_info = true;

      ZPMeta::Node tmp;
      tmp.CopyFrom(p->master());

      ZPMeta::Node* master = p->mutable_master();
      master->CopyFrom(p->slaves(j));
      ZPMeta::Node* slave = p->mutable_slaves(j);
      slave->CopyFrom(tmp);

      p->set_state(ZPMeta::PState::STUCK);
      break;
    }
  }
}

void ZPMetaServer::DoAddSlaveForTableInfo(ZPMeta::Table *table_info, int partition, const std::string &ip, int port, bool *should_update_table_info) {

  if (partition < 0 || partition >= table_info->partitions_size()) {
    LOG(ERROR) << "invalid partition num in DoAddSlaveForTableInfo for " << table_info->name() << " : " << partition;
    return;
  }

  ZPMeta::Partitions* p = table_info->mutable_partitions(partition);

  if (p->master().ip() == ip && p->master().port() == port) {
    return;
  }

  int slaves_size = p->slaves_size();
  for (int j = 0; j < slaves_size; j++) {
    if (p->slaves(j).ip() == ip && p->slaves(j).port() == port) {
      return;
    }
  }

  *should_update_table_info = true;
  ZPMeta::Node *slave = p->add_slaves();
  slave->set_ip(ip);
  slave->set_port(port);

  auto iter = nodes_.find(slash::IpPortString(ip, port));
  if (iter != nodes_.end()) {
    auto it = iter->second.find(table_info->name());
    if (it == iter->second.end()) {
      LOG(INFO) << "DoAddSlaveForTableInfo: insert " << table_info->name() << " into nodes_[" << slash::IpPortString(ip, port) << "]";
      iter->second.insert(table_info->name());
    }
  } else {
    LOG(INFO) << "DoAddSlaveForTableInfo: insert " << table_info->name() << " into nodes_[" << slash::IpPortString(ip, port) << "]";
    std::set<std::string> ts;
    ts.insert(table_info->name());
    nodes_.insert(std::unordered_map<std::string, std::set<std::string> >::value_type(slash::IpPortString(ip, port), ts));
  }
  DebugNodes();
}

void ZPMetaServer::DoUpNodeForTableInfo(ZPMeta::Table *table_info, const std::string ip, int port, bool *should_update_table_info) {
  int slaves_size = 0;
  for (int i = 0; i < table_info->partitions_size(); ++i) {
    ZPMeta::Partitions* p = table_info->mutable_partitions(i);
    if (p->master().ip() == "" && p->master().port() == 0) {
      slaves_size = p->slaves_size();
      for(int j = 0; j < slaves_size; j++) {
        if (p->slaves(j).ip() == ip && p->slaves(j).port() == port) {
          *should_update_table_info = true;
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
}

void ZPMetaServer::DoClearStuckForTableInfo(ZPMeta::Table *table_info, int partition, bool *should_update_table_info) {
  ZPMeta::Partitions* p = table_info->mutable_partitions(partition);
  if (p->state() == ZPMeta::PState::STUCK) {
    p->set_state(ZPMeta::PState::ACTIVE);
    *should_update_table_info = true;
  }
}

enum ZPNodeStatus {
  kNodeUp,
  kNodeDown
};

bool ZPMetaServer::ProcessUpdateNodes(const ZPMetaUpdateTaskDeque task_deque, ZPMeta::Nodes *nodes) {
  bool should_update_nodes = false;
  std::string ip;
  int port = 0;
  for (auto iter = task_deque.begin(); iter != task_deque.end(); iter++) {
    LOG(INFO) << "process task in ProcessUpdateNode: " << iter->ip_port << ", " << iter->op;
    if (!slash::ParseIpPortString(iter->ip_port, ip, port)) {
      return false;
    }
    if (iter->op == ZPMetaUpdateOP::kOpAdd) {
      if (FindNode(*nodes, ip, port)) {
        SetNodeStatus(nodes, ip, port, kNodeUp, &should_update_nodes);
      } else {
        ZPMeta::NodeStatus *node_status = nodes->add_nodes();
        node_status->mutable_node()->set_ip(ip);
        node_status->mutable_node()->set_port(port);
        node_status->set_status(kNodeUp);
        should_update_nodes = true;
      }
    } else if (iter->op == ZPMetaUpdateOP::kOpRemove) {
      SetNodeStatus(nodes, ip, port, kNodeDown, &should_update_nodes);
    }
  }
  if (should_update_nodes) {
    Status s = SetNodes(*nodes);
    if (!s.ok()) {
      LOG(ERROR) << "SetNodes in ProcessUpdateNodes error: " << s.ToString();
      return false;
    }
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(*nodes, &text_format);
    LOG(INFO) << "nodes : [" << text_format << "]";
  }
  return true;
}

void ZPMetaServer::AddClearStuckTaskIfNeeded(const ZPMetaUpdateTaskDeque &task_deque) {
  for (auto iter = task_deque.begin(); iter != task_deque.end(); iter++) {
    if (iter->op == ZPMetaUpdateOP::kOpSetMaster) {
      UpdateTask task = *iter;
      task.op = ZPMetaUpdateOP::kOpClearStuck;
      LOG(INFO) << "Add Clear Stuck Task for " << task.table << " : " << task.partition; 
      AddMetaUpdateTask(task);
    }
  }
}

bool ZPMetaServer::ShouldRetryAddVersion(const ZPMetaUpdateTaskDeque task_deque) {
  for (auto iter = task_deque.begin(); iter != task_deque.end(); iter++) {
    if (iter->op == ZPMetaUpdateOP::kOpAddVersion) {
      return true;
    }
  }
  return false;
}

bool ZPMetaServer::GetSlaveOffset(const std::string &table, const std::string &ip_port, const int partition, int32_t *filenum, int64_t *offset) {
  slash::MutexLock l(&offset_mutex_);
  auto iter = offset_.find(table);
  if (iter == offset_.end()) {
    return false;
  }
  auto ite = iter->second.find(std::to_string(partition));
  if (ite == iter->second.end()) {
    return false;
  }
  auto it = ite->second.find(ip_port);
  if (it == ite->second.end()) {
    return false;
  }

  *filenum = it->second.filenum;
  *offset = it->second.offset;
  return true;
}

void ZPMetaServer::Reorganize(const std::vector<ZPMeta::NodeStatus> &t_alive_nodes, std::vector<ZPMeta::NodeStatus> *alive_nodes) {
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
  bool done = false;
  while (!done) {
    for (auto iter_m = m.begin(); iter_m != m.end(); iter_m++) {
      if (iter_m->second.empty()) {
        empty_count++;
        if (empty_count == msize) {
          done = true;
          break;
        }
        continue;
      } else {
        LOG(INFO) << "PUSH " << iter_m->second.back().node().ip() << ":" << iter_m->second.back().node().port();
        alive_nodes->push_back(iter_m->second.back());
        iter_m->second.pop_back();
      }
    }
  }
}

void ZPMetaServer::SetNodeStatus(ZPMeta::Nodes *nodes, const std::string &ip, int port, int status, bool *should_update_node) {
  for (int i = 0; i < nodes->nodes_size(); i++) {
    ZPMeta::NodeStatus* node_status = nodes->mutable_nodes(i);
    if (ip == node_status->node().ip() && port == node_status->node().port()) {
      if (node_status->status() == status) {
      } else {
        *should_update_node = true;
        node_status->set_status(status);
      }
    }
  }
}

void ZPMetaServer::GetAllAliveNode(const ZPMeta::Nodes &nodes, std::vector<ZPMeta::NodeStatus> *alive_nodes) {
  for (int i = 0; i < nodes.nodes_size(); i++) {
    const ZPMeta::NodeStatus node_status = nodes.nodes(i);
    if (node_status.status() == 0) {
      alive_nodes->push_back(node_status);
    }
  }
}

Status ZPMetaServer::GetTableInfo(const std::string &table, ZPMeta::Table *table_info) {
  std::string value;
  floyd::Status fs = floyd_->DirtyRead(table, value);
  if (fs.ok()) {
    table_info->Clear();
    if (!table_info->ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table_info failed, table: " << table << " value: " << value;
      return Status::Corruption("Parse failed");
    }
    return Status::OK();
  } else if (fs.IsNotFound()) {
    return Status::NotFound("table_info not found");
  } else {
    LOG(ERROR) << "Floyd read table_info failed: " << fs.ToString();
    return Status::Corruption("Read table_info failed!");
  }
}

bool ZPMetaServer::FindNode(const ZPMeta::Nodes &nodes, const std::string &ip, int port) {
  for (int i = 0; i < nodes.nodes_size(); ++i) {
    const ZPMeta::NodeStatus& node_status = nodes.nodes(i);
    if (ip == node_status.node().ip() && port == node_status.node().port()) {
      return true;
    }
  }
  return false;
}

void ZPMetaServer::RestoreNodeAlive(const std::vector<ZPMeta::NodeStatus> &alive_nodes) {
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

Status ZPMetaServer::RemoveTableFromTableList(const std::string &name) {
  std::string value;
  ZPMeta::TableName table_name;
  ZPMeta::TableName new_table_name;
  Status s = Get(kMetaTables, value);
  if (s.ok() || s.IsNotFound()) {
    if (!table_name.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table_name failed, error: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    for (int i = 0; i < table_name.name_size(); ++i) {
      if (table_name.name(i) != name) {
        new_table_name.add_name(table_name.name(i));
      }
    }

    std::string text_format;
    google::protobuf::TextFormat::PrintToString(new_table_name, &text_format);
    LOG(INFO) << "Tables : [" << text_format << "]";

    if (!new_table_name.SerializeToString(&value)) {
      LOG(ERROR) << "Serialization new_table_name failed, value: " <<  value;
      return Status::Corruption("Serialize error");
    }
    return Set(kMetaTables, value);
  }
  return s;
}

Status ZPMetaServer::GetTableList(std::vector<std::string> *tables) {
  std::string value;
  ZPMeta::TableName table_name;
  Status s = Get(kMetaTables, value);
  if (s.ok()) {
    if (!table_name.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table failed, error: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    tables->clear();
    for (int i = 0; i< table_name.name_size(); i++) {
      tables->push_back(table_name.name(i));
    }
  }
  return s;
}

Status ZPMetaServer::UpdateTableList(const std::string &name) {
  std::string value;
  ZPMeta::TableName table_name;
  Status s = Get(kMetaTables, value);
  if (s.ok() || s.IsNotFound()) {
    if (!table_name.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table_name failed, error: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    table_name.add_name(name);
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(table_name, &text_format);
    LOG(INFO) << "Tables : [" << text_format << "]";

    if (!table_name.SerializeToString(&value)) {
      LOG(ERROR) << "Serialization table_name failed, value: " <<  value;
      return Status::Corruption("Serialize error");
    }
    return Set(kMetaTables, value);
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

Status ZPMetaServer::DeleteTable(const std::string &name) {
  return Delete(name);
}

Status ZPMetaServer::SetNodes(const ZPMeta::Nodes &nodes) {
  std::string new_value;
  if (!nodes.SerializeToString(&new_value)) {
    LOG(ERROR) << "Serialization nodes failed, new value: " <<  new_value;
    return Status::Corruption("Serialize error");
  }

  return Set(kMetaNodes, new_value);
}

Status ZPMetaServer::GetAllNodes(ZPMeta::Nodes *nodes) {
  // Load from Floyd
  std::string value;
  floyd::Status fs = floyd_->DirtyRead(kMetaNodes, value);
  nodes->Clear();
  if (fs.ok()) {
    // Deserialization
    if (!nodes->ParseFromString(value)) {
      LOG(ERROR) << "deserialization AllNodeInfo failed, value: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    return Status::OK();
  } else if (fs.IsNotFound()) {
    return Status::NotFound("No node in cluster Now");
  } else {
    LOG(ERROR) << "GetAllNodes, floyd read failed: " << fs.ToString();
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

  int tmp_version = -1;

// Get Version
  fs = floyd_->Read(kMetaVersion, value);
  if (fs.ok()) {
    if (value == "") {
      tmp_version = -1;
    } else {
      tmp_version = std::stoi(value);
    }
  } else {
    LOG(ERROR) << "Read floyd version failed in InitVersion: " << fs.ToString() << ", try again";
    return Status::Corruption("Read Version error");
  }

// Update nodes_
  fs = floyd_->Read(kMetaTables, value);
  LOG(INFO) << "InitVersion read tables, ret: " << fs.ToString();
  if (fs.ok()) {
    if (value != "") {
      if (!tables.ParseFromString(value)) {
        LOG(ERROR) << "Deserialization table failed, error: " << value;
        return Status::Corruption("Parse failed");
      }
      
      //node_mutex_ have already been hold in InitVersionIfNeeded
      nodes_.clear();
      for (int i = 0; i < tables.name_size(); i++) {
        fs = floyd_->Read(tables.name(i), value);
        if (!fs.ok()) {
          LOG(ERROR) << "Read floyd table_info failed in InitVersion: " << fs.ToString() << ", try again";
          return Status::Corruption("Read table_info error");
        }
        if (!table_info.ParseFromString(value)) {
          LOG(ERROR) << "Deserialization table_info failed, table: " << tables.name(i) << " value: " << value;
          return Status::Corruption("Parse failed");
        }

        for (int j = 0; j < table_info.partitions_size(); j++) {
          partition = table_info.partitions(j);

          if (partition.master().ip() != "" && partition.master().port() != -1) {
            ip_port = slash::IpPortString(partition.master().ip(), partition.master().port());
            auto iter = nodes_.find(ip_port);
            if (iter != nodes_.end()) {
              iter->second.insert(tables.name(i));
            } else {
              std::set<std::string> ts;
              ts.insert(tables.name(i));
              nodes_.insert(std::unordered_map<std::string, std::set<std::string> >::value_type(ip_port, ts));
            }
          }

          for (int k = 0; k < partition.slaves_size(); k++) {
            ip_port = slash::IpPortString(partition.slaves(k).ip(), partition.slaves(k).port());
            auto iter = nodes_.find(ip_port);
            if (iter != nodes_.end()) {
              iter->second.insert(tables.name(i));
            } else {
              std::set<std::string> ts;
              ts.insert(tables.name(i));
              nodes_.insert(std::unordered_map<std::string, std::set<std::string> >::value_type(ip_port, ts));
            }
          }
        }
      }
      DebugNodes();
    }
  } else {
    LOG(ERROR) << "Read floyd tables failed in InitVersion: " << fs.ToString() << ", try again";
    return Status::Corruption("Read tables error");
  }

  // Update Version
  version_ = tmp_version;
  LOG(INFO) << "Got version " << version_;
  
  return Status::OK();
}

Status ZPMetaServer::AddVersion() {
  Status s = Set(kMetaVersion, std::to_string(version_+1));
  if (s.ok()) {
    version_++;
    LOG(INFO) << "AddVersion success: " << version_;
  } else {
    LOG(INFO) << "AddVersion failed: " << s.ToString();
    return Status::Corruption("AddVersion Error");
  }
  return Status::OK();
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

inline bool ZPMetaServer::GetLeader(std::string *ip, int *port) {
  int fy_port = 0;
  bool res = floyd_->GetLeader(*ip, fy_port);
  if (res) {
    *port = fy_port - kMetaPortShiftFY;
  }
  return res;
}

Status ZPMetaServer::BecomeLeader() {
  ZPMeta::Nodes nodes;
  Status s = GetAllNodes(&nodes);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "GetAllNodes error in BecomeLeader, error: " << s.ToString();
    return s;
  }
  std::vector<ZPMeta::NodeStatus> alive_nodes;
  GetAllAliveNode(nodes, &alive_nodes);
  RestoreNodeAlive(alive_nodes);
  while (!should_exit_) {
    s = InitVersion();
    if (s.ok()) {
      break;
    }
    sleep(1);
  }

  return s;
}

void ZPMetaServer::CleanLeader() {
  if (leader_cli_) {
    leader_cli_->Close();
    delete leader_cli_;
    leader_cli_ = NULL;
  }
  leader_ip_.clear();
  leader_cmd_port_ = 0;
}
