#include "src/meta/zp_meta_server.h"

#include <cstdlib>
#include <ctime>
#include <sys/resource.h>
#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/repeated_field.h>

#include "pink/include/pink_cli.h"
#include "slash/include/env.h"

#include "include/zp_meta.pb.h"

std::string NodeOffsetKey(const std::string& table, int partition_id,
    const std::string& ip, int port) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s_%u_%s:%u",
      table.c_str(), partition_id, ip.c_str(), port);
  return std::string(buf);
}

ZPMetaServer::ZPMetaServer()
  : should_exit_(false), started_(false) {
  LOG(INFO) << "ZPMetaServer start initialization";

  // Try to raise the file descriptor
  struct  rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) != -1) {
    if (limit.rlim_cur < (rlim_t)g_zp_conf->max_file_descriptor_num()) {
      // rlim_cur could be set by any user while rlim_max are
      // changeable only by root.
      rlim_t previous_limit = limit.rlim_cur;
      limit.rlim_cur = g_zp_conf->max_file_descriptor_num();
      if(limit.rlim_cur > limit.rlim_max) {
        limit.rlim_max = g_zp_conf->max_file_descriptor_num();
      }
      if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
        LOG(WARNING) << "your 'limit -n ' of " << previous_limit << " is not enough for zeppelin to start, zeppelin have successfully reconfig it to " << limit.rlim_cur;
      } else {
        LOG(FATAL) << "your 'limit -n ' of " << previous_limit << " is not enough for zeppelin to start, but zeppelin can not reconfig it: " << strerror(errno) <<" do it by yourself";
      };
    }
  } else {
    LOG(WARNING) << "getrlimir error: " << strerror(errno);
  }

  floyd::Options fy_options;

  // We should replace ip/port to ip:port along with the PortShift;
  fy_options.members = g_zp_conf->meta_addr();
  for (auto it = fy_options.members.begin(); it != fy_options.members.end(); it++) {
    std::replace(it->begin(), it->end(), '/', ':');
    std::string ip;
    int port;
    slash::ParseIpPortString(*it, ip, port);
    *it = slash::IpPortString(ip, port + kMetaPortShiftFY);
  }
  fy_options.local_ip = g_zp_conf->local_ip();
  fy_options.local_port = g_zp_conf->local_port() + kMetaPortShiftFY;
  fy_options.path = g_zp_conf->data_path();

  floyd::Floyd::Open(fy_options, &floyd_);

  info_store_ = new ZPMetaInfoStore(floyd_);

  Status s = ZPMetaMigrateRegister::Create(floyd_, &migrate_register_);
  if (!s.ok()) {
    LOG(FATAL) << "Failed to create migrate register, error: " << s.ToString();
  }

  condition_cron_ = new ZPMetaConditionCron(&offset_map_, update_thread_);

  cmds_.reserve(300);
  InitClientCmdTable();  

  conn_factory_ = new ZPMetaClientConnFactory(); 
  server_handle_ = new ZPMetaServerHandle();
  server_thread_ = pink::NewDispatchThread(
      g_zp_conf->local_port() + kMetaPortShiftCmd, 
      g_zp_conf->meta_thread_num(),
      conn_factory_,
      kMetaDispathCronInterval,
      kMetaDispathQueueSize,
      server_handle_);

  server_thread_->set_thread_name("ZPMetaDispatch");
  // TODO(anan) set keepalive
  //server_thread_->set_keepalive_timeout(kIdleTimeout);

  update_thread_ = new ZPMetaUpdateThread();
}

ZPMetaServer::~ZPMetaServer() {
  server_thread_->StopThread();
  delete server_thread_;
  delete conn_factory_;
  delete server_handle_;

  DestoryCmdTable(cmds_);
  delete update_thread_;
  CleanLeader();
  delete migrate_register_;
  delete info_store_;
  delete floyd_;
  LOG(INFO) << "ZPMetaServer Delete Done";
}

void ZPMetaServer::Start() {
  LOG(INFO) << "ZPMetaServer started on port:" << g_zp_conf->local_port();
  
  Status s = Status::Incomplete("Info store load incompleted");
  while (!should_exit_ && !s.ok()) {
    sleep(1);
    s = info_store_->Load();
    LOG(INFO) << "Info store load from floyd, ret: " << s.ToString();
  }

  if (should_exit_) {
    return;
  }

  if (pink::RetCode::kSuccess != server_thread_->StartThread()) {
    LOG(INFO) << "Disptch thread start failed";
    return;
  }

  while (!should_exit_) {
    DoTimingTask();
    int sleep_count = kMetaCronWaitCount;
    while (!should_exit_ && sleep_count-- > 0) {
      usleep(MetaCronInterval * 1000);
    }
  }
  return Status::OK();
}

Cmd* ZPMetaServer::GetCmd(const int op) {
  return GetCmdFromTable(op, cmds_);
}

Status ZPMetaServer::GetMetaInfoByTable(const std::string& table,
    ZPMeta::MetaCmdResponse_Pull *ms_info) {
  ms_info->set_version(info_store_.epoch());
  ZPMeta::Table* table_info = ms_info->add_info();
  s = info_store_.GetTableMeta(table, table_info);
  if (!s.ok()) {
    LOG(WARNING) << "Get table meta for node failed: " << s.ToString()
      << ", table: " << table;
    return s;
  }
  return Status::OK();
}

Status GetMetaInfoByNode(const std::string& ip_port,
    ZPMeta::MetaCmdResponse_Pull *ms_info) {
  // Get epoch first and because the epoch was updated at last
  // no lock is needed here
  ms_info->set_version(info_store_.epoch());

  std::set<std::string> tables;
  Status s = info_store_.GetTablesForNode(ip_port, &tables);
  if (!s.ok()) {
    LOG(WARNING) << "Get all tables for Node failed: " << s.ToString()
      << ", node: " << ip_port;
    return s;
  }

  for (const auto& t : tables) {
    ZPMeta::Table* table_info = ms_info->add_info();
    s = info_store_.GetTableMeta(t, table_info);
    if (!s.ok()) {
      LOG(WARNING) << "Get one table meta for node failed: " << s.ToString()
        << ", node: " << ip_port << ", table: " << t;
      return s;
    }
  }
  return Status::OK();
}



void ZPMetaServer::UpdateNodeAlive(const std::string& ip_port) {
  if (info_store_.UpdateNodeAlive(ip_port)) {
    // new node
    LOG(INFO) << "PendingUpdate to add Node Alive " << ip_port;
    update_thread_->PendingUpdate(UpdateTask(ZPMetaUpdateOP::kOpAdd, ip_port));
  }
  return Stauts::OK();
}

void ZPMetaServer::CheckNodeAlive() {
  std::set<std::string> nodes;
  info_store_.FetchExpiredNode(&nodes);
  for (const auto& n : nodes) {
    LOG(INFO) << "PendingUpdate to remove Node Alive " << ip_port;
    update_thread_->PendingUpdate(UpdateTask(ZPMetaUpdateOP::kOpRemove, n));
  }
}

Status ZPMetaServer::GetAllMetaNodes(ZPMeta::MetaCmdResponse_ListMeta *nodes) {
  std::string value;
  ZPMeta::Nodes allnodes;
  std::vector<std::string> meta_nodes;
  floyd_->GetAllNodes(meta_nodes);

  ZPMeta::MetaNodes *p = nodes->mutable_nodes();
  std::string leader_ip;
  int leader_port = 0;
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

Status ZPMetaServer::GetMetaStatus(std::string *result) {
  floyd_->GetServerStatus(*result);
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

Status ZPMetaServer::Migrate(int epoch, const std::vector<ZPMeta::RelationCmdUnit>& diffs) {
  if (epoch != version_) {
    return Status::InvalidArgument("Expired epoch");
  }
  
  // Register
  assert(!diffs.empty());
  Status s = migrate_register_->Init(diffs);
  if (!s.ok()) {
    LOG(WARNING) << "Migrate register Init failed, error: " << s.ToString();
    return s;
  }

  // retry some time to ProcessMigrate
  int retry = kInitMigrateRetryNum;
  do {
    s = ProcessMigrate();
  
  } while (s.IsIncomplete() && retry-- > 0);
  return s;
}

Status ZPMetaServer::ProcessMigrate() {
  // Get next 
  std::vector<ZPMeta::RelationCmdUnit> diffs;
  Status s = migrate_register_->GetN(kMetaMigrateOnceCount, &diffs);
  if (s.IsNotFound()) {
    LOG(INFO) << "No migrate to be processed";
  } else if (!s.ok()) {
    LOG(WARNING) << "Get next N migrate diffs failed, error: " << s.ToString();
    return s;
  }

  bool has_process = false;
  for (const auto& diff : diffs) {
    s = AddSlave(diff.table(), diff.partition(), diff.right());
    if (!s.ok()) {
      LOG(WARNING) << "AddSlave when process migrate failed: " << s.ToString()
        << ", Partition: " << diff.table() << "_" << diff.partition()
        << ", new node: " << diff.right().ip() << ":" << diff.right().port();
      continue;
    }
    
    // Begin offset condition wait
    condition_cron_.AddCronTask(
        OffsetCondition(diff.table(), diff.partition(),
          diff.left(), diff.right()),
        UpdateTask(ZPMetaUpdateOP::kOpRemoveSlave,
          slash::IpPortString(diff.left().ip(), diff.left().port()),
          diff.table(), diff.parititon()));

    has_process = true;
  }

  if (!has_process) {
    LOG(WARNING) << "No migrate item be success begin";
    return Status::Incomplete("no migrate item begin");
  }
  return Status::OK();
}

bool ZPMetaServer::IsLeader() {
  slash::MutexLock l(&(leader_joint_.leader_mutex));
  if (leader_joint_.ip == g_zp_conf->local_ip() && 
      leader_joint_.port == g_zp_conf->local_port() + kMetaPortShiftCmd) {
    return true;
  }
  return false;
}

Status ZPMetaServer::RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse *response) {
  // Do not try to connect leader even failed,
  // and leave this to RefreshLeader process in cron
  slash::MutexLock l(&(leader_joint_.mutex));
  if (leader_joint_.cli == NULL) {
    LOG(ERROR) << "Failed to RedirectToLeader, cli is NULL";
    return Status::Corruption("no leader connection");
  }
  Status s = leader_joint_.cli->Send(&request);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to send redirect message to leader, error: "
      << s.ToString() << ", leader: " << leader_joint_.ip
      << ":" << leader_joint_.port;  
    return s;
  }
  s = leader_joint_.cli->Recv(response); 
  if (!s.ok()) {
    LOG(ERROR) << "Failed to recv redirect message from leader, error: "
      << s.ToString() << ", leader: " << leader_joint_.ip
      << ":" << leader_joint_.port;  
  }
  return s;
}

Status ZPMetaServer::RefreshLeader() {
  std::string leader_ip;
  int leader_port = 0, leader_cmd_port = 0;
  if (!GetLeader(&leader_ip, &leader_port)) {
    LOG(WARNING) << "No leader yet";
    return Status::Incomplete("No leader yet");
  }

  // No change
  leader_cmd_port = leader_port + kMetaPortShiftCmd;
  slash::MutexLock l(&(leader_joint_.leader_mutex));
  if (leader_ip == leader_joint_.ip
      && leader_cmd_port == leader_joint_.port) {
    return Stauts::OK();
  }
  
  // Leader changed
  LOG(WARNING) << "Leader changed from: "
    << leader_joint_.ip << ":" << leader_joint_.port
    << ", To: " <<  leader_ip << ":" << leader_cmd_port;
  leader_joint_.CleanLeader();

  // I'm new leader
  Status s;
  if (leader_ip == g_zp_conf->local_ip() && 
      leader_port == g_zp_conf->local_port()) {
    LOG(INFO) << "Become leader: " << leader_ip << ":" << leader_port;
    s = info_store_->RestoreNodeAlive();
    if (!s.ok()) {
      LOG(ERROR) << "Restore Node alive failed: " << s.ToString();
      return s;
    }
    return Status::OK();
  }
  
  // Connect to new leader
  leader_joint_.cli = pink::NewPbCli();
  leader_joint_.ip = leader_ip;
  leader_joint_.port = leader_cmd_port;
  s = leader_joint_.cli->Connect(leader_joint_.ip,
      leader_joint_.port);
  if (!s.ok()) {
    leader_joint_.CleanLeader();
    LOG(ERROR) << "Connect to leader: " << leader_ip << ":" << leader_cmd_port
      << " failed: " << s.ToString();
  } else {
    LOG(INFO) << "Connect to leader: " << leader_ip << ":" << leader_cmd_port
      << " success.";
    leader_joint_.cli->set_send_timeout(1000);
    leader_joint_.cli->set_recv_timeout(1000);
  }
  return s;
}

void ZPMetaServer::InitClientCmdTable() {

  // Ping Command
  Cmd* pingptr = new PingCmd(kCmdFlagsRead | kCmdFlagsRedirect);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::PING), pingptr));

  //Pull Command
  Cmd* pullptr = new PullCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::PULL), pullptr));

  //Init Command
  Cmd* initptr = new InitCmd(kCmdFlagsWrite | kCmdFlagsRedirect);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::INIT), initptr));

  //SetMaster Command
  Cmd* setmasterptr = new SetMasterCmd(kCmdFlagsWrite | kCmdFlagsRedirect);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::SETMASTER), setmasterptr));

  //AddSlave Command
  Cmd* addslaveptr = new AddSlaveCmd(kCmdFlagsWrite | kCmdFlagsRedirect);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::ADDSLAVE), addslaveptr));

  //RemoveSlave Command
  Cmd* removeslaveptr = new RemoveSlaveCmd(kCmdFlagsWrite | kCmdFlagsRedirect);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::REMOVESLAVE), removeslaveptr));

  //ListTable Command
  Cmd* listtableptr = new ListTableCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::LISTTABLE), listtableptr));

  //ListNode Command
  Cmd* listnodeptr = new ListNodeCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::LISTNODE), listnodeptr));

  //ListMeta Command
  Cmd* listmetaptr = new ListMetaCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::LISTMETA), listmetaptr));

  //MetaStatus Command
  Cmd* meta_status_ptr = new MetaStatusCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::METASTATUS), meta_status_ptr));

  //DropTable Command
  Cmd* droptableptr = new DropTableCmd(kCmdFlagsWrite | kCmdFlagsRedirect);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::DROPTABLE), droptableptr));

  // Migrate Command
  Cmd* migrateptr = new MigrateCmd(kCmdFlagsWrite | kCmdFlagsRedirect);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::MIGRATE),
        migrateptr));

  // Cancel Migrate Command
  Cmd* cancel_migrate_ptr = new CancelMigrateCmd(kCmdFlagsWrite | kCmdFlagsRedirect);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::CANCELMIGRATE),
        cancel_migrate_ptr));
  
  //// Check Migrate Command
  //Cmd* check_migrate_ptr = new CheckMigrateCmd(kCmdFlagsWrite);
  //cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::Type::CHECKMIGRATE),
  //      check_migrate_ptr));
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

    int j = 0;
    int candidate = -1;
    NodeOffset max_node_offset;
    NodeOffset node_offset;
    for (j = 0; j < slaves_size; j++) {
      if (IsAlive(alive_nodes, p->slaves(j).ip(), p->slaves(j).port())) {
        bool ret = GetSlaveOffset(table_info->name(), i, p->slaves(j).ip(), p->slaves(j).port(), &node_offset);
        if (ret
            && (candidate == -1  // the first candidate
              || node_offset.filenum > max_node_offset.filenum
              || (node_offset.filenum == max_node_offset.filenum
                && node_offset.offset > max_node_offset.offset))) {
          candidate = j;
          max_node_offset = node_offset;
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

void ZPMetaServer::DoRemoveSlaveForTableInfo(ZPMeta::Table *table_info, const std::string& table, int partition, const std::string &ip, int port, bool *should_update_table_info) {
  if (table_info->name() != table) {
    return;
  }

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

void ZPMetaServer::DoSetMasterForTableInfo(ZPMeta::Table *table_info, const std::string& table, int partition, const std::string &ip, int port, bool *should_update_table_info) {
  if (table_info->name() != table) {
    return;
  }

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

void ZPMetaServer::DoAddSlaveForTableInfo(ZPMeta::Table *table_info, const std::string& table, int partition, const std::string &ip, int port, bool *should_update_table_info) {
  if (table_info->name() != table) {
    return;
  }

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

void ZPMetaServer::DoClearStuckForTableInfo(ZPMeta::Table *table_info, const std::string& table, int partition, bool *should_update_table_info) {
  if (table_info->name() != table) {
    return;
  }

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

void ZPMetaServer::SetNodeStatus(ZPMeta::Nodes *nodes, const std::string &ip, int port, int status, bool *should_update_node) {
  for (int i = 0; i < nodes->nodes_size(); i++) {
    ZPMeta::NodeStatus* node_status = nodes->mutable_nodes(i);
    if (ip == node_status->node().ip() && port == node_status->node().port()) {
      if (node_status->status() == status) {
      } else {
        *should_update_node = true;
        node_status->set_status(status); } }
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
  Status fs = floyd_->DirtyRead(table, value);
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

Status ZPMetaServer::ExistInTableList(const std::string &name, bool *found) {
  std::string value;
  ZPMeta::TableName table_name;
  *found = false;
  Status s = Get(kMetaTables, value);
  if (s.ok()) {
    if (!table_name.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization table_name failed, error: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    for (int i = 0; i < table_name.name_size(); i++) {
      if (table_name.name(i) == name) {
        *found = true;
        break;
      }
    }
    return Status::OK();
  } else if (s.IsNotFound()) {
    return Status::OK();
  }
  return s;
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

Status ZPMetaServer::Get(const std::string &key, std::string &value) {
  Status fs = floyd_->DirtyRead(key, value);
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
  Status fs = floyd_->Delete(key);
  if (fs.ok()) {
    return Status::OK();
  } else {
    LOG(ERROR) << "Floyd delete failed: " << fs.ToString();
    return Status::Corruption("floyd delete error!");
  }
}

inline bool ZPMetaServer::GetLeader(std::string *ip, int *port) {
  int fy_port = 0;
  bool res = floyd_->GetLeader(ip, &fy_port);
  if (res) {
    *port = fy_port - kMetaPortShiftFY;
  }
  return res;
}

// Statistic related
uint64_t ZPMetaServer::last_qps() {
  return last_qps_.load();
}

uint64_t ZPMetaServer::query_num() {
  return query_num_.load();
}

void ZPMetaServer::PlusQueryNum() {
  query_num_++;
}

void ZPMetaServer::ResetLastSecQueryNum() {
  uint64_t cur_time_us = slash::NowMicros();
  last_qps_ = (query_num_ - last_query_num_) * 1000000 / (cur_time_us - last_time_us_ + 1);
  last_query_num_ = query_num_.load();
  last_time_us_ = cur_time_us;
}

void ZPMetaServer::UpdateOffset(const ZPMeta::MetaCmd_Ping &ping) {
  slash::MutexLock l(&(node_offsets_.mutex));
  for (const auto& po : ping.offset()) {
    std::string offset_key = NodeOffsetKey(po.table_name(), po.partition(),
        ping.node().ip(), ping.node().port());
    node_offsets_.offsets[offset_key] = NodeOffset(po.filenum(), po.offset());
  }
}

bool ZPMetaServer::GetSlaveOffset(const std::string &table, int partition,
    const std::string ip, int port, NodeOffset* node_offset) {
  slash::MutexLock l(&(node_offsets_.mutex));
  auto iter = node_offsets_.offsets.find(NodeOffsetKey(table,
        partition, ip, port));
  if (iter == node_offsets_.offsets.end()) {
    return false;
  }
  *node_offset = iter->second;
  return true;
}

void ZPMetaServer::DebugOffset() {
  slash::MutexLock l(&(node_offsets_.mutex));
  for (const auto& item : node_offsets_.offsets) {
    LOG(INFO) << item.first << "->"
      << item.second.filenum << "_" << item.second.offset; 
  }
}

void ZPMetaServer::DoTimingTask() {
  // Refresh Leader joint
  s = RefreshLeader();
  if (!s.ok()) {
    LOG(WARNING) << "Refresh Leader failed: " << s.ToString();
  }

  // Refresh info_store
  if (!IsLeader()) {
    Status s = info_store_.Refresh();
    if (!s.ok()) {
      LOG(WARNING) << "Refresh info_store_ failed: " << s.ToString();
    }
  }

  // Update statistic info
  ResetLastSecQueryNum();
  LOG(INFO) << "ServerQueryNum: " << g_meta_server->query_num()
      << " ServerCurrentQps: " << g_meta_server->last_qps();

  // Check alive
  CheckNodeAlive();
}

