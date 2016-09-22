#include <glog/logging.h>
#include <google/protobuf/text_format.h>

#include "slash_string.h"
#include "zp_meta_server.h"
#include "zp_meta.pb.h"

enum ZPNodeStatus {
  kNodeUp,
  kNodeDown
};

ZPMetaServer::ZPMetaServer(const ZPOptions& options)
  : worker_num_(6), options_(options), version_(-1), leader_first_time_(true), leader_cli_(NULL), leader_cmd_port_(0) {

  // Convert ZPOptions
  floyd::Options fy_options;
  fy_options.seed_ip = options.seed_ip;
  fy_options.seed_port = options.seed_port + kMetaPortShiftFY;
  fy_options.local_ip = options.local_ip;
  fy_options.local_port = options.local_port + kMetaPortShiftFY;
  fy_options.data_path = options.data_path;
  fy_options.log_path = options.log_path;

  floyd_ = new floyd::Floyd(fy_options);

  for (int i = 0; i < worker_num_ ; ++i) {
    zp_meta_worker_thread_[i] = new ZPMetaWorkerThread(kMetaWorkerCronInterval);
  }
  zp_meta_dispatch_thread_ = new ZPMetaDispatchThread(options.local_port + kMetaPortShiftCmd, worker_num_, zp_meta_worker_thread_, kMetaDispathCronInterval);
}

ZPMetaServer::~ZPMetaServer() {
  delete zp_meta_dispatch_thread_;
  for (int i = 0; i < worker_num_; ++i) {
    delete zp_meta_worker_thread_[i];
  }
  CleanLeader();
  delete floyd_;
}

Status ZPMetaServer::Start() {
  LOG(INFO) << "ZPMetaServer started on port:" << options_.local_port << ", seed is " << options_.seed_ip.c_str() << ":" <<options_.seed_port;
  floyd_->Start();
  std::string leader_ip;
  int leader_port;
  while (!GetLeader(leader_ip, leader_port)) {
    LOG(INFO) << "Wait leader ... ";
    // Wait leader election
    sleep(1);
  }
  LOG(INFO) << "Got Leader: " << leader_ip << ":" << leader_port;
  InitVersion();
  zp_meta_dispatch_thread_->StartThread();

  server_mutex_.Lock();
  server_mutex_.Lock();
  return Status::OK();
}

Status ZPMetaServer::InitVersion() {
  std::string value;
  ZPMeta::MetaCmdResponse_Pull ms_info;
  while(1) {
    floyd::Status fs = floyd_->Read(ZP_META_KEY_MT, value);
    if (fs.ok()) {
      if (value == "") {
        version_ = -1;
      } else {
        ms_info.Clear();
        if (!ms_info.ParseFromString(value)) {
          LOG(ERROR) << "Deserialization full_meta failed in InitVersion, value: " << value;
        }
        version_ = ms_info.version();
      }
      LOG(INFO) << "Got version " << version_;
      return Status::OK();
//    } else if (fs.IsNotFound()) {
//      version_ = -1;
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

Status ZPMetaServer::Distribute(int num) {
  slash::MutexLock l(&node_mutex_);
  if (PartitionNums() != 0) {
    return Status::Corruption("Already Distribute");
  }

 
  Status s;
  ZPMeta::Nodes nodes;
  s = GetAllNode(nodes);
  if (!s.ok()) {
    return s;
  }

  std::vector<ZPMeta::NodeStatus> alive_nodes;
  GetAllAliveNode(nodes, alive_nodes);

  if (alive_nodes.empty()) {
    return Status::Corruption("no nodes");
  }

  int an_num = alive_nodes.size();

  ZPMeta::Replicaset replicaset;

  ZPMeta::MetaCmdResponse_Pull ms_info;
  ms_info.set_version(0);

  for (int i = 0; i < num; i++) {

    replicaset.Clear();
    replicaset.set_id(i);
    ZPMeta::Node *node = replicaset.add_node();
    node->CopyFrom(alive_nodes[i % an_num].node());

    node = replicaset.add_node();
    node->CopyFrom(alive_nodes[(i + 1) % an_num].node());

    node = replicaset.add_node();
    node->CopyFrom(alive_nodes[(i + 2) % an_num].node());

    ZPMeta::Partitions *p = ms_info.add_info();
    p->set_id(i);
    p->mutable_master()->CopyFrom(alive_nodes[i % an_num].node());

    ZPMeta::Node *slave = p->add_slaves();
    slave->CopyFrom(alive_nodes[(i + 1) % an_num].node());

    slave = p->add_slaves();
    slave->CopyFrom(alive_nodes[(i + 2) % an_num].node());

    s= SetReplicaset(i, replicaset);
    if (!s.ok()) {
      return s;
    }
  }
  s = SetMSInfo(ms_info);
  if (!s.ok()) {
    return s;
  }

  std::string text_format;
  google::protobuf::TextFormat::PrintToString(ms_info, &text_format);
  LOG(INFO) << "ms_info : [" << text_format << "]";

  floyd::Status fs = floyd_->Write(ZP_META_KEY_PN, std::to_string(num));
  if (fs.ok()) {
    return Status::OK();
  } else {
    LOG(ERROR) << "Floyd write partition_num failed: " << fs.ToString();
    return Status::Corruption("floyd set error!");
  }

  return Status::OK();
}

Status ZPMetaServer::AddNodeAlive(const std::string& ip_port) {
  {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);
  gettimeofday(&now, NULL);
  node_alive_[ip_port] = now;
  }

  std::string ip;
  int port;
  if (!slash::ParseIpPortString(ip_port, ip, port)) {
    return Status::Corruption("parse ip_port error");
  }
  Status s = AddNode(ip, port);
  if (!s.ok()) {
    return s;
  }

  LOG(INFO) << "Add Node Alive";
  update_thread_.ScheduleUpdate(ip_port, ZPMetaUpdateOP::kOpAdd);
  return Status::OK();
}

Status ZPMetaServer::GetAllNode(ZPMeta::Nodes &nodes) {
  // Load from Floyd
  std::string value;
  floyd::Status fs = floyd_->DirtyRead(ZP_META_KEY_ND, value);
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

void ZPMetaServer::GetAllAliveNode(ZPMeta::Nodes &nodes, std::vector<ZPMeta::NodeStatus> &alive_nodes) {
  for (int i = 0; i < nodes.nodes_size(); i++) {
    const ZPMeta::NodeStatus node_status = nodes.nodes(i);
    if (node_status.status() == 0) {
      alive_nodes.push_back(node_status);
    }
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

Status ZPMetaServer::SetNodeStatus(ZPMeta::Nodes& nodes, const std::string &ip, int port,
    int status /*0-kNodeUp 1-kNodeDown*/) {
  std::string new_value;
  for (int i = 0; i < nodes.nodes_size(); ++i) {
    ZPMeta::NodeStatus* node_status = nodes.mutable_nodes(i);
    if (ip == node_status->node().ip() && port == node_status->node().port()) {
      if (node_status->status() == status) {
        return Status::OK();
      } else {
        node_status->set_status(status);
        if (!nodes.SerializeToString(&new_value)) {
          LOG(ERROR) << "Serialization new meta failed, new value: " <<  new_value;
          return Status::Corruption("Serialize error");
        }
        floyd::Status fs = floyd_->Write("nodes", new_value);
        if (fs.ok()) {
          return Status::OK();
        } else {
          LOG(ERROR) << "SetNodeStatus, floyd write failed: " << fs.ToString();
          return Status::Corruption("floyd set error!");
        }
      }
    } else {
      continue;
    }
  }
  return Status::NotFound("not found this node");
}

Status ZPMetaServer::AddNode(const std::string &ip, int port) {
  std::string new_value;
  ZPMeta::Nodes nodes;

  slash::MutexLock l(&node_mutex_);

  Status s = GetAllNode(nodes);
  bool should_add = false;
  if (s.ok()) {
    if (FindNode(nodes, ip, port)) {
      return SetNodeStatus(nodes, ip, port, ZPNodeStatus::kNodeUp);
    } else {
      should_add = true;
    }
  }
  if (s.IsNotFound() || should_add) {
    ZPMeta::NodeStatus *node_status = nodes.add_nodes();
    node_status->mutable_node()->set_ip(ip);
    node_status->mutable_node()->set_port(port);
    node_status->set_status(ZPNodeStatus::kNodeUp);
    if (!nodes.SerializeToString(&new_value)) {
      LOG(ERROR) << "serialization new meta failed, new value: " <<  new_value;
      return Status::Corruption("Serialize error");
    }
    floyd::Status fs = floyd_->Write(ZP_META_KEY_ND, new_value);
    if (fs.ok()) {
      return Status::OK();
    } else {
      LOG(ERROR) << "SetNodeStatus, floyd write failed: " << fs.ToString();
      return Status::Corruption("floyd set error!");
    }
  }
  return s;
}

Status ZPMetaServer::OffNode(const std::string &ip, int port) {
  ZPMeta::Nodes nodes;
  ZPMeta::MetaCmdResponse_Pull ms_info;

  slash::MutexLock l(&node_mutex_);

  Status s = GetAllNode(nodes);
  if (!s.ok()) {
    LOG(ERROR) << "GetAllNode error in OffNode, error: " << s.ToString();
    return s;
  }
  s = SetNodeStatus(nodes, ip, port, ZPNodeStatus::kNodeDown);
  if (!s.ok()) {
    LOG(ERROR) << "SetNodeStatus error in OffNode, error: " << s.ToString();
    return s;
  }
  s = GetMSInfo(ms_info);
  if (!s.ok()) {
    LOG(ERROR) << "GetMSInfo error in OffNode, error: " << s.ToString();
    return s;
  }

  ZPMeta::Node tmp;
  bool should_rewrite = false;

  for (int i = 0; i < ms_info.info_size(); ++i) {
    ZPMeta::Partitions* p = ms_info.mutable_info(i);
    if (ip != p->master().ip() || port != p->master().port()) {
      continue;
    }

    should_rewrite = true;
    tmp.CopyFrom(p->master());
    ZPMeta::Node* master = p->mutable_master();
    if (p->slaves_size() > 0) {
      master->CopyFrom(p->slaves(0));
      ZPMeta::Node* first = p->mutable_slaves(0);
      first->CopyFrom(tmp);
    }
    tmp.Clear();
  }

  if (!should_rewrite) {
    return Status::OK();
  }

  int v = ms_info.version();
  if (v != version_) {
    LOG(WARNING) << "Version not match, version_ = " << version_ << " version in floyd = " << v;
  }
  ms_info.set_version(version_ + 1);

  s = SetMSInfo(ms_info);
  if (s.ok()) {
    version_++; 
  } else {
    LOG(ERROR) << "SetMSInfo error in OffNode, error: " << s.ToString();
  }
  return s;
}

void ZPMetaServer::CheckNodeAlive() {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);

  std::vector<std::string> need_remove;
  NodeAliveMap::iterator it = node_alive_.begin();
  gettimeofday(&now, NULL);
  for (; it != node_alive_.end(); ++it) {
    if (now.tv_sec - (it->second).tv_sec > NODE_META_TIMEOUT_M) {
      need_remove.push_back(it->first);
    }
  }

  std::vector<std::string>::iterator rit = need_remove.begin();
  for (; rit != need_remove.end(); ++rit) {
    node_alive_.erase(*rit);
    update_thread_.ScheduleUpdate(*rit, ZPMetaUpdateOP::kOpRemove);
  }
}

void ZPMetaServer::UpdateNodeAlive(const std::string& ip_port) {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);
  gettimeofday(&now, NULL);
  if (node_alive_.find(ip_port) == node_alive_.end()) {
    LOG(WARNING) << "Update unknown node alive:" << ip_port;
    return;
  }
  node_alive_[ip_port] = now;
}

Status ZPMetaServer::SetReplicaset(uint32_t partition_id, const ZPMeta::Replicaset &replicaset) {
  std::string new_value;
  if (!replicaset.SerializeToString(&new_value)) {
    LOG(ERROR) << "Serialization new meta failed, new value: " <<  new_value;
    return Status::Corruption("Serialize error");
  }
  return Set(PartitionId2Key(partition_id), new_value);
}

Status ZPMetaServer::SetMSInfo(const ZPMeta::MetaCmdResponse_Pull &cmd) {
  std::string new_value;
  if (!cmd.SerializeToString(&new_value)) {
    LOG(ERROR) << "Serialization full_meta failed, new value: " <<  new_value;
    return Status::Corruption("Serialize error");
  }
  return Set(ZP_META_KEY_MT, new_value);
}

Status ZPMetaServer::GetMSInfo(ZPMeta::MetaCmdResponse_Pull &ms_info) {
  std::string value;
  floyd::Status fs = floyd_->DirtyRead(ZP_META_KEY_MT, value);
  if (fs.ok()) {
    ms_info.Clear();
    if (!ms_info.ParseFromString(value)) {
      LOG(ERROR) << "Deserialization full_meta failed, value: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    return Status::OK();
  } else {
    LOG(ERROR) << "Floyd read full_meta failed: " << fs.ToString();
    return Status::Corruption("floyd delete error!");
  }
}

int ZPMetaServer::PartitionNums() {
  std::string value;
  floyd::Status fs = floyd_->DirtyRead(ZP_META_KEY_PN, value);
  if (fs.ok()) {
    return std::stoi(value, nullptr);
  } else {
    LOG(ERROR) << "PartitionNum error, " << fs.ToString();
    return 0;
  }
}

bool ZPMetaServer::IsLeader() {
  std::string leader_ip;
  int leader_port = 0, leader_cmd_port = 0;
  while (!GetLeader(leader_ip, leader_port)) {
    LOG(INFO) << "Wait leader ... ";
    // Wait leader election
    sleep(1);
  }
  LOG(INFO) << "Leader: " << leader_ip << ":" << leader_port;

  slash::MutexLock l(&leader_mutex_);
  leader_cmd_port = leader_port + kMetaPortShiftCmd;
  if (leader_ip == leader_ip_ && leader_cmd_port == leader_cmd_port_) {
    // has connected to leader
    return false;
  }
  
  // Leader changed
  if (leader_ip == options_.local_ip && 
      leader_port == options_.local_port) {
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

Status ZPMetaServer::BecomeLeader() {
  ZPMeta::Nodes nodes;
  Status s = GetAllNode(nodes);
  if (!s.ok()) {
    LOG(ERROR) << "GetAllNode error in BecomeLeader, error: " << s.ToString();
    return s;
  }
  std::vector<ZPMeta::NodeStatus> alive_nodes;
  GetAllAliveNode(nodes, alive_nodes);
  RestoreNodeAlive(alive_nodes);

  InitVersion();

  return s;
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

inline void ZPMetaServer::CleanLeader() {
  if (leader_cli_) {
    leader_cli_->Close();
    delete leader_cli_;
    leader_cli_ = NULL;
  }
  leader_ip_.clear();
  leader_cmd_port_ = 0;
}

inline bool ZPMetaServer::GetLeader(std::string& ip, int& port) {
  int fy_port = 0;
  bool res = floyd_->GetLeader(ip, fy_port);
  if (res) {
    port = fy_port - kMetaPortShiftFY;
  }
  return res;
}

std::string PartitionId2Key(uint32_t id) {
  std::string key(ZP_META_KEY_PREFIX);
  key += std::to_string(id);
  return key;
}

