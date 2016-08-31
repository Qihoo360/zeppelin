#include <glog/logging.h>

#include "slash_string.h"
#include "zp_meta_server.h"
#include "zp_meta.pb.h"

ZPMetaServer::ZPMetaServer(const ZPOptions& options)
  : partition_num_(0), options_(options), leader_first_time_(true), leader_cli_(NULL), leader_cmd_port_(0){

  //pthread_rwlock_init(&state_rw_, NULL);
  // Convert ZPOptions
  floyd::Options* fy_options = new floyd::Options();
  fy_options->seed_ip = options.seed_ip;
  fy_options->seed_port = options.seed_port + kMetaPortShiftFY;
  fy_options->local_ip = options.local_ip;
  fy_options->local_port = options.local_port + kMetaPortShiftFY;
  fy_options->data_path = options.data_path;
  fy_options->log_path = options.log_path;

  floyd_ = new floyd::Floyd(*fy_options);

  worker_num_ = 6;
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
  zp_meta_dispatch_thread_->StartThread();

  server_mutex_.Lock();
  server_mutex_.Lock();
  return Status::OK();
}

bool ZPMetaServer::IsLeader() {
  std::string leader_ip;
  int leader_port = 0, leader_cmd_port = 0;
  while (!GetLeader(leader_ip, leader_port)) {
    DLOG(INFO) << "Wait leader ... ";
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
      LOG(ERROR) << "Become to leader";
      BecomeLeader(); // Just become leader
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
    LOG(ERROR) << "connect to leader: " << leader_ip_ << ":" << leader_cmd_port_ << " failed";
  }
  leader_cli_->set_send_timeout(1000);
  leader_cli_->set_recv_timeout(1000);
  return false;
}

Status ZPMetaServer::BecomeLeader() {
  ZPMeta::Nodes nodes;
  Status s = GetAllNode(nodes);
  if (!s.ok()) {
    return s;
  }
  std::vector<ZPMeta::NodeStatus> alive_nodes;
  GetAllAliveNode(nodes, alive_nodes);
  LOG(INFO) << "Restore Node Alive from floyd";
  RestoreNodeAlive(alive_nodes);
  return s;
}

Status ZPMetaServer::RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse &response) {
  slash::MutexLock l(&leader_mutex_);
  pink::Status s = leader_cli_->Send(&request);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to redirect message to leader, " << s.ToString();
    return Status::Corruption(s.ToString());
  }
  s = leader_cli_->Recv(&response); 
  if (!s.ok()) {
    LOG(ERROR) << "Failed to get redirect message response from leader" << s.ToString();
    return Status::Corruption(s.ToString());
  }
  //std::string text_format;
  //google::protobuf::TextFormat::PrintToString(response, &text_format);
  //LOG(INFO) << "recever redirect message response from leader: [" << text_format << "]";
  return Status::OK();
}

void ZPMetaServer::GetAllAliveNode(ZPMeta::Nodes &nodes, std::vector<ZPMeta::NodeStatus> &alive_nodes) {
  for (int i = 0; i < nodes.nodes_size(); i++) {
    const ZPMeta::NodeStatus node_status = nodes.nodes(i);
    if (node_status.status() == 0) {
      alive_nodes.push_back(node_status);
    }
  }
}

#define NEXT(j, n) ((j+1) % n)
#define NNEXT(j, n) ((j+2) % n)
Status ZPMetaServer::Distribute(int num) {
  if (partition_num_ != 0) {
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
  int j = 0;

  ZPMeta::Replicaset replicaset;

  ZPMeta::MetaCmd_Update ms_info;

  for (int i = 0; i < num; i++) {

    replicaset.Clear();
    replicaset.set_id(i);
    ZPMeta::Node *node = replicaset.add_node();
    node->CopyFrom(alive_nodes[j].node());
//    node->mutable_node()->set_ip(alive_nodes[j].node().ip());
//    node->mutable_node()->set_port(alive_nodes[j].node().port());

    node = replicaset.add_node();
    node->CopyFrom(alive_nodes[NEXT(j, an_num)].node());
//    node->mutable_node()->set_ip(alive_nodes[NEXT(j, an_num)].node().ip());
//    node->mutable_node()->set_port(alive_nodes[NEXT(j, an_num)].node().port());

    node = replicaset.add_node();
    node->CopyFrom(alive_nodes[NNEXT(j, an_num)].node());
//    node->mutable_node()->set_ip(alive_nodes[NNEXT(j, an_num)].node().ip());
//    node->mutable_node()->set_port(alive_nodes[NNEXT(j, an_num)].node().port());



    ZPMeta::Partitions *p = ms_info.add_info();
    p->set_id(i);
    p->mutable_master()->CopyFrom(alive_nodes[j].node());
//    p->mutable_master()->set_ip(alive_nodes[j].node().ip());
//    p->mutable_master()->set_port(alive_nodes[j].node().port());

    ZPMeta::Node *slave = p->add_slaves();
    p->mutable_master()->CopyFrom(alive_nodes[NEXT(j, an_num)].node());
//    slave->set_ip(alive_nodes[NEXT(j, an_num)].node().ip());
//    slave->set_port(alive_nodes[NEXT(j, an_num)].node().port());

    slave = p->add_slaves();
    p->mutable_master()->CopyFrom(alive_nodes[NNEXT(j, an_num)].node());
//    slave->set_ip(alive_nodes[NNEXT(j, an_num)].node().ip());
//    slave->set_port(alive_nodes[NNEXT(j, an_num)].node().port());

    s= SetReplicaset(i, replicaset);
    if (!s.ok()) {
      return s;
    }

    j++;
  }
  s = SetMSInfo(ms_info);
  if (!s.ok()) {
    return s;
  }

  floyd::Status fs = floyd_->Write("partition_num", std::to_string(num));
  if (fs.ok()) {
    partition_num_ = num;
    return Status::OK();
  } else {
    LOG(ERROR) << "floyd write partition_num failed: " << fs.ToString();
    return Status::Corruption("floyd set error!");
  }

  update_thread_.ScheduleBroadcast();

}

Status ZPMetaServer::SetMSInfo(const ZPMeta::MetaCmd_Update &cmd) {
  std::string new_value;
  if (!cmd.SerializeToString(&new_value)) {
    LOG(ERROR) << "serialization ZPMetaCmd failed, new value: " <<  new_value;
    return Status::Corruption("Serialize error");
  }
  return SetFlat("full_meta", new_value);
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
    update_thread_.ScheduleUpdate(*rit, ZPMetaUpdateOP::OP_REMOVE);
  }
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
  update_thread_.ScheduleUpdate(ip_port, ZPMetaUpdateOP::OP_ADD);
}

void ZPMetaServer::UpdateNodeAlive(const std::string& ip_port) {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);
  gettimeofday(&now, NULL);
  if (node_alive_.find(ip_port) == node_alive_.end()) {
    LOG(WARNING) << "update unknown node alive:" << ip_port;
    return;
  }
  node_alive_[ip_port] = now;
}

void ZPMetaServer::RestoreNodeAlive(std::vector<ZPMeta::NodeStatus> &alive_nodes) {
  struct timeval now;
  gettimeofday(&now, NULL);

  slash::MutexLock l(&alive_mutex_);
  auto iter = alive_nodes.begin();
  while (iter != alive_nodes.end()) {
    node_alive_[slash::IpPortString(iter->node().ip(), iter->node().port())] = now;
    iter++;
  }
}

//Status ZPMetaServer::GetPartition(uint32_t partition_id, ZPMeta::Partitions &partitions) {
//  // Load from Floyd
//  std::string value;
//  slash::Status s = GetFlat(PartitionId2Key(partition_id), value);
//  if (!s.ok()) {
//    // Error or Not Found
//    return s;
//  }
//
//  // Deserialization
//  partitions.Clear();
//  if (!partitions.ParseFromString(value)) {
//    LOG(ERROR) << "deserialization current meta failed, value: " << value;
//    return slash::Status::Corruption("Parse failed");
//  }
//  return s;
//}
//
//Status ZPMetaServer::SetPartition(uint32_t partition_id, const ZPMeta::Partitions &partition) {
//  std::string new_value;
//  if (!partition.SerializeToString(&new_value)) {
//    LOG(ERROR) << "serialization new meta failed, new value: " <<  new_value;
//    return Status::Corruption("Serialize error");
//  }
//  return SetFlat(PartitionId2Key(partition_id), new_value);
//}
//
Status ZPMetaServer::SetReplicaset(uint32_t partition_id, const ZPMeta::Replicaset &replicaset) {
  std::string new_value;
  if (!replicaset.SerializeToString(&new_value)) {
    LOG(ERROR) << "serialization new meta failed, new value: " <<  new_value;
    return Status::Corruption("Serialize error");
  }
  return SetFlat(PartitionId2Key(partition_id), new_value);
}

Status ZPMetaServer::GetFlat(const std::string &key, std::string &value) {
  floyd::Status fs = floyd_->DirtyRead(key, value);
  if (fs.ok()) {
    return Status::OK();
  } else if (fs.IsNotFound()) {
    return Status::NotFound("not found from floyd");
  } else {
    LOG(ERROR) << "floyd read failed: " << fs.ToString();
    return Status::Corruption("floyd get error!");
  }
}

Status ZPMetaServer::SetFlat(const std::string &key, const std::string &value) {
  floyd::Status fs = floyd_->Write(key, value);
  if (fs.ok()) {
    return Status::OK();
  } else {
    LOG(ERROR) << "floyd write failed: " << fs.ToString();
    return Status::Corruption("floyd set error!");
  }
}

Status ZPMetaServer::DeleteFlat(const std::string &key) {
  floyd::Status fs = floyd_->Delete(key);
  if (fs.ok()) {
    return Status::OK();
  } else {
    LOG(ERROR) << "floyd delete failed: " << fs.ToString();
    return Status::Corruption("floyd delete error!");
  }
}

int ZPMetaServer::PNums() {
  if (partition_num_ == 0) {
    std::string value;
    floyd::Status fs = floyd_->DirtyRead("partition_num", value);
    if (fs.ok()) {
      partition_num_ = std::stoi(value, nullptr);
    } else {
      LOG(ERROR) << "PNum error, " << fs.ToString();
      return 0;
    }
  }
  return partition_num_;
}

Status ZPMetaServer::GetMSInfo(ZPMeta::MetaCmd_Update &ms_info) {
  std::string value;
  floyd::Status fs = floyd_->DirtyRead("full_meta", value);
  if (fs.ok()) {
    ms_info.Clear();
    if (!ms_info.ParseFromString(value)) {
      LOG(ERROR) << "deserialization full_meta failed, value: " << value;
      return slash::Status::Corruption("Parse failed");
    }
    return Status::OK();
  } else {
    LOG(ERROR) << "floyd read full_meta failed: " << fs.ToString();
    return Status::Corruption("floyd delete error!");
  }
}

Status ZPMetaServer::GetAllNode(ZPMeta::Nodes &nodes) {
  // Load from Floyd
  std::string value;
  floyd::Status fs = floyd_->DirtyRead("nodes", value);
  if (fs.ok()) {
    // Deserialization
    nodes.Clear();
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

bool ZPMetaServer::FindNode(ZPMeta::Nodes &nodes, const std::string &ip, int port) {
  for (int i = 0; i < nodes.nodes_size(); ++i) {
    const ZPMeta::NodeStatus& node_status = nodes.nodes(i);
    if (ip == node_status.node().ip() && port == node_status.node().port()) {
      return true;
    }
  }
  return false;
}

Status ZPMetaServer::AddNode(const std::string &ip, int port) {
  std::string new_value;
  ZPMeta::Nodes nodes;
  Status s = GetAllNode(nodes);
  bool should_add = false;
  if (s.ok()) {
    if (FindNode(nodes, ip, port)) {
      return SetNodeStatus(nodes, ip, port, 0);
    } else {
      should_add = true;
    }
  } else if (s.IsNotFound() || should_add) {
    ZPMeta::NodeStatus *node_status = nodes.add_nodes();
    node_status->mutable_node()->set_ip(ip);
    node_status->mutable_node()->set_port(port);
    node_status->set_status(0);
    if (!nodes.SerializeToString(&new_value)) {
      LOG(ERROR) << "serialization new meta failed, new value: " <<  new_value;
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

  return s;
}

Status ZPMetaServer::SetNodeStatus(ZPMeta::Nodes& nodes, const std::string &ip, int port, int status /*0-UP 1-DOWN*/) {
  std::string new_value;
  for (int i = 0; i < nodes.nodes_size(); ++i) {
    ZPMeta::NodeStatus* node_status = nodes.mutable_nodes(i);
    if (ip == node_status->node().ip() && port == node_status->node().port()) {
      if (node_status->status() == status) {
        return Status::OK();
      } else {
        node_status->set_status(status);
        if (!nodes.SerializeToString(&new_value)) {
          LOG(ERROR) << "serialization new meta failed, new value: " <<  new_value;
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

Status ZPMetaServer::OffNode(const std::string &ip, int port) {

  ZPMeta::Nodes nodes;
  ZPMeta::MetaCmd_Update ms_info;
  Status s = GetAllNode(nodes);

  if (!s.ok()) {
    return s;
  }
  s = SetNodeStatus(nodes, ip, port, 1);
  if (!s.ok()) {
    return s;
  }
  s = GetMSInfo(ms_info);
  if (!s.ok()) {
    return s;
  }

  ZPMeta::Node tmp;

  for (int i = 0; i < ms_info.info_size(); ++i) {
    ZPMeta::Partitions* p = ms_info.mutable_info(i);
    if (ip != p->master().ip() || port != p->master().port()) {
      continue;
    }

    tmp.CopyFrom(p->master());
    ZPMeta::Node* master = p->mutable_master();
    if (p->slaves_size() > 0) {
      master->CopyFrom(p->slaves(0));
      ZPMeta::Node* first = p->mutable_slaves(0);
      first->CopyFrom(tmp);
    }
    tmp.Clear();
    
  }

  s = SetMSInfo(ms_info);
  if (!s.ok()) {
    return s;
  }

}
