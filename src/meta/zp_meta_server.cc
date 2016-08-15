#include <glog/logging.h>

#include "slash_string.h"
#include "zp_meta_server.h"
#include "zp_meta.pb.h"

ZPMetaServer::ZPMetaServer(const ZPOptions& options)
  : options_(options), leader_first_time_(true), leader_cli_(NULL), leader_cmd_port_(0){

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
      LOG(ERROR) << "Become to leaader";
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
  ZPMeta::Partitions partitions;
  Status s = GetPartition(1, partitions);
  if (s.ok()) {
    LOG(INFO) << "Restore Node Alive from floyd";
    RestoreNodeAlive(partitions);
  }
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

void ZPMetaServer::CheckNodeAlive() {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);

  std::vector<std::string> need_remove;
  NodeAliveMap::iterator it = node_alive_.begin();
  gettimeofday(&now, NULL);
  for (; it != node_alive_.end(); ++it) {
    if (now.tv_sec - (it->second).tv_sec > NODE_ALIVE_LEASE) {
      need_remove.push_back(it->first);
    }
  }

  std::vector<std::string>::iterator rit = need_remove.begin();
  for (; rit != need_remove.end(); ++rit) {
    node_alive_.erase(*rit);
    update_thread_.ScheduleUpdate(*rit, ZPMetaUpdateOP::OP_REMOVE);
  }
}

void ZPMetaServer::AddNodeAlive(const std::string& ip_port) {
  struct timeval now;
  slash::MutexLock l(&alive_mutex_);
  gettimeofday(&now, NULL);
  node_alive_[ip_port] = now;
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

void ZPMetaServer::RestoreNodeAlive(const ZPMeta::Partitions &partitions) {
  if (!partitions.IsInitialized()) {
    return;
  }
  struct timeval now;
  gettimeofday(&now, NULL);

  slash::MutexLock l(&alive_mutex_);
  ZPMeta::Node master = partitions.master();
  node_alive_[slash::IpPortString(master.ip(), master.port())] = now;
  for (int i = 0; i < partitions.slaves_size(); ++i) {
    const ZPMeta::Node& node = partitions.slaves(i);
    node_alive_[slash::IpPortString(node.ip(), node.port())] = now;
  }
}

Status ZPMetaServer::GetPartition(uint32_t partition_id, ZPMeta::Partitions &partitions) {
  // Load from Floyd
  std::string value;
  slash::Status s = GetFlat(PartitionId2Key(partition_id), value);
  if (!s.ok()) {
    // Error or Not Found
    return s;
  }

  // Deserialization
  partitions.Clear();
  if (!partitions.ParseFromString(value)) {
    LOG(ERROR) << "deserialization current meta failed, value: " << value;
    return slash::Status::Corruption("Parse failed");
  }
  return s;
}

Status ZPMetaServer::SetPartition(uint32_t partition_id, const ZPMeta::Partitions &partitions) {
  std::string new_value;
  if (!partitions.SerializeToString(&new_value)) {
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
