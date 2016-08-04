#include <glog/logging.h>

#include "zp_meta_server.h"

ZPMetaServer::ZPMetaServer(const ZPOptions& options)
  : options_(options) {

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

  //pthread_rwlock_destroy(&state_rw_);
}

Status ZPMetaServer::Start() {
  LOG(INFO) << "ZPMetaServer started on port:" << options_.local_port << ", seed is " << options_.seed_ip.c_str() << ":" <<options_.seed_port;
  floyd_->Start();
  zp_meta_dispatch_thread_->StartThread();

  server_mutex_.Lock();
  server_mutex_.Lock();
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

Status ZPMetaServer::Set(const std::string &key, const std::string &value) {
  floyd::Status fs = floyd_->Write(key, value);
	if (fs.ok()) {
    return Status::OK();
  } else {
    LOG(ERROR) << "floyd write failed: " << fs.ToString();
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
    LOG(ERROR) << "floyd read failed: " << fs.ToString();
    return Status::Corruption("floyd get error!");
  }
}

//Status ZPMetaServer::Delete(const std::string &key) {
//  floyd::Status fs = floyd_->Write(key, value);
//	if (fs.ok()) {
//    return Status::OK();
//  } else {
//    LOG(ERROR) << "floyd write failed: " << fs.ToString();
//    return Status::Corruption("floyd set error!");
//  }
//}
