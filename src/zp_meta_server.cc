#include "zp_meta_server.h"

#include <glog/logging.h>

ZPMetaServer::ZPMetaServer(const ZPOptions& options)
  : options_(options) {

  pthread_rwlock_init(&state_rw_, NULL);

  // Create nemo handle
  nemo::Options option;
  std::string db_path = options.data_path;

  LOG(INFO) << "Loading data...";
  db_ = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(db_path, option));
  assert(db_);

  LOG(INFO) << " Success";

  // Create thread
 // worker_num_ = 2; 
 // for (int i = 0; i < worker_num_; i++) {
 //   zp_worker_thread_[i] = new ZPWorkerThread(kWorkerCronInterval);
 // }

  //zp_dispatch_thread_ = new ZPDispatchThread(options_.local_port, worker_num_, zp_worker_thread_, kDispatchCronInterval);
  //zp_heartbeat_thread_ = new ZPHeartbeatThread(options_.local_port + kPortShiftHeartbeat, kHeartbeatCronInterval);
  
  //zp_meta_thread_ = new ZPMetaThread(options.seed_ip, options.seed_port);

  logger_ = new Binlog(options_.log_path);

  // TODO rm
  //LOG(INFO) << "local_host " << options_.local_ip << ":" << options.local_port;
}

ZPMetaServer::~ZPMetaServer() {
  //delete zp_heartbeat_thread_;
  //delete zp_dispatch_thread_;
  //delete zp_meta_thread_;

 // for (int i = 0; i < worker_num_; i++) {
 //   delete zp_worker_thread_[i];
 // }
  
  pthread_rwlock_destroy(&state_rw_);
}

Status ZPMetaServer::Start() {
  //zp_dispatch_thread_->StartThread();
  //zp_meta_thread_->StartThread();

  LOG(INFO) << "ZPMetaServer started on port:" <<  options_.local_port << ", seed is " << options_.seed_ip.c_str() << ":" << options_.seed_port;
  server_mutex_.Lock();
  server_mutex_.Lock();
  return Status::OK();
}
