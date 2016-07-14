#include <glog/logging.h>

#include "zp_meta_server.h"

ZPMetaServer::ZPMetaServer(const ZPOptions& options)
  : options_(options) {

  pthread_rwlock_init(&state_rw_, NULL);

  // Convert ZPOptions
  floyd::Options* fy_options = new floyd::Options();
  fy_options->seed_ip = options.seed_ip;
  fy_options->seed_port = options.seed_port + kMetaPortShiftFY;
  fy_options->local_ip = options.local_ip;
  fy_options->local_port = options.local_port + kMetaPortShiftFY;
  fy_options->data_path = options.data_path;
  fy_options->log_path = options.log_path;

  floyd_ = new floyd::Floyd(*fy_options);
  
}

ZPMetaServer::~ZPMetaServer() {
  pthread_rwlock_destroy(&state_rw_);
}

Status ZPMetaServer::Start() {
  LOG(INFO) << "ZPMetaServer started on port:" << options_.local_port << ", seed is " << options_.seed_ip.c_str() << ":" <<options_.seed_port;
  floyd_->Start();
  server_mutex_.Lock();
  server_mutex_.Lock();
  return Status::OK();
}
