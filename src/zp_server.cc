#include "zp_server.h"

#include <glog/logging.h>

#include "zp_worker_thread.h"
#include "zp_dispatch_thread.h"

ZPServer::ZPServer(const ZPOptions& options)
  : options_(options) {

  // Create nemo handle
  nemo::Options option;
  std::string db_path = options.data_path;

  LOG(INFO) << "Loading data...";
  db_ = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(db_path, option));
  assert(db_);

  LOG(INFO) << " Success";

  // Create thread
  worker_num_ = 2; 
  for (int i = 0; i < worker_num_; i++) {
    zp_worker_thread_[i] = new ZPWorkerThread(1000);
  }

  zp_dispatch_thread_ = new ZPDispatchThread(options_.local_port, worker_num_, zp_worker_thread_, 3000);
}

ZPServer::~ZPServer() {
  delete zp_dispatch_thread_;

  for (int i = 0; i < worker_num_; i++) {
    delete zp_worker_thread_[i];
  }
}

Status ZPServer::Start() {
  zp_dispatch_thread_->StartThread();

  LOG(INFO) << "ZPServer started on port:" <<  options_.local_port << ", seed is " << options_.seed_ip.c_str() << ":" << options_.seed_port;
  server_mutex.Lock();
  server_mutex.Lock();
  return Status::OK();
}

////// ServerConn //////
//ZPServerConn::ZPServerConn(int fd, std::string &ip_port, pink::Thread *thread)
//    : PbConn(fd, ip_port) {
//  server_thread_ = dynamic_cast<ZPServerThread *>(thread);
//}
//
//int ZPServerConn::DealMessage() {
//  uint32_t buf;
//  memcpy((char *)(&buf), rbuf_ + 4, sizeof(uint32_t));
//  uint32_t msg_code = ntohl(buf);
//  LOG_INFO ("deal message msg_code:%d\n", msg_code);
//
//  set_is_reply(true);
//
//  switch (msg_code) {
//    case client::OPCODE::WRITE: {
//      client::Write_Request request;
//      client::Write_Response* response = new client::Write_Response;
//      request.ParseFromArray(rbuf_ + 8, header_len_ - 4);
//
//      Status result = server_thread_->server_->ZP_->Write(request.key(), request.value());
//      if (!result.ok()) {
//        response->set_status(1);
//        response->set_msg(result.ToString());
//        LOG_ERROR("write failed %s", result.ToString().c_str());
//      } else {
//        response->set_status(0);
//        LOG_INFO ("write key(%s) ok!", request.key().c_str());
//      }
//      res_ = response;
//      break;
//    }
//
//    case client::OPCODE::READ: {
//      client::Read_Request request;
//      client::Read_Response* response = new client::Read_Response;
//      request.ParseFromArray(rbuf_ + 8, header_len_ - 4);
//
//      std::string value;
//      Status result = server_thread_->server_->ZP_->Read(request.key(), value);
//      if (!result.ok()) {
//        response->set_status(1);
//        LOG_ERROR("read failed %s", result.ToString().c_str());
//      } else if (result.ok()) {
//        response->set_status(0);
//        response->set_value(value);
//        LOG_INFO ("read key(%s) ok!", request.key().c_str());
//      }
//      res_ = response;
//      break;
//    }
//
//    default:
//      LOG_INFO ("invalid msg_code %d\n", msg_code);
//      break;
//  }
//
//  return 0;
//}
//
//////// ServerThread //////
//ZPServerThread::ZPServerThread(int port, ZPServer *server)
//    : HolyThread<ZPServerConn>(port) {
//  server_ = server;
//}
