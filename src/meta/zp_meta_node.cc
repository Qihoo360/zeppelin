#include <glog/logging.h>
#include "slash_string.h"

#include "zp_meta.pb.h"
#include "zp_meta_node.h"
#include "zp_meta_server.h"

extern ZPMetaServer *zp_meta_server;

void JoinCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  ZPMeta::MetaCmd* request = static_cast<ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_JOIN);

  struct timeval now;
  gettimeofday(&now, NULL);
  zp_meta_server->alive_mutex_.Lock();
  zp_meta_server->node_alive_.insert(
    std::make_pair(slash::IpPortString(request->join().node().ip(),
                   request->join().node().port()),
                   now));
  zp_meta_server->alive_mutex_.Unlock();
  //@todo wake update thread
  //
  status_res->set_code(0);
  status_res->set_msg("Join OK!");
  result_ = slash::Status::OK();
  DLOG(INFO) << "Join node: " << request->join().node().ip() << ":"
             << request->join().node().port();
}

void PingCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  ZPMeta::MetaCmd* request = static_cast<ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = static_cast<ZPMeta::MetaCmdResponse*>(res);

  ZPMeta::MetaCmdResponse_Status* status_res = response->mutable_status();
  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PING);
  
  // Update Ping time
  std::string node = slash::IpPortString(request->ping().node().ip(),
                                         request->ping().node().port());
  struct timeval now;
  gettimeofday(&now, NULL);
  zp_meta_server->alive_mutex_.Lock();
  zp_meta_server->node_alive_[node] = now;
  zp_meta_server->alive_mutex_.Unlock();
  
  status_res->set_code(0);
  status_res->set_msg("Ping OK!");
  result_ = slash::Status::OK();
  DLOG(INFO) << "Receive ping from node: " << request->ping().node().ip()
             << ":" << request->ping().node().port();
}

