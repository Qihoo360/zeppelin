#include "zp_heartbeat_conn.h"

#include "slash_string.h"

ZPHeartbeatConn::ZPHeartbeatConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = reinterpret_cast<ZPHeartbeatThread*>(thread);
}

ZPHeartbeatConn::~ZPHeartbeatConn() {
}

int ZPHeartbeatConn::DealMessage() {
  //printf ("DealMessage\n");
  request_.ParseFromArray(rbuf_ + 4, header_len_);
  LOG_INFO("Heartbeat DealMessage type:%d", (int)request_.type());

  // for now, only one cmd SYNC
  switch (request_.type()) {
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_PING: {
      response_.set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PING);

      ZPMeta::MetaCmdResponse_Status* status = response_.mutable_status();
      status->set_code(ZPMeta::StatusCode::kOk);
      LOG_INFO("Receive Ping cmd\n");
      set_is_reply(true);
      break;
    }
    case ZPMeta::MetaCmd_Type::MetaCmd_Type_JOIN: {
      response_.set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_JOIN);

      ZPMeta::MetaCmdResponse_Status* status = response_.mutable_status();
      status->set_code(ZPMeta::StatusCode::kOk);
      LOG_INFO("Receive Join (%s:%d)\n", request_.join().node().ip().c_str(), request_.join().node().port());
      set_is_reply(true);
      break;
    }
    default:
      LOG_INFO("Unsupported cmd\n");
  }

  res_ = &response_;
  return 0;
}
