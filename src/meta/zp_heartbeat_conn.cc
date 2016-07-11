#include "zp_heartbeat_conn.h"

#include <glog/logging.h>
#include "zp_meta_server.h"
#include "zp_meta.pb.h"

#include "slash_string.h"

extern ZPMetaServer *zp_meta_server;

ZPHeartbeatConn::ZPHeartbeatConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = reinterpret_cast<ZPHeartbeatThread*>(thread);
}

ZPHeartbeatConn::~ZPHeartbeatConn() {
}

int ZPHeartbeatConn::DealMessage() {
//  uint32_t buf;
//  memcpy((char *)(&buf), rbuf_ + 4, sizeof(uint32_t));
//  uint32_t msg_code = ntohl(buf);
//  LOG(INFO) << "HearbeatThead DealMessage msg_code:" << msg_code << ", buf:" << buf;
//
//
//  Cmd* cmd = self_thread_->GetCmd(static_cast<ZPMeta::OPCODE>(msg_code));
//  if (cmd == NULL) {
//    LOG(ERROR) << "unsupported msg_code: " << msg_code;
//    return -1;
//  }
//
//  Status s = cmd->Init(rbuf_ + 8, header_len_ - 4);
//  if (!s.ok()) {
//    LOG(ERROR) << "command Init failed, " << s.ToString();
//  }
//
//  switch (msg_code) {
//    case ZPMeta::OPCODE::JOIN: {
//      DLOG(INFO) << "Receive Join cmd";
//      // TODO set fd
//      JoinCmd *join_cmd = dynamic_cast<JoinCmd *>(cmd);
//      join_cmd->set_fd(fd());
//      join_cmd->Do();
//      set_is_reply(true);
//      break;
//    }
//    case ZPMeta::OPCODE::PING: {
//      cmd->Do();
//      DLOG(INFO) << "Receive Ping cmd";
//      set_is_reply(true);
//      break;
//    }
//  }
//
//
//
//  if (cmd->is_write()) {
//    if (cmd->result().ok()) {
//      // Restore Message
//    }
//    // TODO add RecordLock for write cmd
//  }
//
//  res_ = cmd->Response();

  return 0;
}
