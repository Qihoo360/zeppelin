#include "zp_heartbeat_conn.h"

#include <glog/logging.h>
#include "zp_server.h"
#include "zp_admin.h"

#include "slash_string.h"

extern ZPServer *zp_server;

ZPHeartbeatConn::ZPHeartbeatConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = reinterpret_cast<ZPHeartbeatThread*>(thread);
}

ZPHeartbeatConn::~ZPHeartbeatConn() {
}

int ZPHeartbeatConn::DealMessage() {
  uint32_t buf;
  memcpy((char *)(&buf), rbuf_ + 4, sizeof(uint32_t));
  uint32_t msg_code = ntohl(buf);
  LOG(INFO) << "HearbeatThead DealMessage msg_code:" << msg_code << ", buf:" << buf;


  Cmd* cmd = self_thread_->GetCmd(static_cast<ServerControl::OPCODE>(msg_code));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported msg_code: " << msg_code;
    return -1;
  }

  Status s = cmd->Init(rbuf_ + 8, header_len_ - 4);
  if (!s.ok()) {
    LOG(ERROR) << "command Init failed, " << s.ToString();
  }

  switch (msg_code) {
    case ServerControl::OPCODE::JOIN: {
      DLOG(INFO) << "Receive Join cmd";
      // TODO set fd
      JoinCmd *join_cmd = dynamic_cast<JoinCmd *>(cmd);
      join_cmd->set_fd(fd());
      join_cmd->Do();
      set_is_reply(true);
      break;
    }
    case ServerControl::OPCODE::PING: {
      cmd->Do();
      DLOG(INFO) << "Receive Ping cmd";
      set_is_reply(true);
      break;
    }
  }



  if (cmd->is_write()) {
    if (cmd->result().ok()) {
      // Restore Message
    }
    // TODO add RecordLock for write cmd
  }

  res_ = cmd->Response();

  return 0;
  
//  set_is_reply(true);
//  if (argv_[0] == "ping") {
//    memcpy(wbuf_ + wbuf_len_, "+PONG\r\n", 7);
//    wbuf_len_ += 7;
//  } else if (argv_[0] == "spci") {
//    int64_t sid = -1;
//    slash::string2l(argv_[1].data(), argv_[1].size(), &sid);
//    zp_server->MayUpdateSlavesMap(sid, fd());
//    memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
//    wbuf_len_ += 5;
//  } else {
//    memcpy(wbuf_ + wbuf_len_, "-ERR What the fuck are u sending\r\n", 34);
//    wbuf_len_ += 34;
//  }
  return 0;
}
