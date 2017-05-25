#ifndef ZP_SYNC_CONN_H
#define ZP_SYNC_CONN_H

#include "pink/include/pb_conn.h"
#include "pink/include/pink_thread.h"
#include "include/client.pb.h"
#include "include/zp_command.h"

class ZPSyncConn: public pink::PbConn {
public:
  ZPSyncConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~ZPSyncConn();
  virtual int DealMessage() override;

private:
  client::SyncRequest request_;
  void DebugReceive(const client::CmdRequest &crequest) const;
};

class ZPSyncConnFactory : public pink::ConnFactory {
 public:
  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string &ip_port, pink::Thread *thread) const {
    return new ZPSyncConn(connfd, ip_port, thread);
  }
};

#endif
