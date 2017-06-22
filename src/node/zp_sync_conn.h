#ifndef ZP_SYNC_CONN_H
#define ZP_SYNC_CONN_H

#include "include/client.pb.h"
//#include "include/zp_command.h"

#include "pink/include/pb_conn.h"
#include "pink/include/server_thread.h"

class ZPSyncConn: public pink::PbConn {
public:
  ZPSyncConn(int fd, std::string ip_port, pink::ServerThread *thread);
  virtual ~ZPSyncConn();
  virtual int DealMessage() override;

private:
  client::SyncRequest request_;
  void DebugReceive(const client::CmdRequest &crequest) const;
};

class ZPSyncConnHandle : public pink::ServerHandle {
 public:
  ZPSyncConnHandle() {}
  virtual ~ZPSyncConnHandle() {}

  virtual void CronHandle() const;
  virtual bool AccessHandle(std::string& ip) const {
    return true;
  }
};

class ZPSyncConnFactory : public pink::ConnFactory {
 public:
  virtual pink::PinkConn* NewPinkConn(
      int connfd,
      const std::string &ip_port,
      pink::ServerThread *server_thread,
      void* worker_private_data) const override {
    return new ZPSyncConn(connfd, ip_port, server_thread);
  }
};

#endif
