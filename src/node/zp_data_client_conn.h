#ifndef ZP_DATA_CLIENT_CONN_H
#define ZP_DATA_CLIENT_CONN_H

#include <string>
#include "pink/include/pb_conn.h"
#include "pink/include/pink_thread.h"
#include "pink/include/server_thread.h"

#include "include/client.pb.h"

class ZPDataClientConn : public pink::PbConn {
 public:
  ZPDataClientConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~ZPDataClientConn();

  virtual int DealMessage();

 private:
  client::CmdRequest request_;
  client::CmdResponse response_;
  
  int DealMessageInternal();
};

class ZPDataClientConnHandle : public pink::ServerHandle {
 public:
  explicit ZPDataClientConnHandle() {}
  virtual ~ZPDataClientConnHandle() {}

  virtual void CronHandle() const;
  virtual bool AccessHandle(std::string& ip) const {
    return true;
  }
};

class ZPDataClientConnFactory : public pink::ConnFactory {
 public:
  virtual pink::PinkConn *NewPinkConn(int connfd, const std::string &ip_port,
      pink::Thread *thread) const {
    return new ZPDataClientConn(connfd, ip_port, thread);
  }
};

#endif
