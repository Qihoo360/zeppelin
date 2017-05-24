#ifndef ZP_META_CLIENT_CONN_H
#define ZP_META_CLIENT_CONN_H

#include "include/zp_meta.pb.h"

#include "pink/include/pb_conn.h"
#include "pink/include/server_thread.h"

class ZPMetaServerHandle;
class ZPMetaClientConn;
class ZPMetaClientConnFactory;

class ZPMetaServerHandle : public pink::ServerHandle {
 public:
  explicit ZPMetaServerHandle() {}
  virtual ~ZPMetaServerHandle() {}

  virtual void CronHandle() const;
  virtual bool AccessHandle(std::string& ip) const {
    return true;
  }
};

class ZPMetaClientConn : public pink::PbConn {
 public:
  ZPMetaClientConn(int fd, const std::string& ip_port, pink::Thread* thread);
  virtual ~ZPMetaClientConn();
  virtual int DealMessage();

 private:
  ZPMeta::MetaCmd request_;
  ZPMeta::MetaCmdResponse response_;
};

class ZPMetaClientConnFactory : public pink::ConnFactory {
 public:
  explicit ZPMetaClientConnFactory() {}

  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string &ip_port, pink::Thread *thread) const override {
    return new ZPMetaClientConn(connfd, ip_port, thread);
  }
};

#endif
