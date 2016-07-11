#ifndef ZP_META_THREAD_H
#define ZP_META_THREAD_H

#include "zp_const.h"
#include "zp_pb_cli.h"

#include "status.h"
#include "pink_thread.h"
#include "slash_mutex.h"

class ZPMetaThread : public pink::Thread {
 public:

  // TODO  use floyd
  ZPMetaThread(int ip, int port) {
    //  : floyd_ip_(ip), floyd_port_(port) {
    // use comma separated ip1:port1,ip2:port2
    //floyd::client::Option floyd_option(floyd_ip_ + ":" + floyd_port_);
    //floyd_ = new floyd::client::Cluster(floyd_option);
  }

  virtual ~ZPMetaThread();

 private:

  // TODO maybe use uuid or serverId
  
  std::string floyd_ip_;
  int floyd_port_;

  // TODO wrap a class
  //floyd::client::Cluster* floyd_;

  virtual void* ThreadMain();
};

#endif
