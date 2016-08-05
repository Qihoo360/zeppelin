#ifndef ZP_META_CLIENT_CONN_H
#define ZP_META_CLIENT_CONN_H

#include "pink_thread.h"
#include "pb_conn.h"
#include "pb_cli.h"

#include "zp_meta.pb.h"
#include "zp_meta_worker_thread.h"

class ZPMetaWorkerThread;

class ZPMetaClientConn : public pink::PbConn {
 public:
  ZPMetaClientConn(int fd, std::string ip_port, pink::Thread* thread);
  virtual ~ZPMetaClientConn();

  virtual int DealMessage();
  ZPMetaWorkerThread* self_thread() {
    return self_thread_;
  }

 private:
  ZPMeta::MetaCmd request_;
  ZPMeta::MetaCmdResponse response_;

  ZPMetaWorkerThread* self_thread_;

  // Connect with leader
  bool IsLeader();
  pink::PbCli* leader_cli_;
  std::string leader_ip_;
  int leader_cmd_port_;
  void LeaderClean() {
    if (leader_cli_) {
      leader_cli_->Close();
      leader_ip_.clear();
      leader_cmd_port_ = 0;
    }
  }
};


#endif
