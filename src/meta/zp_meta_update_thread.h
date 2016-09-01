#ifndef ZP_META_UPDATE_THREAD_H
#define ZP_META_UPDATE_THREAD_H

#include <glog/logging.h>
#include <string>
#include <unordered_map>
#include "slash_string.h"
#include "slash_status.h"
#include "bg_thread.h"
#include "pb_cli.h"
#include "zp_meta.pb.h"

typedef std::unordered_map<std::string, pink::PbCli*> DataCliMap;

enum ZPMetaUpdateOP {
  OP_ADD,
  OP_REMOVE
};

class ZPMetaUpdateThread {
public:
  ZPMetaUpdateThread();
  
  ~ZPMetaUpdateThread() {
    worker_.Stop();
    DataCliMap::iterator it = data_sender_.begin();
    for (; it != data_sender_.end(); ++it) {
      (it->second)->Close();
      delete it->second;
    }
  }

  void ScheduleUpdate(const std::string ip_port, ZPMetaUpdateOP op) {
    std::string ip;
    int port;
    if (!slash::ParseIpPortString(ip_port, ip, port)) {
      return;
    }
    ZPMetaUpdateArgs* arg = new ZPMetaUpdateArgs(this, ip, port, op);
    worker_.StartIfNeed();
    LOG(INFO) << "Schedule to update thread worker, update";
    worker_.Schedule(&DoMetaUpdate, static_cast<void*>(arg));
  }

  void ScheduleBroadcast() {
    worker_.StartIfNeed();
    LOG(INFO) << "Schedule to update thread worker, broadcast";
    worker_.Schedule(&DoMetaBroadcast, static_cast<void*>(this));
  }

  static void DoMetaUpdate(void *);
  static void DoMetaBroadcast(void *);

private:
  DataCliMap data_sender_;
  pink::BGThread worker_;
  slash::Status MetaUpdate(const std::string ip, int port, ZPMetaUpdateOP op);
  slash::Status MetaBroadcast();
  slash::Status UpdateSender(const std::string &ip, int port, ZPMetaUpdateOP op);
  void SendUpdate(const std::string &ip, int port, ZPMeta::MetaCmd_Update &ms_info);
  void SendUpdate(ZPMeta::MetaCmd_Update &ms_info);

  struct ZPMetaUpdateArgs {
    ZPMetaUpdateThread *thread;
    std::string ip;
    int port;
    ZPMetaUpdateOP op;
    ZPMetaUpdateArgs(ZPMetaUpdateThread *_thread, const std::string& _ip, int _port, ZPMetaUpdateOP _op) :
      thread(_thread), ip(_ip), port(_port), op(_op) {}
  };

};


#endif
