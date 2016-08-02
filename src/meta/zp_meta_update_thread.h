#ifndef ZP_META_UPDATE_THREAD_H
#define ZP_META_UPDATE_THREAD_H

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
    worker_.Schedule(&DoMetaUpdate, static_cast<void*>(arg));
  }

  static void DoMetaUpdate(void *);

private:
  DataCliMap data_sender_;
  pink::BGThread worker_;
  slash::Status MetaUpdate(const std::string ip, int port, ZPMetaUpdateOP op);
  slash::Status UpdateFloyd(const std::string &ip, int port, ZPMetaUpdateOP op, ZPMeta::Partitions &partitions);
  slash::Status UpdateSender(const std::string &ip, int port, ZPMetaUpdateOP op);
  void SendUpdate(ZPMeta::Partitions &Partitions);
  void UpdatePartition(ZPMeta::Partitions &partitions,
    const std::string& ip, int port, ZPMetaUpdateOP op, int id);
  void SetMaster(ZPMeta::Partitions &partitions, const std::string &ip, int port) {
    ZPMeta::Node *master = partitions.mutable_master();
    master->set_ip(ip);
    master->set_port(port);
  }

  bool IsTheOne(const ZPMeta::Node &node, const std::string &ip, int port) {
    return (node.ip() == ip && node.port() == port);
  }

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
