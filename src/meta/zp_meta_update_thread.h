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
  kOpAdd,
  kOpRemove
};

class ZPMetaUpdateThread {
public:
  ZPMetaUpdateThread();
  ~ZPMetaUpdateThread();

  void ScheduleUpdate(const std::string ip_port, ZPMetaUpdateOP op);
  static void DoMetaUpdate(void *p);

private:
  pink::BGThread worker_;
  slash::Status MetaUpdate(const std::string ip, int port, ZPMetaUpdateOP op);

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
