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

enum ZPMetaUpdateOP {
  kOpAdd,
  kOpRemove,
  kOpAddVersion
};

typedef std::unordered_map<std::string, ZPMetaUpdateOP> ZPMetaUpdateTaskMap;

class ZPMetaUpdateThread {
public:
  ZPMetaUpdateThread();
  ~ZPMetaUpdateThread();

  void ScheduleUpdate(ZPMetaUpdateTaskMap task_map);
  static void DoMetaUpdate(void *p);

private:
  pink::BGThread worker_;
  slash::Status MetaUpdate(ZPMetaUpdateTaskMap task_map);

  struct ZPMetaUpdateArgs {
    ZPMetaUpdateThread * thread;
    ZPMetaUpdateTaskMap task_map;
    ZPMetaUpdateArgs(ZPMetaUpdateThread *_thread, ZPMetaUpdateTaskMap _task_map) :
      thread(_thread), task_map(_task_map) {}
  };

};


#endif
