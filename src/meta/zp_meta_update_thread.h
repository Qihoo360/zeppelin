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
  kOpAddVersion,
  kOpSetMaster,
  kOpClearStuck,
  kOpAddSlave,
  kOpRemoveSlave
};

struct UpdateTask {
  ZPMetaUpdateOP op;
  std::string ip_port;
  std::string table;
  int partition;
};

typedef std::deque<UpdateTask> ZPMetaUpdateTaskDeque;

class ZPMetaUpdateThread {
public:
  ZPMetaUpdateThread();
  ~ZPMetaUpdateThread();

  void ScheduleUpdate(ZPMetaUpdateTaskDeque task_deque);
  static void DoMetaUpdate(void *p);

private:
  pink::BGThread worker_;
  slash::Status MetaUpdate(ZPMetaUpdateTaskDeque task_deque);

  struct ZPMetaUpdateArgs {
    ZPMetaUpdateThread * thread;
    ZPMetaUpdateTaskDeque task_deque;
    ZPMetaUpdateArgs(ZPMetaUpdateThread *_thread, ZPMetaUpdateTaskDeque _task_deque) :
      thread(_thread), task_deque(_task_deque) {}
  };

};


#endif
