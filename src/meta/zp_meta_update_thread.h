#ifndef ZP_META_UPDATE_THREAD_H
#define ZP_META_UPDATE_THREAD_H

#include <string>
#include <unordered_map>
#include <glog/logging.h>

#include "include/zp_meta.pb.h"

#include "pink/include/bg_thread.h"
//#include "pink/include/pink_cli.h"

#include "slash/include/slash_string.h"
#include "slash/include/slash_status.h"

enum ZPMetaUpdateOP : unsigned int {
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
  UpdateTask(ZPMetaUpdateOP o, const std::string& ip,
      const std::string&t, int p)
    : op(o), ip_port(ip), table(t), partition(p) {}
};

typedef std::deque<UpdateTask> ZPMetaUpdateTaskDeque;

class ZPMetaUpdateThread {
public:
  ZPMetaUpdateThread();
  ~ZPMetaUpdateThread();

  void PendingUpdate(const UpdateTask& task, bool priority = false);

private:
  pink::BGThread* worker_;
  slash::Mutex task_mutex_;
  ZPMetaUpdateTaskDeque task_deque_;
  static void DoMetaUpdate(void *p);

};

#endif
