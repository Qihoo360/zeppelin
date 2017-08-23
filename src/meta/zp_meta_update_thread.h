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
  kOpUpNode,  // ip_port
  kOpDownNode,  // ip_port
  kOpAddTable,  // table parition num
  kOpRemoveTable,
  kOpAddSlave,  // ip_port table partition
  kOpRemoveSlave,  // ip_port table partition
  kOpRemoveDup,  // ip_port table partition
  kOpSetMaster,  // ip_port table partition
  kOpSetStuck  //table partition
};

struct UpdateTask {
  ZPMetaUpdateOP op;
  std::string ip_port;
  std::string table;
  int partition;  // or partiiton num for kOpAddTable
  UpdateTask(ZPMetaUpdateOP o, const std::string& ip,
      const std::string&t, int p)
    : op(o), ip_port(ip), table(t), partition(p) {
    }

  UpdateTask(ZPMetaUpdateOP o, const std::string& ip)
    : op(o), ip_port(ip) { 
    }
};

typedef std::deque<UpdateTask> ZPMetaUpdateTaskDeque;

class ZPMetaUpdateThread {
public:
  ZPMetaUpdateThread(ZPMetaInfoStore* is);
  ~ZPMetaUpdateThread();

  void PendingUpdate(const UpdateTask& task, bool priority = false);

private:
  pink::BGThread* worker_;
  slash::Mutex task_mutex_;
  ZPMetaUpdateTaskDeque task_deque_;
  ZPMetaInfoStore* info_store_;
  static void UpdateFunc(void *p);

};

#endif
