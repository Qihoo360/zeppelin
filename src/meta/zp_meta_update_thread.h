#ifndef ZP_META_UPDATE_THREAD_H
#define ZP_META_UPDATE_THREAD_H
#include <deque>
#include <string>
#include <unordered_map>
#include <glog/logging.h>
#include "pink/include/bg_thread.h"
#include "slash/include/slash_string.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_info_store.h"
#include "src/meta/zp_meta_migrate_register.h"

enum ZPMetaUpdateOP : unsigned int {
  kOpUpNode,
  kOpDownNode,
  kOpAddTable,
  kOpRemoveTable,
  kOpAddSlave,
  kOpRemoveSlave,
  kOpHandover,  // Replace one node by another with the same role
  kOpSetMaster,
  kOpSetStuck,  // Stuck the partition
  kOpSetActive  // ACTIVE the partition
};

struct UpdateTask {
  ZPMetaUpdateOP op;
  std::string ip_port;
  std::string ip_port_o;
  std::string table;
  int partition;  // or partiiton num for kOpAddTable

  UpdateTask(ZPMetaUpdateOP o,
      const std::string& ip,
      const std::string& ip1,
      const std::string&t,
      int p)
    : op(o), ip_port(ip), ip_port_o(ip1),
    table(t), partition(p) { 
    }

  UpdateTask(ZPMetaUpdateOP o,
      const std::string& ip,
      const std::string&t,
      int p)
    : op(o), ip_port(ip), table(t), partition(p) {
    }

  UpdateTask(ZPMetaUpdateOP o,
      const std::string& ip)
    : op(o), ip_port(ip) { 
    }

};

typedef std::deque<UpdateTask> ZPMetaUpdateTaskDeque;

class ZPMetaUpdateThread {
public:
  explicit ZPMetaUpdateThread(ZPMetaInfoStore* is,
      ZPMetaMigrateRegister* migrate);
  ~ZPMetaUpdateThread();

  Status PendingUpdate(const UpdateTask& task);
  void Active();
  void Abandon();

private:
  std::atomic<bool> is_stuck_;
  std::atomic<bool> should_stop_;
  pink::BGThread* worker_;
  slash::Mutex task_mutex_;
  ZPMetaUpdateTaskDeque task_deque_;
  ZPMetaInfoStore* info_store_;
  ZPMetaMigrateRegister* migrate_;

  static void UpdateFunc(void *p);
  Status ApplyUpdates(ZPMetaUpdateTaskDeque& task_deque);

};

#endif
