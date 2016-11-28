#ifndef ZP_BINLOG_SENDER
#define ZP_BINLOG_SENDER
#include <list>
#include <vector>
#include <string>
#include <unordered_map>

#include "env.h"
#include "slash_status.h"
#include "slash_mutex.h"
#include "pink_thread.h"

#include "zp_meta_utils.h"
#include "zp_binlog.h"

using slash::Status;
using slash::Slice;
#define ZPBinlogSendTaskIndex std::unordered_map< std::string, std::list< ZPBinlogSendTask* >::iterator >

std::string ZPBinlogSendTaskName(const std::string& table, int32_t id, const Node& target);

/**
 * ZPBinlogSendTask
 */
class ZPBinlogSendTask {
public:
  static Status Create(const std::string &table_name, int32_t id,
      const Node& target, uint32_t ifilenum,uint64_t ioffset,
      ZPBinlogSendTask** tptr);
  ZPBinlogSendTask(const std::string &table_name, int32_t id, const Node& target,
    uint32_t ifilenum, uint64_t ioffset);
  ~ZPBinlogSendTask();

  bool send_next;

  std::string name() const {
    return name_;
  }
  std::string table_name() const {
    return table_name_;
  }

  int32_t partition_id() {
    return partition_id_;
  }
  Node node() const {
    return node_;
  }
  uint32_t filenum() {
    return filenum_;
  }
  uint64_t offset() {
    return offset_;
  }

  Status ProcessTask(std::string &item);

private:
  std::string name_;
  const std::string table_name_;
  const int32_t partition_id_;
  const Node node_;
  uint32_t filenum_;
  uint64_t offset_;
  std::string binlog_filename_;
  slash::SequentialFile *queue_;
  BinlogReader *reader_;
  Status Init();
};


/**
 * ZPBinlogSendTaskPool
 */
class ZPBinlogSendTaskPool {
public:
  ZPBinlogSendTaskPool();
  ~ZPBinlogSendTaskPool();

  bool TaskExist(const std::string& task_name);

  Status AddNewTask(const std::string &table, int32_t id, const Node& target,
    uint32_t ifilenum, uint64_t ioffset);
  Status RemoveTask(const std::string &name);
  int32_t TaskFilenum(const std::string &name);

  // Use by Task Worker
  // Who Fetchout one task, process it, and then PutBack
  Status FetchOut(ZPBinlogSendTask** task);
  Status PutBack(ZPBinlogSendTask* task);

  void Dump();

private:
  pthread_rwlock_t tasks_rwlock_;
  ZPBinlogSendTaskIndex task_ptrs_;
  std::list<ZPBinlogSendTask*> tasks_;
  Status AddTask(ZPBinlogSendTask* task);
};


/**
 * ZPBinlogSendThread
 */
class ZPBinlogSendThread : public pink::Thread {
public:
  ZPBinlogSendThread(ZPBinlogSendTaskPool *pool);
  virtual ~ZPBinlogSendThread();

private:
  ZPBinlogSendTaskPool *pool_;
  virtual void* ThreadMain();
};

#endif //ZP_BINLOG_SENDER
