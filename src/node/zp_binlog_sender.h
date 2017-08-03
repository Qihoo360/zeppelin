// Copyright 2017 Qihoo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef SRC_NODE_ZP_BINLOG_SENDER_H_
#define SRC_NODE_ZP_BINLOG_SENDER_H_
#include <list>
#include <string>
#include <unordered_map>

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/env.h"
#include "pink/include/pink_thread.h"

#include "include/client.pb.h"
#include "include/zp_meta_utils.h"
#include "include/zp_binlog.h"

using slash::Status;
using slash::Slice;

class ZPBinlogSendTask;
struct ZPBinlogSendTaskHandle {
  std::list< ZPBinlogSendTask* >::iterator iter;
  uint64_t sequence;  // use squence to distinguish task with same name
};

typedef std::unordered_map< std::string,
        ZPBinlogSendTaskHandle > ZPBinlogSendTaskIndex;

std::string ZPBinlogSendTaskName(const std::string& table,
    int32_t id, const Node& target);

/**
 * ZPBinlogSendTask
 */
class ZPBinlogSendTask  {
 public:
  static Status Create(uint64_t seq, const std::string &table_name, int32_t id,
      const std::string& binlog_prefix, const Node& target,
      uint32_t ifilenum, uint64_t ioffset,
      ZPBinlogSendTask** tptr);

  ZPBinlogSendTask(uint64_t seq, const std::string &table_name, int32_t id,
      const std::string& binlog_prefix, const Node& target,
      uint32_t ifilenum, uint64_t ioffset);
  ~ZPBinlogSendTask();

  bool send_next;

  uint64_t sequence() const {
    return sequence_;
  }
  std::string name() const {
    return name_;
  }
  std::string table_name() const {
    return table_name_;
  }
  int32_t partition_id() const {
    return partition_id_;
  }
  Node node() const {
    return node_;
  }
  uint32_t filenum() const {
    return filenum_;
  }
  uint64_t offset() const {
    return offset_;
  }
  uint64_t process_error_time() const {
    return process_error_time_;
  }
  void renew_process_error_time() {
    process_error_time_ = slash::NowMicros();
  }
  uint32_t pre_filenum() const {
    return pre_filenum_;
  }
  uint64_t pre_offset() const {
    return pre_offset_;
  }
  std::string pre_content() const {
    return pre_content_;
  }

  Status ProcessTask();
  void BuildLeaseSyncRequest(int64_t lease_time,
      client::SyncRequest* msg) const;
  void BuildCommonSyncRequest(client::SyncRequest *msg) const;

 private:
  uint64_t sequence_;
  std::string name_;  // Name of the task
  const std::string table_name_;  // Name of its table
  const int32_t partition_id_;
  const Node node_;
  uint32_t filenum_;
  uint64_t offset_;
  uint64_t process_error_time_;
  
  // Record The last item filenum and offset
  // For sending use later
  uint32_t pre_filenum_;
  uint64_t pre_offset_;
  std::string pre_content_;
  bool pre_has_content_;
  std::string binlog_filename_;  // Name of the binlog file
  slash::SequentialFile *queue_;
  BinlogReader *reader_;
  Status Init();
  // Record current filenum and offset in the pre one
  // So that we can know where the last binlog item begin
  void RecordPreOffset() {
    pre_filenum_ = filenum_;
    pre_offset_ = offset_;
  }
};

//
// ZPBinlogSendTaskPool
//
class ZPBinlogSendTaskPool  {
 public:
  ZPBinlogSendTaskPool();
  ~ZPBinlogSendTaskPool();

  bool TaskExist(const std::string& task_name);

  Status AddNewTask(const std::string &table, int32_t id,
      const std::string& binlog_filename, const Node& target,
      uint32_t ifilenum, uint64_t ioffset, bool force);
  Status RemoveTask(const std::string &name);
  int32_t TaskFilenum(const std::string &name);
  size_t Size() {
    slash::RWLock l(&tasks_rwlock_, false);
    return task_ptrs_.size();
  }

  // Use by Task Worker
  // Who Fetchout one task, process it, and then PutBack
  Status FetchOut(ZPBinlogSendTask** task);
  Status PutBack(ZPBinlogSendTask* task);

  void Dump();

 private:
  pthread_rwlock_t tasks_rwlock_;
  uint64_t next_sequence_;  // Give every task a unique sequence
  ZPBinlogSendTaskIndex task_ptrs_;
  std::list<ZPBinlogSendTask*> tasks_;
  Status AddTask(ZPBinlogSendTask* task);
};

/**
 * ZPBinlogSendThread
 */
class ZPBinlogSendThread : public pink::Thread  {
 public:
  explicit ZPBinlogSendThread(ZPBinlogSendTaskPool *pool);
  virtual ~ZPBinlogSendThread();

 private:
  ZPBinlogSendTaskPool *pool_;
  virtual void* ThreadMain();
  bool RenewPeerLease(ZPBinlogSendTask* task);
};

#endif  // SRC_NODE_ZP_BINLOG_SENDER_H_
