#include "zp_binlog_sender.h"

#include <limits>
#include <glog/logging.h>
#include "zp_const.h"
#include "zp_data_server.h"
#include "zp_data_partition.h"

extern ZPDataServer* zp_data_server;

static void BuildSyncRequest(uint32_t filenum, uint64_t offset,
    const std::string& content, client::SyncRequest *msg) {
  client::CmdRequest req;
  req.ParseFromString(content);
  msg->set_epoch(zp_data_server->meta_epoch());
  client::Node *node = msg->mutable_from();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());
  client::SyncOffset *offset = msg->mutable_offset();
  offset->set_filenum(filenum);
  offset->set_offset(offset);
  client::CmdRequest *req_ptr = msg->mutable_request();
  req_ptr->CopyFrom(req);
  //std::string text_format;
  //google::protobuf::TextFormat::PrintToString(msg, &text_format);
  //DLOG(INFO) << "SyncRequest to be sent: [" << text_format << "]";
} 

std::string ZPBinlogSendTaskName(const std::string& table, int32_t id, const Node& target) {
  char buf[256];
  sprintf(buf, "%s_%d_%s_%d", table.c_str(), id, target.ip.c_str(), target.port);
  return std::string(buf);
}

/**
 * ZPBinlogSendTask
 */
Status ZPBinlogSendTask::Create(const std::string &table, int32_t id, const Node& target,
      uint32_t ifilenum, uint64_t ioffset,
      ZPBinlogSendTask** tptr) {
  *tptr = NULL;
  ZPBinlogSendTask* task = new ZPBinlogSendTask(table, id, target,
      ifilenum, ioffset);
  Status s = task->Init();
  if (s.ok()) {
    *tptr = task;
  } else {
    delete task;
  }
  return s;
}

ZPBinlogSendTask::ZPBinlogSendTask(const std::string &table, int32_t id, const Node& target,
    uint32_t ifilenum, uint64_t ioffset) :
  send_next(true),
  table_name_(table),
  partition_id_(id),
  node_(target),
  filenum_(ifilenum),
  offset_(ioffset),
  pre_filenum_(0),
  pre_offset_(0) {
    name_ = ZPBinlogSendTaskName(table, partition_id_, target);
    pre_content_.reserve(1024 * 1024);
  }

ZPBinlogSendTask::~ZPBinlogSendTask() {
  delete reader_;
  delete queue_;
}

Status ZPBinlogSendTask::Init() {
  Partition* partition = zp_data_server->GetTablePartitionById(table_name_, partition_id_);
  if (partition == NULL) {
    return Status::NotFound("partiiton not exist");
  }

  binlog_filename_ = partition->GetBinlogFilename();
  std::string confile = NewFileName(binlog_filename_, filenum_);
  if (!slash::NewSequentialFile(confile, &queue_).ok()) {
    return Status::IOError("ZPBinlogSendTask Init new sequtial file failed");
  }
  reader_ = new BinlogReader(queue_);
  return reader_->Seek(offset_);
}

Status ZPBinlogSendTask::ProcessTask() {
  if (reader_ == NULL || queue_ == NULL) {
    return Status::InvalidArgument("Error Task");
  }

  // Check task position
  uint32_t curnum = 0;
  uint64_t curoffset = 0;
  Partition* partition = zp_data_server->GetTablePartitionById(table_name_,
      partition_id_);
  if (partition == NULL) {
    return Status::InvalidArgument("Error Task with nono exist partition");
  }
  partition->GetBinlogOffset(&curnum, &curoffset);
  if (filenum_ == curnum && offset_ == curoffset) {
    // No more binlog item in current task, switch to others
    return Status::EndFile("no more binlog item");
  }
  //LOG(INFO) << "Processing a task" << table_name_
  // << "parititon: " << partition_id_;
  RecordPreOffset();

  uint64_t consume_len = 0;
  Status s = reader_->Consume(&consume_len, &pre_content_);
  if (s.IsEndFile()) {
    // Roll to next File
    std::string confile = NewFileName(binlog_filename_, filenum_ + 1);

    if (slash::FileExists(confile)) {
      DLOG(INFO) << "BinlogSender (" << node_ << ") roll to new binlog " << confile;
      delete reader_;
      reader_ = NULL;
      delete queue_;
      queue_ = NULL;

      s = slash::NewSequentialFile(confile, &(queue_));
      if (!s.ok()) {
        LOG(WARNING) << "Failed to roo to next binlog file:" << (filenum_ + 1)
          << " Error:" << s.ToString(); 
        return s;
      }
      reader_ = new BinlogReader(queue_);
      filenum_++;
      offset_ = 0;
      return ProcessTask();
    }
    // Return EndFile
  } else if (!s.ok()) {
    LOG(WARNING) << "ZPBinlogSendTask failed to Consume: " << s.ToString(); 
    // Return Error
  }

  if (s.ok()) {
    offset_ += consume_len;
  }
  return s;
}

/**
 * ZPBinlogSendTaskPool
 */
ZPBinlogSendTaskPool::ZPBinlogSendTaskPool() {
  pthread_rwlock_init(&tasks_rwlock_, NULL);
  task_ptrs_.reserve(1000);
  LOG(INFO) << "size: " << tasks_.size();
}

ZPBinlogSendTaskPool::~ZPBinlogSendTaskPool() {
  std::list<ZPBinlogSendTask*>::iterator it;
  for (it = tasks_.begin(); it != tasks_.end(); ++it) {
    delete *it;
  }
  pthread_rwlock_destroy(&tasks_rwlock_);
}

bool ZPBinlogSendTaskPool::TaskExist(const std::string& task_name) {
  slash::RWLock l(&tasks_rwlock_, false);
  if (task_ptrs_.find(task_name) == task_ptrs_.end()) {
    return false;
  }
  return true;
}

Status ZPBinlogSendTaskPool::AddNewTask(const std::string &table_name, int32_t id, const Node& target,
    uint32_t ifilenum, uint64_t ioffset, bool force) {
  ZPBinlogSendTask* task_ptr = NULL;
  Status s = ZPBinlogSendTask::Create(table_name, id, target, ifilenum, ioffset, &task_ptr);
  if (!s.ok()) {
    return s;
  }
  if (force && TaskExist(task_ptr->name())) {
    RemoveTask(task_ptr->name());
  }
  s = AddTask(task_ptr);
  if (!s.ok()) {
    delete task_ptr;
  }
  return s;
}

Status ZPBinlogSendTaskPool::AddTask(ZPBinlogSendTask* task) {
  assert(task != NULL);
  slash::RWLock l(&tasks_rwlock_, true);
  if (task_ptrs_.find(task->name()) != task_ptrs_.end()) {
    return Status::InvalidArgument("Task already exist");
  }
  tasks_.push_back(task);
  // index point to the last one just push back
  task_ptrs_[task->name()] = tasks_.end();
  --(task_ptrs_[task->name()]);
  return Status::OK();
}

Status ZPBinlogSendTaskPool::RemoveTask(const std::string &name) {
  slash::RWLock l(&tasks_rwlock_, true);
  ZPBinlogSendTaskIndex::iterator it = task_ptrs_.find(name);
  if (it == task_ptrs_.end()) {
    return Status::NotFound("Task not exist");
  }
  // Task has been FetchOut should be deleted when Pushback
  if (it->second != tasks_.end()) {
    delete *(it->second);
  }
  tasks_.erase(it->second);
  task_ptrs_.erase(it);
  return Status::OK();
}

// Return the task filenum indicated by id and node
// -1 when the task is not exist
// max() when the task is exist but is processing now
int32_t ZPBinlogSendTaskPool::TaskFilenum(const std::string &name) {
  slash::RWLock l(&tasks_rwlock_, false);
  ZPBinlogSendTaskIndex::iterator it = task_ptrs_.find(name);
  if (it == task_ptrs_.end()) {
    return -1;
  }
  if (it->second == tasks_.end()) {
    // The task is processing by some thread
    return std::numeric_limits<int32_t>::max();
  }
  return (*(it->second))->filenum();
}

// Fetch one task out from the front of tasks_ list
// and live the its ptr point to the tasks_.end()
// to distinguish from task has been removed
Status ZPBinlogSendTaskPool::FetchOut(ZPBinlogSendTask** task_ptr) {
  slash::RWLock l(&tasks_rwlock_, true);
  if (tasks_.size() == 0) {
    return Status::NotFound("No more task");
  }
  *task_ptr = tasks_.front();
  tasks_.pop_front();
  // Do not remove from the task_ptrs_ map
  // When the same task put back we need to know it is a old one
  task_ptrs_[(*task_ptr)->name()] = tasks_.end();
  return Status::OK();
}

// PutBack the task who has been FetchOut
// return NotFound when the task is not exist in index map task_pts_
// which mean the task has been removed or its not a task fetch out before
Status ZPBinlogSendTaskPool::PutBack(ZPBinlogSendTask* task) {
  slash::RWLock l(&tasks_rwlock_, true);
  ZPBinlogSendTaskIndex::iterator it = task_ptrs_.find(task->name());
  if (it == task_ptrs_.end()              // task has been removed
      || it->second != tasks_.end()) {    // task belong to same partition has beed added
    delete task;
    return Status::NotFound("Task may have been deleted");
  }
  tasks_.push_back(task);
  it->second = tasks_.end();
  --(it->second);
  return Status::OK();
}

void ZPBinlogSendTaskPool::Dump() {
  slash::RWLock l(&tasks_rwlock_, false);
  ZPBinlogSendTaskIndex::iterator it = task_ptrs_.begin();
  LOG(INFO) << "----------------------------";
  for (; it != task_ptrs_.end(); ++it) {
    std::list<ZPBinlogSendTask*>::iterator tptr = it->second;
    LOG(INFO) << "----------------------------";
    LOG(INFO) << "+Binlog Send Task" << it->first;
    if (tptr != tasks_.end()) {
      LOG(INFO) << "  +Table  " << (*tptr)->table_name();
      LOG(INFO) << "  +Partition  " << (*tptr)->partition_id();
      LOG(INFO) << "  +Node  " << (*tptr)->node();
      LOG(INFO) << "  +filenum " << (*tptr)->filenum();
      LOG(INFO) << "  +offset " << (*tptr)->offset();
    } else {
      LOG(INFO) << "  +Being occupied";
    }
    LOG(INFO) << "----------------------------";
  }
  LOG(INFO) << "----------------------------";
}

/**
 * ZPBinlogSendThread
 */

ZPBinlogSendThread::ZPBinlogSendThread(ZPBinlogSendTaskPool *pool)
  : pink::Thread::Thread(),
  pool_(pool) {
  }

ZPBinlogSendThread::~ZPBinlogSendThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  LOG(INFO) << "a BinlogSender thread " << thread_id() << " exit!";
  }

void* ZPBinlogSendThread::ThreadMain() {
  struct timeval begin, now;

  while (!should_exit_) {
    sleep(1);
    ZPBinlogSendTask* task = NULL;
    Status s = pool_->FetchOut(&task);
    if (!s.ok()) {
      // TODO change to condition
      //LOG(INFO) << "No task to be processed";
      continue;
    }

    // Fetched one task, process it
    gettimeofday(&begin, NULL);
    while (!should_exit_) {
      // Record offset of current binlog item for sending later
      if (task->send_next) {
        // Process ProcessTask
        s = task->ProcessTask();
        if (s.IsEndFile()) {
          //LOG(INFO) << "No more binlog item for table: " << task->table_name()
          //  << " parititon: " << task->partition_id();
          pool_->PutBack(task);
          break;
        } else if (!s.ok()) {
          LOG(WARNING) << "BinlogSender Parse error, " << s.ToString();
          sleep(1);
        }
      }

      // Send binlog
      if (s.ok()) {
        client::SyncRequest sreq;
        BuildSyncRequest(task->pre_filenum(), task->pre_offset(), task->pre_content(), &sreq);
        s = zp_data_server->SendToPeer(task->node(), sreq);
        if (!s.ok()) {
          LOG(ERROR) << "Failed to send to peer " << task->node()
            << ", table:" << task->table_name() << ", partition:" << task->partition_id()
            << ", filenum:" << task->filenum() << ", offset:" << task->offset()
            << ", Error: " << s.ToString();
          task->send_next = false;
          sleep(1);
        } else {
          task->send_next = true;
        }
      }

      // Check if need to switch task
      gettimeofday(&now, NULL);
      if (now.tv_sec - begin.tv_sec > kBinlogTimeSlice) {
        // Switch Task
        pool_->PutBack(task);
        break;
      }
    }
  }

  return NULL;
}



