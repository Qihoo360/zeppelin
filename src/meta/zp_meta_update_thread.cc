#include "src/meta/zp_meta_update_thread.h"
#include <google/protobuf/text_format.h> 
#include "include/zp_const.h"
#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_server.h"

#include "src/meta/zp_meta_info_store.h"

extern ZPMetaServer* g_meta_server;

ZPMetaUpdateThread(ZPMetaInfoStore* is)
  info_store_(is) {
  worker_ = new pink::BGThread();
  worker_->set_thread_name("ZPMetaUpdate");
}

ZPMetaUpdateThread::~ZPMetaUpdateThread() {
  worker_->StopThread();
  delete worker_;
}

void ZPMetaUpdateThread::PendingUpdate(const UpdateTask &task, bool priority) {
  slash::MutexLock l(&task_mutex_);
  if (priority) {
    task_deque_.push_front(task);
  } else {
    task_deque_.push_back(task);
  }

  if (task_deque_.size() == 1) {
    worker_->StartThread();
    worker_->DelaySchedule(kMetaDispathCronInterval,
        &DoMetaUpdate, static_cast<void*>(this));
  }
}

void ZPMetaUpdateThread::DoMetaUpdate(void *p) {
  ZPMetaUpdateThread *thread = static_cast<ZPMetaUpdateThread*>(p);

  ZPMetaUpdateTaskDeque tasks;
  {
    slash::MutexLock l(&task_mutex_);
    tasks = task_queue_;
    task_queue_.clear();
  }

  ApplyUpdates(tasks);
}

Status ZPMetaUpdateThread::ApplyUpdates(ZPMetaUpdateTaskDeque& task_deque) {
  // Get current meta info
  ZPMetaInfoStoreSnap info_store_snap;
  info_store_->GetSnapshot(&info_store_snap);
  
  Status s;
  while (!task_deque.empty()) {
    UpdateTask cur_task = task_deque.pop_front();
    switch (cur_task.op) {
      case ZPMetaUpdateOP::kOpUpNode:
        s = info_store_snap.UpNode(cur_task.ip_port)
        break;
      case ZPMetaUpdateOP::kOpDownNode:
        s = info_store_snap.DownNode(cur_task.ip_port)
        break;
      case ZPMetaUpdateOP::kOpAddSlave:
        s = info_store_snap.AddSlave(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpRemoveSlave:
        s = info_store_snap.RemoveSlave(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpSetMaster:
        s = info_store_snap.SetMaster(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      default:
        s = Status::Corruption("Unknown task type");
    }

    if (!s.ok()) {
      LOG(WARNING) << "Update task process failed: " << s.ToString()
        << "task: (" << cur_task.op << ", " << cur_task.table
        << ", " << cur_task.partition << ", " cur_task.ip_port;
    }
  }

  // Check node alive and change table master
  info_store_snap.RefreshTableWithNodeAlive();
  
  // Write back to info_store
  s = info_store_->Apply(info_store_snap);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to apply update change to info_store: " << s.ToString();
  } 
  return s;
}

