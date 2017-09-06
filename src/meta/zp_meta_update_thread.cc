#include "src/meta/zp_meta_update_thread.h"
#include <google/protobuf/text_format.h> 
#include "include/zp_const.h"
#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_server.h"
#include "src/meta/zp_meta_info_store.h"

extern ZPMetaServer* g_meta_server;

ZPMetaUpdateThread::ZPMetaUpdateThread(ZPMetaInfoStore* is)
  : is_stuck_(false),
  info_store_(is) {
  worker_ = new pink::BGThread();
  worker_->set_thread_name("ZPMetaUpdate");
}

ZPMetaUpdateThread::~ZPMetaUpdateThread() {
  worker_->StopThread();
  delete worker_;
}

Status ZPMetaUpdateThread::PendingUpdate(const UpdateTask &task) {
  if (is_stuck_) {
    return Status::Incomplete("Update thread stucked");
  }
  slash::MutexLock l(&task_mutex_);
  task_deque_.push_back(task);

  if (task_deque_.size() == 1) {
    worker_->StartThread();
    worker_->DelaySchedule(kMetaDispathCronInterval,
        &UpdateFunc, static_cast<void*>(this));
  }
}

void ZPMetaUpdateThread::UpdateFunc(void *p) {
  ZPMetaUpdateThread *thread = static_cast<ZPMetaUpdateThread*>(p);

  ZPMetaUpdateTaskDeque tasks;
  {
    slash::MutexLock l(&(thread->task_mutex_));
    tasks = thread->task_deque_;
    thread->task_deque_.clear();
  }

  thread->ApplyUpdates(tasks);
}

Status ZPMetaUpdateThread::ApplyUpdates(ZPMetaUpdateTaskDeque& task_deque) {
  // Get current meta info
  ZPMetaInfoStoreSnap info_store_snap;
  info_store_->GetSnapshot(&info_store_snap);
  
  Status s;
  while (!task_deque.empty()) {
    UpdateTask cur_task = task_deque.front();
    task_deque.pop_front();
    switch (cur_task.op) {
      case ZPMetaUpdateOP::kOpUpNode:
        s = info_store_snap.UpNode(cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpDownNode:
        s = info_store_snap.DownNode(cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpAddSlave:
        s = info_store_snap.AddSlave(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpRemoveDup:
        s = info_store_snap.DeleteDup(cur_task.table, cur_task.partition,
            cur_task.ip_port);
      case ZPMetaUpdateOP::kOpRemoveSlave:
        s = info_store_snap.DeleteSlave(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpSetMaster:
        s = info_store_snap.SetMaster(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpAddTable:
        s = info_store_snap.AddTable(cur_task.table, cur_task.partition);
        break;
      case ZPMetaUpdateOP::kOpRemoveTable:
        s = info_store_snap.RemoveTable(cur_task.table);
        break;
      case ZPMetaUpdateOP::kOpSetStuck:
        s = info_store_snap.SetStuck(cur_task.table, cur_task.partition);
        break;
      default:
        s = Status::Corruption("Unknown task type");
    }

    if (!s.ok()) {
      LOG(WARNING) << "Update task process failed: " << s.ToString()
        << "task: (" << static_cast<int>(cur_task.op) << ", " << cur_task.table
        << ", " << cur_task.partition << ", " << cur_task.ip_port;
    }
  }

  // Check node alive and change table master
  info_store_snap.RefreshTableWithNodeAlive();
  
  // Write back to info_store
  s = info_store_->Apply(info_store_snap);
  while (s.IsIOError()) {
    is_stuck_ = true;
    LOG(WARNING) << "Failed to apply update change since floyd error: "
      << s.ToString() << ", Retry in 1 second";
    sleep(1);
    s = info_store_->Apply(info_store_snap);
  }
  is_stuck_ = false;

  if (!s.ok()) {
    LOG(ERROR) << "Failed to apply update change to info_store: " << s.ToString();
  } 
  return s;
}

