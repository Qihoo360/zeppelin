#include "src/meta/zp_meta_update_thread.h"

#include <google/protobuf/text_format.h>

#include "include/zp_const.h"
#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_server.h"

extern ZPMetaServer* g_meta_server;

ZPMetaUpdateThread::ZPMetaUpdateThread() {
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

  // TODO(wk) implement in update thread
  slash::Status s = g_meta_server->DoUpdate(tasks);
  if (s.IsIncomplete()) {
    thread->PendingUpdate(UpdateTask(kOpAddVersion, "pad_ip:6666", "", -1));
  }
  if (!s.ok()) {
    for (const auto& t : tasks) {
      thread->PendingUpdate(t, true);
    }
  }
  return s;
}
