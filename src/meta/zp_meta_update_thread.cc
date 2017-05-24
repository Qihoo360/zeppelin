#include "src/meta/zp_meta_update_thread.h"

#include <google/protobuf/text_format.h>

#include "include/zp_const.h"
#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_server.h"

extern ZPMetaServer* g_meta_server;

ZPMetaUpdateThread::ZPMetaUpdateThread() {
  worker_.set_thread_name("ZPMetaUpdate");
}

ZPMetaUpdateThread::~ZPMetaUpdateThread() {
  worker_.StopThread();
}

void ZPMetaUpdateThread::ScheduleUpdate(ZPMetaUpdateTaskDeque task_deque) {
  ZPMetaUpdateArgs* arg = new ZPMetaUpdateArgs(this, task_deque);
  worker_.StartThread();
  LOG(INFO) << "Schedule to update thread worker, task num: " << task_deque.size();
  worker_.Schedule(&DoMetaUpdate, static_cast<void*>(arg));
}

void ZPMetaUpdateThread::DoMetaUpdate(void *p) {
  ZPMetaUpdateThread::ZPMetaUpdateArgs *args = static_cast<ZPMetaUpdateThread::ZPMetaUpdateArgs*>(p);
  ZPMetaUpdateThread *thread = args->thread;
  thread->MetaUpdate(args->task_deque);

  delete args;
}

slash::Status ZPMetaUpdateThread::MetaUpdate(ZPMetaUpdateTaskDeque task_deque) {
  slash::Status s;
  s = g_meta_server->DoUpdate(task_deque);
  if (s.IsCorruption() && s.ToString() == "Corruption: AddVersion Error") {
    UpdateTask task = {kOpAddVersion, "pad_ip:6666", "", -1};
    g_meta_server->AddMetaUpdateTask(task);
  }
  if (!s.ok()) {
    sleep(1);
    g_meta_server->AddMetaUpdateTaskDequeFromFront(task_deque);
  }
  return s;
}
