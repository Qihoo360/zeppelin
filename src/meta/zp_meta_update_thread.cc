#include <google/protobuf/text_format.h>
#include "zp_const.h"
#include "zp_meta_update_thread.h"
#include "zp_meta.pb.h"
#include "zp_meta_server.h"

extern ZPMetaServer* g_meta_server;

ZPMetaUpdateThread::ZPMetaUpdateThread() {
}

ZPMetaUpdateThread::~ZPMetaUpdateThread() {
  worker_.Stop();
}

void ZPMetaUpdateThread::ScheduleUpdate(ZPMetaUpdateTaskMap task_map) {
  ZPMetaUpdateArgs* arg = new ZPMetaUpdateArgs(this, task_map);
  worker_.StartIfNeed();
  LOG(INFO) << "Schedule to update thread worker, task num: " << task_map.size();
  worker_.Schedule(&DoMetaUpdate, static_cast<void*>(arg));
}

void ZPMetaUpdateThread::DoMetaUpdate(void *p) {
  ZPMetaUpdateThread::ZPMetaUpdateArgs *args = static_cast<ZPMetaUpdateThread::ZPMetaUpdateArgs*>(p);
  ZPMetaUpdateThread *thread = args->thread;
  thread->MetaUpdate(args->task_map);

  delete args;
}

slash::Status ZPMetaUpdateThread::MetaUpdate(ZPMetaUpdateTaskMap task_map) {
  slash::Status s;
  s = g_meta_server->DoUpdate(task_map);
  if (s.IsCorruption() && s.ToString() == "Corruption: AddVersion Error") {
    g_meta_server->AddMetaUpdateTask("", kOpAddVersion);
  }
  if (!s.ok()) {
    sleep(1);
    for (auto iter = task_map.begin(); iter != task_map.end(); iter++) {
      g_meta_server->AddMetaUpdateTask(iter->first, iter->second);
    }
  }
  return s;
}
