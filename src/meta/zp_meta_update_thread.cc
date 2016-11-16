#include <google/protobuf/text_format.h>
#include "zp_const.h"
#include "zp_meta_update_thread.h"
#include "zp_meta.pb.h"
#include "zp_meta_server.h"

extern ZPMetaServer* zp_meta_server;

ZPMetaUpdateThread::ZPMetaUpdateThread() {
}

ZPMetaUpdateThread::~ZPMetaUpdateThread() {
  worker_.Stop();
}

//void ZPMetaUpdateThread::ScheduleUpdate(const std::string ip_port, ZPMetaUpdateOP op) {
//  std::string ip;
//  int port;
//  if (!slash::ParseIpPortString(ip_port, ip, port)) {
//    return;
//  }
//  ZPMetaUpdateArgs* arg = new ZPMetaUpdateArgs(this, ip, port, op);
//  worker_.StartIfNeed();
//  LOG(INFO) << "Schedule to update thread worker, update: " << op << " " << ip << " " << port;
//  worker_.Schedule(&DoMetaUpdate, static_cast<void*>(arg));
//}

void ZPMetaUpdateThread::ScheduleUpdate(ZPMetaUpdateTaskMap task_map) {
  ZPMetaUpdateArgs* arg = new ZPMetaUpdateArgs(this, task_map);
  worker_.StartIfNeed();
  LOG(INFO) << "Schedule to update thread worker, task num: " << task_map.size();
  worker_.Schedule(&DoMetaUpdate, static_cast<void*>(arg));
}

//void ZPMetaUpdateThread::DoMetaUpdate(void *p) {
//  ZPMetaUpdateThread::ZPMetaUpdateArgs *args = static_cast<ZPMetaUpdateThread::ZPMetaUpdateArgs*>(p);
//  ZPMetaUpdateThread *thread = args->thread;
//  thread->MetaUpdate(args->ip, args->port, args->op);
//
//  delete args;
//}

void ZPMetaUpdateThread::DoMetaUpdate(void *p) {
  ZPMetaUpdateThread::ZPMetaUpdateArgs *args = static_cast<ZPMetaUpdateThread::ZPMetaUpdateArgs*>(p);
  ZPMetaUpdateThread *thread = args->thread;
  thread->MetaUpdate(args->task_map);

  delete args;
}

//slash::Status ZPMetaUpdateThread::MetaUpdate(const std::string ip, int port, ZPMetaUpdateOP op) {
//
//  slash::Status s;
//
//  ZPMeta::MetaCmd_Pull ms_info;
//
//  if (ZPMetaUpdateOP::kOpAdd == op) {
//    
//  } else if (ZPMetaUpdateOP::kOpRemove == op) {
//    s = zp_meta_server->OffNode(ip, port);
//    if (!s.ok()) {
//      LOG(ERROR) << "OffNode error in MetaUpdate, error: " << s.ToString();
//      return s;
//    }
//    LOG(INFO) << "Update meta success, update: " << op << " " << ip << " " << port;
//  }
//  return s;
//}

slash::Status ZPMetaUpdateThread::MetaUpdate(ZPMetaUpdateTaskMap task_map) {
  slash::Status s;
  s = zp_meta_server->DoUpdate(task_map);
  return s;
}
