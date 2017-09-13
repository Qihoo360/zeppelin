// Copyright 2017 Qihoo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "src/meta/zp_meta_update_thread.h"
#include <google/protobuf/text_format.h>
#include "include/zp_const.h"
#include "include/zp_meta.pb.h"
#include "src/meta/zp_meta_server.h"
#include "src/meta/zp_meta_info_store.h"

extern ZPMetaServer* g_meta_server;

ZPMetaUpdateThread::ZPMetaUpdateThread(ZPMetaInfoStore* is,
    ZPMetaMigrateRegister* m)
  : is_stuck_(false),
  should_stop_(true),
  info_store_(is),
  migrate_(m) {
  worker_ = new pink::BGThread();
  worker_->set_thread_name("ZPMetaUpdate");
}

ZPMetaUpdateThread::~ZPMetaUpdateThread() {
  worker_->StopThread();
  delete worker_;
}

// Invoker should handle the Pending failed situation
Status ZPMetaUpdateThread::PendingUpdate(const UpdateTask &task) {
  // This check and set is not atomic, since it is acceptable
  if (is_stuck_) {
    return Status::Incomplete("Update thread stucked");
  }

  slash::MutexLock l(&task_mutex_);
  if (should_stop_) {
    return Status::Incomplete("Update thread should be stop");
  }
  task_deque_.push_back(task);

  if (task_deque_.size() == 1) {
    worker_->DelaySchedule(kMetaDispathCronInterval,
        &UpdateFunc, static_cast<void*>(this));
  }
  return Status::OK();
}

void ZPMetaUpdateThread::Active() {
  slash::MutexLock l(&task_mutex_);
  int ret = worker_->StartThread();
  if (ret != 0) {
    LOG(FATAL) << "Start update thread failed: " << ret;
    return;
  }
  LOG(INFO) << "Start update thread succ: " << std::hex
    << worker_->thread_id(); 
  should_stop_ = false;
}

void ZPMetaUpdateThread::Abandon() {
  {
  slash::MutexLock l(&task_mutex_);
  should_stop_ = true;
  task_deque_.clear();
  }
  worker_->StopThread();
  worker_->QueueClear();
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

Status ZPMetaUpdateThread::ApplyUpdates(
    const ZPMetaUpdateTaskDeque& task_deque) {
  LOG(INFO) << "Begin Appply Updates, task count: " << task_deque.size();
  // Get current meta info
  ZPMetaInfoStoreSnap info_store_snap;
  info_store_->GetSnapshot(&info_store_snap);

  Status s;
  bool has_succ = false;
  int handover_count = 0;
  for (const auto cur_task : task_deque) {
    LOG(INFO) << "Apply one task, task type: "
      << static_cast<int>(cur_task.op)
      << ", table: " << cur_task.table
      << ", partition: " << cur_task.partition
      << ", ip_port: " << cur_task.ip_port
      << ", ip_port_o: " << cur_task.ip_port_o;
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
      case ZPMetaUpdateOP::kOpHandover:
        handover_count++;
        s = info_store_snap.Handover(cur_task.table, cur_task.partition,
            cur_task.ip_port, cur_task.ip_port_o);
        break;
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
        s = info_store_snap.ChangePState(cur_task.table,
            cur_task.partition, true);
        break;
      case ZPMetaUpdateOP::kOpSetActive:
        s = info_store_snap.ChangePState(cur_task.table,
            cur_task.partition, false);
        break;
      default:
        s = Status::Corruption("Unknown task type");
    }

    if (!s.ok()) {
      LOG(WARNING) << "Update task process failed: " << s.ToString()
        << ", task: (" << static_cast<int>(cur_task.op)
        << ", " << cur_task.table
        << ", " << cur_task.partition
        << ", " << cur_task.ip_port << ")";
    } else {
      has_succ = true;
    }
  }

  if (!has_succ) {
    // No succ item
    LOG(WARNING) << "No update apply task succ";
    migrate_->PutN(handover_count);
    return Status::Corruption("No update apply task succ");
  }

  // Check node alive and change table master
  info_store_snap.RefreshTableWithNodeAlive();

  // Write back to info_store
  s = info_store_->Apply(info_store_snap);
  while (!should_stop_ && s.IsIOError()) {
    is_stuck_ = true;
    LOG(WARNING) << "Failed to apply update change since floyd error: "
      << s.ToString() << ", Retry in 1 second";
    sleep(1);
    s = info_store_->Apply(info_store_snap);
  }
  is_stuck_ = false;

  if (should_stop_) {
    migrate_->PutN(handover_count);
    return s;
  }

  if (!s.ok()) {
    // It's a good choice to just discard the error task, because:
    // 1, It couldn't be recovery
    // 2, As our design, most of the task could be retry outside,
    //    such as those were launched by Ping or Migrate Process.
    //    The rest comes from admin command,
    //    whose lost is acceptable and could be retry by administrator.
    LOG(ERROR) << "Failed to apply updates to info_store: " << s.ToString();
    migrate_->PutN(handover_count);
    return s;
  }

  // Some finish touches
  for (const auto cur_task : task_deque) {
    if (cur_task.op == ZPMetaUpdateOP::kOpHandover) {
      migrate_->Erase(
          DiffKey(cur_task.table,
            cur_task.partition,
            cur_task.ip_port_o,
            cur_task.ip_port));
      LOG(INFO) << "Migrate item finish, partition: "
        << cur_task.table << "_" << cur_task.partition
        << ", from: " << cur_task.ip_port_o
        << ", to: " << cur_task.ip_port;
    }
  }
  migrate_->PutN(handover_count);
  return s;
}

