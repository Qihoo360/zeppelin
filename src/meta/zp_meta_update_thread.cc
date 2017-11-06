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
#include "src/meta/zp_meta.pb.h"
#include "src/meta/zp_meta_server.h"
#include "src/meta/zp_meta_info_store.h"

extern ZPMetaServer* g_meta_server;

ZPMetaUpdateThread::ZPMetaUpdateThread(ZPMetaInfoStore* is,
    ZPMetaMigrateRegister* m)
  : is_stuck_(false),
  should_stop_(true),
  info_store_(is),
  migrate_(m) {
  worker_ = new pink::BGThread(1024 * 1024 * 256);
  worker_->set_thread_name("ZPMetaUpdate");
}

ZPMetaUpdateThread::~ZPMetaUpdateThread() {
  Abandon();
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
  LOG(INFO) << "Begin Apply Updates, task count: " << task_deque.size();
  // Get current meta info
  ZPMetaInfoStoreSnap info_store_snap;
  info_store_->GetSnapshot(&info_store_snap);

  Status s;
  bool has_succ = false;
  int handover_count = 0;

  // Will parse from task's arguments
  std::string node;
  std::string table_name;
  std::string right_node;
  std::string left_node;
  ZPMeta::Table table;
  ZPMeta::MetaCmd_RemoveNodes remove_nodes_cmd;
  int partition;

  for (const auto cur_task : task_deque) {
    LOG(INFO) << "Apply one task, " << cur_task.print_args_text();
    switch (cur_task.op) {
      case ZPMetaUpdateOP::kOpUpNode:
        node = cur_task.sargs[0];
        s = info_store_snap.UpNode(node);
        break;
      case ZPMetaUpdateOP::kOpDownNode:
        node = cur_task.sargs[0];
        s = info_store_snap.DownNode(node);
        break;
      case ZPMetaUpdateOP::kOpRemoveNodes:
        remove_nodes_cmd.ParseFromString(cur_task.sargs[0]);
        s = info_store_snap.RemoveNodes(remove_nodes_cmd);
        break;
      case ZPMetaUpdateOP::kOpAddSlave:
        node = cur_task.sargs[0];
        table_name = cur_task.sargs[1];
        partition = cur_task.iargs[0];
        s = info_store_snap.AddSlave(table_name, partition, node);
        break;
      case ZPMetaUpdateOP::kOpHandover:
        left_node = cur_task.sargs[0];
        right_node = cur_task.sargs[1];
        table_name = cur_task.sargs[2];
        partition = cur_task.iargs[0];

        handover_count++;
        s = info_store_snap.Handover(table_name, partition, left_node, right_node);
        break;
      case ZPMetaUpdateOP::kOpRemoveSlave:
        node = cur_task.sargs[0];
        table_name = cur_task.sargs[1];
        partition = cur_task.iargs[0];
        s = info_store_snap.DeleteSlave(table_name, partition, node);
        break;
      case ZPMetaUpdateOP::kOpSetMaster:
        node = cur_task.sargs[0];
        table_name = cur_task.sargs[1];
        partition = cur_task.iargs[0];
        s = info_store_snap.SetMaster(table_name, partition, node);
        break;
      case ZPMetaUpdateOP::kOpAddTable:
        table.ParseFromString(cur_task.sargs[0]);
        s = info_store_snap.AddTable(table);
        break;
      case ZPMetaUpdateOP::kOpRemoveTable:
        table_name = cur_task.sargs[0];
        s = info_store_snap.RemoveTable(table_name);
        break;
      case ZPMetaUpdateOP::kOpSetStuck:
        table_name = cur_task.sargs[0];
        partition = cur_task.iargs[0];
        s = info_store_snap.ChangePState(table_name, partition, true);
        break;
      case ZPMetaUpdateOP::kOpSetActive:
        table_name = cur_task.sargs[0];
        partition = cur_task.iargs[0];
        s = info_store_snap.ChangePState(table_name, partition, false);
        break;
      default:
        s = Status::Corruption("Unknown task type");
    }

    if (!s.ok()) {
      LOG(WARNING) << "Update task process failed: " << s.ToString() << ", "
        << cur_task.print_args_text();
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
  LOG(INFO) << "Apply update change succ";

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
      left_node = cur_task.sargs[0];
      right_node = cur_task.sargs[1];
      table_name = cur_task.sargs[2];
      partition = cur_task.iargs[0];

      migrate_->Erase(DiffKey(table_name, partition, left_node, right_node));

      LOG(INFO) << "Migrate item finish, partition: "
        << table_name << "_" << partition
        << ", from: " << left_node
        << ", to: " << right_node;
    }
  }
  return s;
}

