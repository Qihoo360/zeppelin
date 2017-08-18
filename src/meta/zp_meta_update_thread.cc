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

  slash::Status s = ApplyUpdates(tasks);
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

Status ZPMetaUpdateThread::ApplyUpdates(ZPMetaUpdateTaskDeque& task_deque) {
  // Get current meta info
  ZPMetaInfoStoreSnap info_store_snap;
  info_store_->GetSnapshot(&info_store_snap);
  
  ZPMetaUpdateTaskDeque failed_tasks;
  while (!task_deque.empty()) {
    UpdateTask cur_task = task_deque.pop_front();
    switch (cur_task.op) {
      case ZPMetaUpdateOP::kOpUpNode:
        info_store_snap.UpNode(cur_task.ip_port)
        break;
      case ZPMetaUpdateOP::kOpDownNode:
        info_store_snap.DownNode(cur_task.ip_port)
        break;
      case ZPMetaUpdateOP::kOpAddSlave:
        info_store_snap.AddSlave(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpRemoveSlave:
        info_store_snap.RemoveSlave(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      case ZPMetaUpdateOP::kOpSetMaster:
        info_store_snap.SetMaster(cur_task.table, cur_task.partition,
            cur_task.ip_port);
        break;
      default:
        LOG(WARNING) << "Unknown task type: " << cur_task.op;
    }
  }
  info_store_snap.RefreshTableWithNodeAlive();

  
  /*
   * Step 2. Apply every update task on every Table
   */
  bool should_update_version = false;
  //  bool should_update_table_set = false;

  for (auto it = tables.begin(); it != tables.end(); it++) {
    table_info.Clear();
    s = GetTableInfo(*it, &table_info);
    if (!s.ok() && !s.IsNotFound()) {
      LOG(ERROR) << "GetTableInfo error in DoUpdate, table: " << *it << " error: " << s.ToString();
      sth_wrong = true;
      continue;
    }
    if (!ProcessUpdateTableInfo(task_deque, nodes, &table_info, &should_update_version)) {
      sth_wrong = true;
      continue;
    }
  }

  /*
   * Step 3. AddClearStuckTaskifNeeded
   */

  AddClearStuckTaskIfNeeded(task_deque);

  /*
   * Step 4. Check whether should we add version after Step [1-2]
   * or is there kOpAddVersion task in task deque, if true,
   * add version
   */

  if (should_update_version || ShouldRetryAddVersion(task_deque)) {
    s = AddVersion();
    if (!s.ok()) {
      return Status::Incomplete("AddVersion Error");
    }
  }

  if (sth_wrong) {
    return Status::Corruption("sth wrong");
  }

  return Status::OK();
}

bool ZPMetaServer::ProcessUpdateTableInfo(const ZPMetaUpdateTaskDeque task_deque, const ZPMeta::Nodes &nodes, ZPMeta::Table *table_info, bool *should_update_version) {

  bool should_update_table_info = false;
  std::string ip;
  int port = 0;
  for (auto iter = task_deque.begin(); iter != task_deque.end(); iter++) {
    LOG(INFO) << "process task in ProcessUpdateTableInfo: " << iter->ip_port << ", " << static_cast<int>(iter->op);
    if (!slash::ParseIpPortString(iter->ip_port, ip, port)) {
      return false;
    }
    if (iter->op == ZPMetaUpdateOP::kOpAdd) {
      DoUpNodeForTableInfo(table_info, ip, port, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpRemove) {
      DoDownNodeForTableInfo(nodes, table_info, ip, port, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpSetMaster) {
      DoSetMasterForTableInfo(table_info, iter->table, iter->partition, ip, port, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpClearStuck) {
      DoClearStuckForTableInfo(table_info, iter->table, iter->partition, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpAddSlave) {
      DoAddSlaveForTableInfo(table_info, iter->table, iter->partition, ip, port, &should_update_table_info);
    } else if (iter->op == ZPMetaUpdateOP::kOpRemoveSlave) {
      DoRemoveSlaveForTableInfo(table_info, iter->table, iter->partition, ip, port, &should_update_table_info);
    }
  }

  if (should_update_table_info) {
    Status s = SetTable(*table_info);
    if (!s.ok()) {
      LOG(ERROR) << "SetTable in ProcessUpdateTableInfo error: " << s.ToString();
      return false;
    } else {
     *should_update_version = true;
    }
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(*table_info, &text_format);
    LOG(INFO) << "table_info : [" << text_format << "]";
  }
  return true;
}

void ZPMetaServer::AddClearStuckTaskIfNeeded(const ZPMetaUpdateTaskDeque &task_deque) {
  for (auto iter = task_deque.begin(); iter != task_deque.end(); iter++) {
    if (iter->op == ZPMetaUpdateOP::kOpSetMaster) {
      UpdateTask task = *iter;
      task.op = ZPMetaUpdateOP::kOpClearStuck;
      LOG(INFO) << "Add Clear Stuck Task for " << task.table << " : " << task.partition; 
      update_thread_->PendingUpdate(task);
    }
  }
}

bool ZPMetaServer::ShouldRetryAddVersion(const ZPMetaUpdateTaskDeque task_deque) {
  for (auto iter = task_deque.begin(); iter != task_deque.end(); iter++) {
    if (iter->op == ZPMetaUpdateOP::kOpAddVersion) {
      return true;
    }
  }
  return false;
}


