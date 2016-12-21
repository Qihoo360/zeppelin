#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_data_partition.h"
#include "zp_binlog_receiver_thread.h"
#include "zp_binlog_receive_bgworker.h"

extern ZPDataServer* zp_data_server;

ZPBinlogReceiveBgWorker::ZPBinlogReceiveBgWorker(int full) {
  bg_thread_ = new pink::BGThread(full);
}

ZPBinlogReceiveBgWorker::~ZPBinlogReceiveBgWorker() {
  bg_thread_->Stop();
  delete bg_thread_;
  LOG(INFO) << "A ZPBinlogReceiveBgWorker " << bg_thread_->thread_id() << " exit!!!";
}

void ZPBinlogReceiveBgWorker::AddTask(ZPBinlogReceiveTask *task) {
  bg_thread_->StartIfNeed();
  bg_thread_->Schedule(&DoBinlogReceiveTask, static_cast<void*>(task));
}

void ZPBinlogReceiveBgWorker::DoBinlogReceiveTask(void* task) {
  ZPBinlogReceiveTask *task_ptr = static_cast<ZPBinlogReceiveTask*>(task);
  uint32_t partition_id = task_ptr->partition_id;
  const Cmd *cmd = task_ptr->cmd;

  Partition* partition = zp_data_server->GetTablePartitionById(cmd->ExtractTable(&task_ptr->request), partition_id);
  if (partition == NULL) {
    LOG(WARNING) << "No partition found for binlog receive bgworker, Partition: "
      << partition->partition_id();
    return;
  }
  if (partition->role() != Role::kNodeSlave) {
    LOG(WARNING) << "Not a slave, ignore the binlog request, Partition: "
      << partition->partition_id();
    return;
  }
  partition->DoBinlogCommand(cmd, task_ptr->request, task_ptr->raw, task_ptr->from_node);
  delete task_ptr;
}

