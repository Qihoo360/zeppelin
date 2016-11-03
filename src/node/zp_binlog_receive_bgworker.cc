#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_data_partition.h"
#include "zp_binlog_receiver_thread.h"
#include "zp_binlog_receive_bgworker.h"

extern ZPDataServer* zp_data_server;

ZPBinlogReceiveBgWorker::~ZPBinlogReceiveBgWorker() {
  Stop();
  LOG(INFO) << "A ZPBinlogReceiveBgWorker " << thread_id() << " exit!!!";
}

void ZPBinlogReceiveBgWorker::DoBinlogReceiveTask(void* arg) {
  ZPBinlogReceiveArg *barg = static_cast<ZPBinlogReceiveArg*>(arg);
  uint32_t partition_id = barg->partition_id;
  const Cmd *cmd = barg->cmd;

  Partition* partition = zp_data_server->GetPartitionById(partition_id);
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
  partition->DoBinlogCommand(cmd, barg->request, barg->raw);
  delete barg;
}

