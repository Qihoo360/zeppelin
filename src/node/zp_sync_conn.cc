#include "zp_sync_conn.h"

#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_data_partition.h"
#include "zp_binlog_receiver_thread.h"

extern ZPDataServer* zp_data_server;

ZPSyncConn::ZPSyncConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = dynamic_cast<ZPBinlogReceiverThread*>(thread);
}

ZPSyncConn::~ZPSyncConn() {
}

int ZPSyncConn::DealMessage() {
  self_thread_->PlusQueryNum();

  request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);

  Cmd* cmd = self_thread_->GetCmd(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported type: " << (int)request_.type();
    return -1;
  }
  Status s = cmd->Init(&request_);
  if (!s.ok()) {
    LOG(ERROR) << "Cmd init failed" << s.ToString();
    return -1;
  }

  // do not reply
  set_is_reply(false);
  std::string raw_msg(rbuf_ + cur_pos_ - header_len_ - 4, header_len_ + 4);
  Partition* partition = zp_data_server->GetPartition(cmd->key());
  if (partition == NULL) {
    LOG(ERROR) << "Error partition";
  }
  partition->DoBinlogCommand(cmd, request_, response_, raw_msg);

  res_ = &response_;
  return 0;
}
