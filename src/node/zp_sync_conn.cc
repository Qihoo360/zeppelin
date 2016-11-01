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
  if (!zp_data_server->Availible()) {
    LOG(WARNING) << "Receive Client command, but the server is not availible yet";
    return -1;
  }
  self_thread_->PlusQueryNum();

  request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);

  Cmd* cmd = zp_data_server->CmdGet(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported type: " << (int)request_.type();
    return -1;
  }

  // do not reply
  set_is_reply(false);
  
  // We need to malloc for args need by binglog_bgworker
  // So that it will not be free after the executing of current function
  // Remeber to free these space by the binlog_bgworker at the end of its task
  ZPBinlogReceiveArg *arg = new ZPBinlogReceiveArg(
      0, // Will be filled by zp_data_server
      cmd,
      request_,
      std::string(rbuf_ + cur_pos_ - header_len_ - 4, header_len_ + 4));

  zp_data_server->DispatchBinlogBGWorker(cmd->ExtractKey(&request_), arg);

  return 0;
}
