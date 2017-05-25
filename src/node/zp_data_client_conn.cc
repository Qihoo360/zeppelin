#include "src/node/zp_data_client_conn.h"

#include <glog/logging.h>
#include "src/node/zp_data_server.h"

extern ZPDataServer* zp_data_server;

////// ZPDataClientConn //////
ZPDataClientConn::ZPDataClientConn(int fd, std::string ip_port,
    pink::Thread* thread) :
  PbConn(fd, ip_port, thread) {
}

ZPDataClientConn::~ZPDataClientConn() {
}

int ZPDataClientConn::DealMessage() {
  set_is_reply(true);
  int s = DealMessageInternal();
  res_ = &response_;
  return s;
}

// Msg is  [ length (int32) | pb_msg (length bytes) ]
int ZPDataClientConn::DealMessageInternal() {
  if (!zp_data_server->Availible()) {
    LOG(WARNING) << "Receive Client command " << static_cast<int>(request_.type())
      << " from (" << ip_port() << "), but the server is not availible yet";
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("server is not availible yet");
    return -1;
  }

  if (!request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_)) {
    LOG(WARNING) << "Receive Client command, but parse error";
    return -1;
  }

  Cmd* cmd = zp_data_server->CmdGet(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("unsupported cmd");
    LOG(ERROR) << "unsupported type: " << (int)request_.type();
    return -1;
  }

  DLOG(INFO) << "Receive client cmd: " << cmd->name()
    << ", table=" << cmd->ExtractTable(&request_)
    << " key=" << cmd->ExtractKey(&request_);

  //self_thread_->PlusStat(cmd->ExtractTable(&request_));

  if (!cmd->is_single_paritition()) {
    cmd->Do(&request_, &response_);
    return 0;
  }

  // Single Partition related Cmds
  std::shared_ptr<Partition> partition;
  int partition_id = cmd->ExtractPartition(&request_);
  if (partition_id >= 0) {
    partition = zp_data_server->GetTablePartitionById(cmd->ExtractTable(&request_),
        partition_id);
  } else {
    partition = zp_data_server->GetTablePartition(cmd->ExtractTable(&request_),
        cmd->ExtractKey(&request_));
  }


  if (partition == NULL) {
    // Partition not found
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("no partition");
    return -1;
  }

  partition->DoCommand(cmd, request_, response_);

  return 0;
}

