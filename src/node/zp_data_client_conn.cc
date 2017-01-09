#include "zp_data_client_conn.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>

#include "zp_data_worker_thread.h"
#include "zp_data_server.h"

extern ZPDataServer* zp_data_server;

////// ZPDataClientConn //////
ZPDataClientConn::ZPDataClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port) {
  self_thread_ = dynamic_cast<ZPDataWorkerThread*>(thread);
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
    LOG(WARNING) << "Receive Client command, but the server is not availible yet";
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("server is not availible yet");
    return -1;
  }
  request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);

  // TODO test only
  switch (request_.type()) {
    case client::Type::SET: {
      DLOG(INFO) << "Receive Set cmd, table=" << request_.set().table_name() << " key=" << request_.set().key();
      break;
    }
    case client::Type::GET: {
      DLOG(INFO) << "Receive Get cmd, table=" << request_.get().table_name() << " key=" << request_.get().key();
      break;
    }
    case client::Type::DEL: {
      DLOG(INFO) << "Receive Del cmd, table=" << request_.del().table_name() << " key=" << request_.del().key();
      break;
    }
    case client::Type::SYNC: {
      DLOG(INFO) << "Receive Sync cmd";
      break;
    }
    default: {
      DLOG(INFO) << "Receive Info cmd " << static_cast<int>(request_.type());
      break;
    }
  }

  Cmd* cmd = zp_data_server->CmdGet(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("unsupported cmd");
    LOG(ERROR) << "unsupported type: " << (int)request_.type();
    return -1;
  }

  self_thread_->PlusStat(cmd->ExtractTable(&request_));

  // Note: We treat 3 kinds of InfoCmds as 1 InfoCmd;
  // we just run InfoCmd here.
  if (request_.type() == client::Type::INFOSTATS
      || request_.type() == client::Type::INFOCAPACITY
      || request_.type() == client::Type::INFOPARTITION) {
    cmd->Do(&request_, &response_);
    return 0;
  }

  // Single Partition related Cmds
  Partition* partition = NULL;
  if (request_.type() ==  client::Type::SYNC) {
    partition = zp_data_server->GetTablePartitionById(cmd->ExtractTable(&request_),
        request_.sync().sync_offset().partition());
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

