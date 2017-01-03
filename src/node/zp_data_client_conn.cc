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

// Msg is  [ length (int32) | pb_msg (length bytes) ]
int ZPDataClientConn::DealMessage() {
  if (!zp_data_server->Availible()) {
    LOG(WARNING) << "Receive Client command, but the server is not availible yet";
    return -1;
  }
  self_thread_->PlusThreadQuerynum();
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
  }

  Cmd* cmd = zp_data_server->CmdGet(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    LOG(ERROR) << "unsupported type: " << (int)request_.type();
    return -1;
  }

  set_is_reply(true);
  //std::string raw_msg(rbuf_ + cur_pos_ - header_len_ - 4, header_len_ + 4);

  Partition* partition = NULL;
  if (request_.type() ==  client::Type::SYNC) {
    partition = zp_data_server->GetTablePartitionById(cmd->ExtractTable(&request_),
        request_.sync().sync_offset().partition());
  } else {
    partition = zp_data_server->GetTablePartition(cmd->ExtractTable(&request_), cmd->ExtractKey(&request_));
  }

  if (partition == NULL) {
    // Partition not found
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("no partition");
    res_ = &response_;
    return -1;
  }

  partition->DoCommand(cmd, request_, response_);

  res_ = &response_;
  return 0;
}

