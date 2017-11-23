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
#include "src/node/zp_data_client_conn.h"

#include <glog/logging.h>
#include <memory>
#include "src/node/zp_data_server.h"

extern ZPDataServer* zp_data_server;

////// ZPDataClientConn ///// /
ZPDataClientConn::ZPDataClientConn(int fd, std::string ip_port,
    pink::ServerThread* server_thread) :
  PbConn(fd, ip_port, server_thread) {
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
    DLOG(WARNING) << "Receive Client command "
      << static_cast<int>(request_.type())
      << " from (" << ip_port() << "), but the server is not availible yet";
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kWait);
    response_.set_msg("server is not availible yet");
    return -1;
  }

  if (!request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_)) {
    LOG(WARNING) << "Receive Client command, but parse error";
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("command parse error");
    return -1;
  }

  Cmd* cmd = zp_data_server->CmdGet(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("unsupported cmd");
    LOG(ERROR) << "unsupported type: " << static_cast<int>(request_.type());
    return -1;
  }

  DLOG(INFO) << "Receive client cmd: " << cmd->name()
    << ", table=" << cmd->ExtractTable(&request_)
    << " key=" << cmd->ExtractKey(&request_);

  zp_data_server->PlusQueryStat(StatType::kClient, cmd->ExtractTable(&request_));

  if (!cmd->is_single_paritition()) {
    cmd->Do(&request_, &response_);
    return 0;
  }

  // Single Partition related Cmds
  std::shared_ptr<Partition> partition;
  int partition_id = cmd->ExtractPartition(&request_);
  if (partition_id >= 0) {
    partition = zp_data_server->GetTablePartitionById(
        cmd->ExtractTable(&request_), partition_id);
  } else {
    partition = zp_data_server->GetTablePartition(
        cmd->ExtractTable(&request_), cmd->ExtractKey(&request_));
  }

  if (partition == NULL) {
    // Partition not found
    response_.set_type(request_.type());
    response_.set_code(client::StatusCode::kError);
    response_.set_msg("no partition");
    return -1;
  }

  partition->DoCommand(cmd, request_, &response_);

  return 0;
}

////// ZPDataClientConnHandle ///// /
void ZPDataClientConnHandle::CronHandle() const {
  // Note: ServerCurrentQPS is the sum of client qps and sync qps;
  zp_data_server->ResetLastStat(StatType::kClient);

  Statistic stat;
  zp_data_server->GetTotalStat(StatType::kClient, &stat);
  uint64_t server_querys = stat.querys;
  uint64_t server_qps = stat.last_qps;

  zp_data_server->GetTotalStat(StatType::kSync, &stat);
  uint64_t sync_querys = stat.querys;
  server_qps += stat.last_qps;

  LOG(INFO) << " ClientQueryNum: " << server_querys
      << " SyncCmdNum: " << sync_querys
      << " ServerCurrentQps: " << server_qps;

  // zp_data_server->DumpTablePartitions();
  zp_data_server->DumpBinlogSendTask();
}
