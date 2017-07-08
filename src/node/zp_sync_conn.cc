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
#include "src/node/zp_sync_conn.h"

#include <glog/logging.h>
#include "src/node/zp_data_server.h"

extern ZPDataServer* zp_data_server;

ZPSyncConn::ZPSyncConn(int fd, std::string ip_port,
    pink::ServerThread* server_thread) :
  PbConn(fd, ip_port, server_thread) {
}

ZPSyncConn::~ZPSyncConn() {
}

void ZPSyncConn::DebugReceive(const client::CmdRequest &crequest) const {
  // Debug info
  switch (crequest.type()) {
    case client::Type::SET:
      DLOG(INFO) << "SyncConn Receive Set cmd, table="
        << crequest.set().table_name() << " key=" << crequest.set().key();
      break;
    case client::Type::GET:
      DLOG(INFO) << "SyncConn Receive Get cmd, table="
        << crequest.get().table_name() << " key=" << crequest.get().key();
      break;
    case client::Type::DEL:
      DLOG(INFO) << "SyncConn Receive Del cmd, table="
        << crequest.del().table_name() << " key=" << crequest.del().key();
      break;
    case client::Type::SYNC:
      DLOG(INFO) << "SyncConn Receive Sync cmd";
      break;

    default:
      DLOG(INFO) << "Receive Info cmd " << static_cast<int>(crequest.type());
      break;
  }
}

int ZPSyncConn::DealMessage() {
  if (!zp_data_server->Availible()) {
    LOG(WARNING) << "Receive Binlog command, but the server is not availible";
    return -1;
  }

  if (!request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_)) {
    LOG(WARNING) << "Receive Binlog command, but parse error";
    return -1;
  }

  // Check request
  if (request_.epoch() < zp_data_server->meta_epoch()) {
    LOG(WARNING) << "Receive Binlog command with expired epoch:"
      << request_.epoch()
      << ", my current epoch :" << zp_data_server->meta_epoch()
      << ", from: (" << request_.from().ip() << ", "
      << request_.from().port() << ")";
    return -1;
  }

  // do not reply
  set_is_reply(false);

  ZPBinlogReceiveTask *arg = NULL;
  if (request_.sync_type() == client::SyncType::SKIP) {
    // Receive a binlog skip request
    client::BinlogSkip bskip = request_.binlog_skip();

    PartitionSyncOption option(
        request_.sync_type(),
        bskip.table_name(),
        bskip.partition_id(),
        slash::IpPortString(request_.from().ip(), request_.from().port()),
        request_.sync_offset().filenum(),
        request_.sync_offset().offset());

    arg = new ZPBinlogReceiveTask(
        option,
        bskip.gap());

  } else if (request_.sync_type() == client::SyncType::CMD) {
    // Receive a cmd request
    client::CmdRequest crequest = request_.request();
    DebugReceive(crequest);

    Cmd* cmd = zp_data_server->CmdGet(static_cast<int>(crequest.type()));
    if (cmd == NULL) {
      LOG(ERROR) << "unsupported type: " << static_cast<int>(crequest.type());
      return -1;
    }

    std::string table_name = cmd->ExtractTable(&crequest);
    if (!cmd->is_single_paritition()) {
      LOG(ERROR) << "SyncConn shouldn't receive multi partition command: "
        << cmd->name() << ", table=" << table_name
        << " key=" << cmd->ExtractKey(&crequest);
      return -1;
    }
    DLOG(INFO) << "Receive sync cmd: " << cmd->name()
      << ", table=" << table_name
      << " key=" << cmd->ExtractKey(&crequest);

    zp_data_server->PlusStat(StatType::kSync, table_name);

    int partition_id = cmd->ExtractPartition(&crequest);
    if (partition_id < 0) {
      // Do not provice partition_id, calculate it by key
      partition_id = zp_data_server->KeyToPartition(table_name,
          cmd->ExtractKey(&crequest));
    }
    if (partition_id < 0) {
      LOG(ERROR) << "SyncConn can not find partition, table: " << table_name;
      return -1;
    }

    PartitionSyncOption option(
        request_.sync_type(),
        cmd->ExtractTable(&crequest),
        partition_id,
        slash::IpPortString(request_.from().ip(), request_.from().port()),
        request_.sync_offset().filenum(),
        request_.sync_offset().offset());

    // We need to malloc for args need by binglog_bgworker
    // So that it will not be free after the executing of current function
    // Remeber to free these space by the binlog_bgworker at the end of its task
    arg = new ZPBinlogReceiveTask(
        option,
        cmd,
        crequest);
  } else {
    LOG(ERROR) << "Unknow Sync Request Type: "
      << static_cast<int>(request_.sync_type());
    return -1;
  }

  // Dispatch
  zp_data_server->DispatchBinlogBGWorker(arg);

  return 0;
}

////// ZPSyncConnHandle ///// /
void ZPSyncConnHandle::CronHandle() const {
  // Note: ServerCurrentQPS is the sum of client qps and sync qps;
  zp_data_server->ResetLastStat(StatType::kSync);
}
