#include "src/node/zp_sync_conn.h"

#include <glog/logging.h>
#include "src/node/zp_data_server.h"
#include "src/node/zp_data_partition.h"

extern ZPDataServer* zp_data_server;

ZPSyncConn::ZPSyncConn(int fd, std::string ip_port, pink::Thread* thread) :
  PbConn(fd, ip_port, thread) {
}

ZPSyncConn::~ZPSyncConn() {
}

void ZPSyncConn::DebugReceive(const client::CmdRequest &crequest) const {
  // Debug info
  switch (crequest.type()) {
    case client::Type::SET:
      DLOG(INFO) << "SyncConn Receive Set cmd, table=" << crequest.set().table_name() << " key=" << crequest.set().key();
      break;
    case client::Type::GET:
      DLOG(INFO) << "SyncConn Receive Get cmd, table=" << crequest.get().table_name() << " key=" << crequest.get().key();
      break;
    case client::Type::DEL:
      DLOG(INFO) << "SyncConn Receive Del cmd, table=" << crequest.del().table_name() << " key=" << crequest.del().key();
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
    LOG(WARNING) << "Receive Binlog command, but the server is not availible yet";
    return -1;
  }

  if (!request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_)) {
    LOG(WARNING) << "Receive Binlog command, but parse error";
    return -1;
  }
  
  // Check request
  if (request_.epoch() < zp_data_server->meta_epoch()) {
    LOG(WARNING) << "Receive Binlog command with expired epoch:" << request_.epoch()
      << ", my current epoch :" << zp_data_server->meta_epoch()
      << ", from: (" << request_.from().ip() << ", " << request_.from().port() << ")";
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
      LOG(ERROR) << "unsupported type: " << (int)crequest.type();
      return -1;
    }

    DLOG(INFO) << "Receive sync cmd: " << cmd->name()
      << ", table=" << cmd->ExtractTable(&crequest)
      << " key=" << cmd->ExtractKey(&crequest);

    std::string table_name = cmd->ExtractTable(&crequest);
    //self_thread_->PlusStat(table_name);
    
    int partition_id = zp_data_server->KeyToPartition(table_name, cmd->ExtractKey(&crequest));
    if (partition_id < 0) {
      LOG(ERROR) << "SyncConn Receive unknow table: " << table_name;
      return -1;
    }

    PartitionSyncOption option(
        request_.sync_type(),
        cmd->ExtractTable(&crequest),
        ((cmd->ExtractPartition(&crequest) >= 0 )
         ? cmd->ExtractPartition(&crequest) : partition_id),
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
    LOG(ERROR) << "Unknow Sync Request Type: " << static_cast<int>(request_.sync_type());
    return -1;
  }

  // Dispatch
  zp_data_server->DispatchBinlogBGWorker(arg);

  return 0;
}
