#include "zp_data_command.h"

#include <google/protobuf/text_format.h>
#include <glog/logging.h>
#include "zp_data_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPDataServer *zp_data_server;

void SetCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  response->set_type(client::Type::SET);

  nemo::Status s = ptr->db()->Set(request->set().key(),
      request->set().value());
  if (!s.ok()) {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(ERROR) << "command failed: Set, caz " << s.ToString();
  } else {
    response->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "Set key(" << request->set().key() << ") at "
      << ptr->table_name() << "_" << ptr->partition_id() << " ok";
  }
}

void GetCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  client::CmdResponse_Get* get_res = response->mutable_get();
  response->set_type(client::Type::GET);

  std::string value;
  nemo::Status s = ptr->db()->Get(request->get().key(), &value);
  if (s.ok()) {
    response->set_code(client::StatusCode::kOk);
    get_res->set_value(value);
    DLOG(INFO) << "Get key(" << request->get().key()
      << ") at " << ptr->table_name() << "_" << ptr->partition_id()
      << " ok, value is (" << value << ")";
  } else if (s.IsNotFound()) {
    response->set_code(client::StatusCode::kNotFound);
    DLOG(INFO) << "Get key(" << request->get().key() <<
      ") at " << ptr->table_name() << "_" << ptr->partition_id() << " not found!";
  } else {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(ERROR) << "command failed: Get at "
      << ptr->table_name() << "_"  << ptr->partition_id()
      << ", caz " << s.ToString();
  }
}

void DelCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  response->set_type(client::Type::DEL);

  int64_t count;
  nemo::Status s = ptr->db()->Del(request->del().key(), &count);
  if (!s.ok()) {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(ERROR) << "command failed: Del, caz " << s.ToString();
  } else {
    response->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "Del key(" << request->del().key()
      << ") at Partition: " << ptr->partition_id() << " ok";
  }
}

void InfoCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* p) const {
  const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);

  response->Clear();
  std::string table_name;
  if (request->has_info() && request->info().has_table_name()) {
    table_name = request->info().table_name();
  }

  switch (request->type()) {
    case client::Type::INFOSTATS: {
      response->set_type(client::Type::INFOSTATS);

      std::vector<Statistic> stats;
      zp_data_server->GetTableStat(table_name, stats);
      DLOG(INFO) << "InfoStat with " << stats.size() << " tables total";

      for (auto it = stats.begin(); it != stats.end(); it++) {
        // TODO anan debug;
        //DLOG(INFO) << "InfoStats ---->";
        //it->Dump();
        client::CmdResponse_InfoStats* info_stat = response->add_info_stats();
        info_stat->set_table_name(it->table_name);
        info_stat->set_total_querys(it->querys);
        info_stat->set_qps(it->last_qps);
      }
      break;
    }
    case client::Type::INFOCAPACITY: {
      response->set_type(client::Type::INFOCAPACITY);
      std::vector<Statistic> stats;
      zp_data_server->GetTableCapacity(table_name, stats);
      DLOG(INFO) << "InfoCapacity with " << stats.size() << " tables total";

      for (auto it = stats.begin(); it != stats.end(); it++) {
        // TODO anan debug;
        //DLOG(INFO) << "InfoCapacity ---->";
        //it->Dump();
        client::CmdResponse_InfoCapacity* info_cpct = response->add_info_capacity();
        info_cpct->set_table_name(it->table_name);
        info_cpct->set_used(it->used_disk);
        info_cpct->set_remain(it->free_disk);
      }
      break;
    }
    case client::Type::INFOPARTITION: {
      response->set_type(client::Type::INFOPARTITION);
      std::unordered_map<std::string, std::vector<PartitionBinlogOffset>> table_offsets;
      zp_data_server->DumpTableBinlogOffsets(table_name, table_offsets);
      DLOG(INFO) << "InfoPartition with " << table_offsets.size() << " tables in total.";

      for (auto& item : table_offsets) {
        client::CmdResponse_InfoPartition* info_part = response->add_info_partition();
        info_part->set_table_name(item.first);
        for(auto& p : item.second) {
          client::SyncOffset* offset = info_part->add_sync_offset();
          offset->set_partition(p.partition_id);
          offset->set_filenum(p.filenum);
          offset->set_offset(p.offset);
        }
      }
      break;
    }
    default: {
      response->set_code(client::StatusCode::kError);
      response->set_msg("unsupported cmd type");
      LOG(ERROR) << "unsupported cmd type" << static_cast<int>(request->type()); 
      return;
    }
  }

  response->set_code(client::StatusCode::kOk);

  // TODO anan debug;
  //std::string text_format;
  //google::protobuf::TextFormat::PrintToString(*response, &text_format);
  //DLOG(INFO) << "InfoCmd text_format : [" << text_format << "]";
}


// Sync between nodes
void SyncCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  client::CmdRequest_Sync sync_req = request->sync();

  slash::Status s;
  Node node(sync_req.node().ip(), sync_req.node().port());
  response->set_type(client::Type::SYNC);

  uint32_t s_filenum = sync_req.sync_offset().filenum();
  uint64_t s_offset = sync_req.sync_offset().offset();
  LOG(INFO) << "SyncCmd with a new node ("
    << ptr->table_name() << "_"  << ptr->partition_id()
    << "_" << node.ip << ":" << node.port << ", "
    << s_filenum << ", " << s_offset << ")";
  s = ptr->SlaveAskSync(node, s_filenum, s_offset);

  if (s.ok()) {
    response->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "SyncCmd add node ok";
  } else if (s.IsEndFile() || s.IsInvalidArgument()) {
    // Need send fallback offset
    response->set_code(client::StatusCode::kFallback);
    client::CmdResponse_Sync *sync_res = response->mutable_sync();
    sync_res->set_table_name(sync_req.table_name());
    client::SyncOffset *offset = sync_res->mutable_sync_offset();
    if (s.IsEndFile()) {
      // Peer's offset is larger than me, send fallback offset
      ptr->GetWinBinlogOffset(&s_filenum, &s_offset);
      DLOG(INFO) << "SyncCmd with offset larger than me, node:"
        << sync_req.node().ip() << ":" << sync_req.node().port();
    } else {
      // Invalid filenum an offset, sen fallback offset
      DLOG(INFO) << "SyncCmd with offset invalid, node:"
        << sync_req.node().ip() << ":" << sync_req.node().port();
    }
    offset->set_filenum(s_filenum);
    s_offset = BinlogBlockStart(s_offset);
    offset->set_offset(s_offset);
    DLOG(INFO) << "Send back fallback binlog point: "
      << s_filenum << ", " << s_offset << " To: "
      << sync_req.node().ip() << ":" << sync_req.node().port();
  } else if (s.IsIncomplete()) {
    // Slave should wait for db sync
    response->set_code(client::StatusCode::kWait);
    DLOG(INFO) << "SyncCmd add node incomplete";
  } else {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(ERROR) << "command failed: Sync, caz " << s.ToString();
  }
}
