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
#include "src/node/zp_data_command.h"

#include <glog/logging.h>
#include <memory>
#include <vector>
#include <unordered_map>
#include "slash/include/slash_string.h"

#include "include/db_nemo.h"
#include "src/node/zp_data_server.h"

extern ZPDataServer *zp_data_server;

void SetCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request =
    static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  // User raw pointer instead of shared_ptr,
  // since it is surely be existed during this caller
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  response->set_type(client::Type::SET);

  rocksdb::Status s;
  if (request->set().has_expire()) {
    int base = 0, ttl = request->set().expire().ttl();
    if (request->set().expire().has_base()) {
      // Come from sync conn
      base = request->set().expire().base();
      ttl -= (time(NULL) - base);
      if (ttl <= 0) {
        // Already expire
        DLOG(INFO) << "Set key(" << request->set().key() << ") at "
          << ptr->table_name() << "_"
          << ptr->partition_id() << " already expired";
        response->set_code(client::StatusCode::kOk);
        return;
      }
    }
    s = ptr->db()->Put(rocksdb::WriteOptions(),
        request->set().key(),
        request->set().value(),
        ttl);
  } else {
    s = ptr->db()->Put(rocksdb::WriteOptions(),
        request->set().key(),
        request->set().value());
  }

  if (!s.ok()) {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(WARNING) << "command failed: Set key(" << request->set().key()
      << ") at " << ptr->table_name() << "_" << ptr->partition_id()
      << ", caz:" << s.ToString();
  } else {
    response->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "Set key(" << request->set().key() << ") at "
      << ptr->table_name() << "_" << ptr->partition_id() << " ok";
  }
}

bool SetCmd::GenerateLog(const google::protobuf::Message *req,
    std::string* log_raw) const {
  const client::CmdRequest* request =
    static_cast<const client::CmdRequest*>(req);
  if (request->set().has_expire()) {
    client::CmdRequest log_req(*request);
    log_req.mutable_set()->mutable_expire()->set_base(time(NULL));
    return log_req.SerializeToString(log_raw);
  }
  return request->SerializeToString(log_raw);
}

void GetCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request =
    static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  client::CmdResponse_Get* get_res = response->mutable_get();
  response->set_type(client::Type::GET);

  std::string value;
  rocksdb::Status s = ptr->db()->Get(rocksdb::ReadOptions(),
      request->get().key(),
      &value);
  if (s.ok()) {
    response->set_code(client::StatusCode::kOk);
    get_res->set_value(value);
    DLOG(INFO) << "Get key(" << request->get().key()
      << ") at " << ptr->table_name() << "_" << ptr->partition_id()
      << " ok, value is (" << value << ")";
  } else if (s.IsNotFound()) {
    response->set_code(client::StatusCode::kNotFound);
    DLOG(INFO) << "Get key(" << request->get().key()
      << ") at " << ptr->table_name() << "_"
      << ptr->partition_id() << " not found!";
  } else {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(WARNING) << "command failed: Get key("
      << request->get().key() << ") at "
      << ptr->table_name() << "_"
      << ptr->partition_id()
      << ", caz " << s.ToString();
  }
}

void DelCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request =
    static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  response->set_type(client::Type::DEL);

  rocksdb::Status s = ptr->db()->Delete(rocksdb::WriteOptions(),
      request->del().key());
  if (!s.ok()) {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(WARNING) << "command failed: Del key(" << request->del().key()
      << ") at " << ptr->table_name() << "_" << ptr->partition_id()
      << ", caz:" << s.ToString();
  } else {
    response->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "Del key(" << request->del().key()
      << ") at Partition: " << ptr->partition_id() << " ok";
  }
}

void MgetCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* ptr) const {
  const client::CmdRequest* request =
    static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  response->Clear();
  response->set_type(client::Type::MGET);

  // One error all error
  Cmd* sub_cmd = zp_data_server->CmdGet(client::Type::GET);
  client::CmdRequest sub_req;
  sub_req.set_type(client::Type::GET);
  client::CmdResponse sub_res;
  sub_res.set_type(client::Type::GET);
  for (auto& key : request->mget().keys()) {
    std::shared_ptr<Partition> partition = zp_data_server->GetTablePartition(
        request->mget().table_name(), key);
    if (partition == NULL) {
      LOG(WARNING) << "command failed: Mget, no partition for key:" << key;
      response->set_code(client::StatusCode::kError);
      response->set_msg("no partition" + key);
      return;
    }

    // convert to multi sub command, then execute
    sub_req.Clear();
    sub_res.Clear();
    client::CmdRequest_Get* get = sub_req.mutable_get();
    get->set_table_name(request->mget().table_name());
    get->set_key(key);
    partition->DoCommand(sub_cmd, sub_req, &sub_res);
    if (sub_res.code() != client::StatusCode::kOk
        && sub_res.code() != client::StatusCode::kNotFound) {
      response->set_code(sub_res.code());
      response->set_msg(sub_res.msg());
      return;
    }
    client::CmdResponse_Mget* mget = response->add_mget();
    mget->set_key(key);
    if (sub_res.code() == client::StatusCode::kOk) {
      mget->set_value(sub_res.get().value());
    } else {
      mget->set_value("");
    }
  }
  response->set_code(client::StatusCode::kOk);
}

void InfoCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* p) const {
  const client::CmdRequest* request =
    static_cast<const client::CmdRequest*>(req);
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
      zp_data_server->GetTableStat(StatType::kClient, table_name, &stats);
      DLOG(INFO) << "InfoStat with " << stats.size() << " tables total";

      for (auto it = stats.begin(); it != stats.end(); it++) {
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
      zp_data_server->GetTableCapacity(table_name, &stats);
      DLOG(INFO) << "InfoCapacity with " << stats.size() << " tables total";

      for (auto it = stats.begin(); it != stats.end(); it++) {
        client::CmdResponse_InfoCapacity* info_cpct =
          response->add_info_capacity();
        info_cpct->set_table_name(it->table_name);
        info_cpct->set_used(it->used_disk);
        info_cpct->set_remain(it->free_disk);
      }
      break;
    }
    case client::Type::INFOREPL: {
      response->set_type(client::Type::INFOREPL);
      std::unordered_map<std::string, client::CmdResponse_InfoRepl> info_repls;
      if (!zp_data_server->GetTableReplInfo(table_name, &info_repls)) {
        response->set_code(client::StatusCode::kError);
        response->set_msg("unknown table name");
        LOG(WARNING) << "Failed to GetTableReplInfo: " << table_name;
        return;
      }

      for (auto& info_repl : info_repls) {
        response->add_info_repl()->CopyFrom(info_repl.second);
      }
      break;
    }
    case client::Type::INFOSERVER: {
      response->set_type(client::Type::INFOSERVER);
      client::CmdResponse_InfoServer info_server;
      if (!zp_data_server->GetServerInfo(&info_server)) {
        LOG(WARNING) << "Failed to GetTableReplInfo: " << table_name;
      }
      response->mutable_info_server()->CopyFrom(info_server);
      break;
    }
    default: {
      response->set_code(client::StatusCode::kError);
      response->set_msg("unsupported cmd type");
      LOG(WARNING) << "unsupported cmd type"
        << static_cast<int>(request->type());
      return;
    }
  }

  response->set_code(client::StatusCode::kOk);
}

// Sync between nodes
void SyncCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request =
    static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  client::CmdRequest_Sync sync_req = request->sync();

  slash::Status s;
  Node node(sync_req.node().ip(), sync_req.node().port());
  response->set_type(client::Type::SYNC);

  BinlogOffset s_boffset(sync_req.sync_offset().filenum(),
      sync_req.sync_offset().offset());
  LOG(INFO) << "SyncCmd with a new node ("
    << ptr->table_name() << "_"  << ptr->partition_id()
    << "_" << node.ip << ":" << node.port << ", "
    << s_boffset.filenum << ", " << s_boffset.offset << ")";
  s = ptr->SlaveAskSync(node, s_boffset);

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
      ptr->GetWinBinlogOffset(&s_boffset);
      DLOG(INFO) << "SyncCmd with offset larger than me, node:"
        << sync_req.node().ip() << ":" << sync_req.node().port();
    } else {
      // Invalid filenum an offset, sen fallback offset
      DLOG(INFO) << "SyncCmd with offset invalid, node:"
        << sync_req.node().ip() << ":" << sync_req.node().port();
    }
    offset->set_filenum(s_boffset.filenum);
    s_boffset.offset= BinlogBlockStart(s_boffset.offset);
    offset->set_offset(s_boffset.offset);
    DLOG(INFO) << "Send back fallback binlog point: "
      << s_boffset.filenum << ", " << s_boffset.offset << " To: "
      << sync_req.node().ip() << ":" << sync_req.node().port();
  } else if (s.IsIncomplete()) {
    // Slave should wait for db sync
    response->set_code(client::StatusCode::kWait);
    DLOG(INFO) << "SyncCmd add node incomplete";
  } else {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(WARNING) << "command failed: Sync, caz " << s.ToString();
  }
}

void FlushDBCmd::Do(const google::protobuf::Message *req,
    google::protobuf::Message *res, void* partition) const {
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->Clear();
  response->set_type(client::Type::FLUSHDB);
  Status s = ptr->FlushDb();
  if (!s.ok()) {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(WARNING) << "command failed: FlushDB at "
      << ptr->table_name() << "_" << ptr->partition_id()
      << ", caz:" << s.ToString();
  } else {
    response->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "FlushDB at "
      << ptr->table_name() << "_" << ptr->partition_id() << " ok";
  }
}
