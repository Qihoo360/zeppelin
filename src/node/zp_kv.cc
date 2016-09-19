#include "zp_kv.h"

#include <glog/logging.h>
#include "zp_data_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPDataServer *zp_data_server;

void InitClientCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
  //Kv
  ////SetCmd
  Cmd* setptr = new SetCmd(kCmdFlagsKv | kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::SET), setptr));
  ////GetCmd
  Cmd* getptr = new GetCmd(kCmdFlagsKv | kCmdFlagsRead);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::GET), getptr));

  // Sync
  Cmd* syncptr = new SyncCmd(kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSuspend);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::SYNC), syncptr));
}

// We use static_cast instead of dynamic_cast, caz we know exactly the Derived class type.
Status SetCmd::Init(google::protobuf::Message *req) {
  client::CmdRequest* request = static_cast<client::CmdRequest*>(req);
  key_ = request->set().key();
  DLOG(INFO) << "SetCmd Init key(" << key_ << ") ok";

  return Status::OK();
}

void SetCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition, bool readonly) {
  client::CmdRequest* request = static_cast<client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  client::CmdResponse_Set* set_res = response->mutable_set();
  response->set_type(client::Type::SET);

  if (readonly) {
    set_res->set_code(client::StatusCode::kError);
    set_res->set_msg("readonly mode");
    result_ = slash::Status::Corruption("readonly mode");
    return;
  }

  LOG(ERROR) << "Test: BGSave";
  //ptr->Bgsave();

  //int32_t ttl;
  nemo::Status s = ptr->db()->Set(request->set().key(), request->set().value());
  //nemo::Status s = zp_data_server->db()->Set(request->set().key(), request->set().value());
  if (!s.ok()) {
    set_res->set_code(client::StatusCode::kError);
    set_res->set_msg(result_.ToString());
    result_ = slash::Status::Corruption(s.ToString());
    LOG(ERROR) << "command failed: Set, caz " << s.ToString();
  } else {
    set_res->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "Set key(" << key_ << ") at Partition: " << ptr->partition_id() << " ok";
    result_ = slash::Status::OK();
  }
}

void GetCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition, bool readonly) {
  client::CmdRequest* request = static_cast<client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  client::CmdResponse_Get* get_res = response->mutable_get();
  response->set_type(client::Type::GET);

  std::string value;
  nemo::Status s = ptr->db()->Get(request->get().key(), &value);
  if (s.ok()) {
    get_res->set_code(client::StatusCode::kOk);
    get_res->set_value(value);
    //result_ = slash::Status::OK();
    DLOG(INFO) << "Get key(" << request->get().key() << ") ok, value is (" << value << ")";
  } else if (s.IsNotFound()) {
    get_res->set_code(client::StatusCode::kNotFound);
    DLOG(INFO) << "Get key(" << request->get().key() << ") not found!";
    //result_ = slash::Status::NotFound();
  } else {
    get_res->set_code(client::StatusCode::kError);
    get_res->set_msg(s.ToString());
    //result_ = slash::Status::Corruption(s.ToString());
    LOG(ERROR) << "command failed: Get, caz " << s.ToString();
  }
}

// Sync between nodes
void SyncCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition, bool readonly) {
  client::CmdRequest* request = static_cast<client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  client::CmdRequest_Sync sync = request->sync();

  slash::Status s;
  Node node(sync.node().ip(), sync.node().port());

  response->set_type(client::Type::SYNC);
  slash::MutexLock l(&(ptr->slave_mutex_));
  if (!ptr->FindSlave(node)) {
    SlaveItem si;
    si.node = node;
    gettimeofday(&si.create_time, NULL);
    si.sender = NULL;

    LOG(INFO) << "SyncCmd Partition:" << ptr->partition_id() << " a new node(" << node.ip << ":" << node.port << ") filenum " << sync.filenum() << ", offset " << sync.offset();
    s = ptr->AddBinlogSender(si, sync.filenum(), sync.offset());

    client::CmdResponse_Sync* sync = response->mutable_sync();
    if (s.ok()) {
      sync->set_code(client::StatusCode::kOk);
      DLOG(INFO) << "SyncCmd add node ok";
      result_ = slash::Status::OK();
    } else if (s.IsIncomplete()) {
      sync->set_code(client::StatusCode::kWait);
      DLOG(INFO) << "SyncCmd add node incomplete";
      result_ = s;
    } else {
      sync->set_code(client::StatusCode::kError);
      sync->set_msg(s.ToString());
      result_ = slash::Status::Corruption(s.ToString());
      LOG(ERROR) << "command failed: Sync, caz " << s.ToString();
    }
  }
}
