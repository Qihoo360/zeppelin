#include "zp_data_command.h"

#include <glog/logging.h>
#include "zp_data_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPDataServer *zp_data_server;

void SetCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  response->set_type(client::Type::SET);

  nemo::Status s = ptr->db()->Set(request->set().key(), request->set().value());
  if (!s.ok()) {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(ERROR) << "command failed: Set, caz " << s.ToString();
  } else {
    response->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "Set key(" << request->set().key() << ") at Partition: " << ptr->partition_id() << " ok";
  }
}

void GetCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  client::CmdResponse_Get* get_res = response->mutable_get();
  response->set_type(client::Type::GET);

  std::string value;
  nemo::Status s = ptr->db()->Get(request->get().key(), &value);
  if (s.ok()) {
    response->set_code(client::StatusCode::kOk);
    get_res->set_value(value);
    DLOG(INFO) << "Get key(" << request->get().key() << ") at Partition " << ptr->partition_id() << " ok, value is (" << value << ")";
  } else if (s.IsNotFound()) {
    response->set_code(client::StatusCode::kNotFound);
    DLOG(INFO) << "Get key(" << request->get().key() << ") at Partition " << ptr->partition_id() << " not found!";
  } else {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(ERROR) << "command failed: Get at Partition " << ptr->partition_id() << ", caz " << s.ToString();
  }
}

// Sync between nodes
void SyncCmd::Do(const google::protobuf::Message *req, google::protobuf::Message *res, void* partition) const {
  const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);
  Partition* ptr = static_cast<Partition*>(partition);

  client::CmdRequest_Sync sync_req = request->sync();

  slash::Status s;
  Node node(sync_req.node().ip(), sync_req.node().port());

  response->set_type(client::Type::SYNC);

  LOG(INFO) << "SyncCmd Partition:" << ptr->partition_id()
    << " a new node(" << node.ip << ":" << node.port << ") filenum " << sync_req.filenum() << ", offset " << sync_req.offset();
  s = ptr->AddBinlogSender(node, sync_req.filenum(), sync_req.offset());

  if (s.ok()) {
    response->set_code(client::StatusCode::kOk);
    DLOG(INFO) << "SyncCmd add node ok";
  } else if (s.IsIncomplete()) {
    response->set_code(client::StatusCode::kWait);
    DLOG(INFO) << "SyncCmd add node incomplete";
  } else {
    response->set_code(client::StatusCode::kError);
    response->set_msg(s.ToString());
    LOG(ERROR) << "command failed: Sync, caz " << s.ToString();
  }
}
