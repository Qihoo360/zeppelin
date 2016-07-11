#include "zp_kv.h"

#include <glog/logging.h>
#include "zp_data_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPDataServer *zp_data_server;

void InitClientCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
  //Kv
  ////SetCmd
  Cmd* setptr = new SetCmd(kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::OPCODE::SET), setptr));
  ////GetCmd
  Cmd* getptr = new GetCmd(kCmdFlagsRead);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::OPCODE::GET), getptr));
}

Status SetCmd::Init(const void *buf, size_t count) {
  client::Set_Request* request = new client::Set_Request;
  request->ParseFromArray(buf, count);
  key_ = request->key();
  
  request_ = request;
  assert(request_ != NULL);

  DLOG(INFO) << "SetCmd::Init key(" << request->key() << ") ok";

  return Status::OK();
}

void SetCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  client::Set_Request* request = dynamic_cast<client::Set_Request*>(request_);
  client::Set_Response* response = new client::Set_Response;

  //int32_t ttl;
  nemo::Status s = zp_data_server->db()->Set(request->key(), request->value());
  if (!s.ok()) {
    response->set_status(1);
    response->set_msg(result_.ToString());
    result_ = slash::Status::Corruption(s.ToString());
    LOG(ERROR) << "command failed: Set, caz " << s.ToString();
  } else {
    response->set_status(0);
    DLOG(INFO) << "Set key(" << request->key() << ") ok";
    result_ = slash::Status::OK();
  }

  response_ = response;
}

Status GetCmd::Init(const void *buf, size_t count) {
  client::Get_Request* request = new client::Get_Request;
  request->ParseFromArray(buf, count);
  
  request_ = request;
  assert(request_ != NULL);

  return Status::OK();
}

void GetCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  client::Get_Request* request = dynamic_cast<client::Get_Request*>(request_);
  client::Get_Response* response = new client::Get_Response;

  std::string value;
  nemo::Status s = zp_data_server->db()->Get(request->key(), &value);
  if (!s.ok()) {
    response->set_status(1);
    result_ = slash::Status::Corruption(s.ToString());
    LOG(ERROR) << "command failed: Get, caz " << s.ToString();
  } else {
    response->set_status(0);
    response->set_value(value);
    result_ = slash::Status::OK();
    DLOG(INFO) << "Get key(" << request->key() << ") ok";
  }
  
  response_ = response;
}

