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
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::SET), setptr));
  ////GetCmd
  Cmd* getptr = new GetCmd(kCmdFlagsRead);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::Type::GET), getptr));
}

// We use static_cast instead of dynamic_cast, caz we know exactly the Derived class type.
Status SetCmd::Init(google::protobuf::Message *req) {
  client::CmdRequest* request = static_cast<client::CmdRequest*>(req);
  key_ = request->set().key();

  return Status::OK();
}

void SetCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  client::CmdRequest* request = static_cast<client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);

  client::CmdResponse_Set* set_res = response->mutable_set();
  response->set_type(client::Type::SET);

  //int32_t ttl;
  nemo::Status s = zp_data_server->db()->Set(request->set().key(), request->set().value());
  if (!s.ok()) {
    set_res->set_status(1);
    set_res->set_msg(result_.ToString());
    result_ = slash::Status::Corruption(s.ToString());
    LOG(ERROR) << "command failed: Set, caz " << s.ToString();
  } else {
    set_res->set_status(0);
    DLOG(INFO) << "Set key(" << key_ << ") ok";
    result_ = slash::Status::OK();
  }
}

void GetCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  client::CmdRequest* request = static_cast<client::CmdRequest*>(req);
  client::CmdResponse* response = static_cast<client::CmdResponse*>(res);

  client::CmdResponse_Get* get_res = response->mutable_get();
  response->set_type(client::Type::GET);

  std::string value;
  nemo::Status s = zp_data_server->db()->Get(request->get().key(), &value);
  if (!s.ok()) {
    get_res->set_status(1);
    result_ = slash::Status::Corruption(s.ToString());
    LOG(ERROR) << "command failed: Get, caz " << s.ToString();
  } else {
    get_res->set_status(0);
    get_res->set_value(value);
    result_ = slash::Status::OK();
    DLOG(INFO) << "Get key(" << request->get().key() << ") ok";
  }
}

