#include "zp_kv.h"

#include <glog/logging.h>
#include "zp_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPServer *zp_server;

Status SetCmd::Init(const void *buf, size_t count) {
  client::Set_Request* request = new client::Set_Request;
  request->ParseFromArray(buf, count);
  key_ = request->key();
  
  request_ = request;
  assert(request_ != NULL);

  DLOG(INFO) << "SetCmd::Init key(" << request->key() << ") ok";

  return Status::OK();
}

void SetCmd::Do() {
  client::Set_Request* request = dynamic_cast<client::Set_Request*>(request_);
  client::Set_Response* response = new client::Set_Response;

  //int32_t ttl;
  nemo::Status s = zp_server->db()->Set(request->key(), request->value());
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

void GetCmd::Do() {
  client::Get_Request* request = dynamic_cast<client::Get_Request*>(request_);
  client::Get_Response* response = new client::Get_Response;

  std::string value;
  nemo::Status s = zp_server->db()->Get(request->key(), &value);
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

