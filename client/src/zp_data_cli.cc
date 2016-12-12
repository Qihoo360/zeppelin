/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include <google/protobuf/text_format.h>

#include<iostream>
#include<string>


#include "include/zp_data_cli.h"

namespace libzp {
ZpDataCli::ZpDataCli() {
  data_cmd_ = client::CmdRequest();
  data_res_ = client::CmdResponse();
}

ZpDataCli::~ZpDataCli() {
}


Status ZpDataCli::Set(const std::string& table, const std::string& key,
    const std::string& value) {
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::SET);
  client::CmdRequest_Set* set_info = data_cmd_.mutable_set();
  set_info->set_table_name(table);
  set_info->set_key(key);
  set_info->set_value(value);

  pink::Status ret = Send(&data_cmd_);

  std::cout<< "sent" << std::endl;
  if (!ret.ok()) {
    return ret;
  }

  std::cout<< "recving..." << std::endl;
  ret = Recv(&data_res_);
  std::cout<< "recved" << std::endl;

  if (!ret.ok()) {
    return Status::IOError(data_res_.msg());
  }
  if (data_res_.code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::NotSupported(data_res_.msg());
  }
}

Status ZpDataCli::Get(const std::string& table, const std::string& key,
    std::string* value) {
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::GET);
  client::CmdRequest_Get* get_cmd = data_cmd_.mutable_get();
  get_cmd->set_table_name(table);
  get_cmd->set_key(key);

  pink::Status ret = Send(&data_cmd_);

  if (!ret.ok()) {
    return Status::IOError("fail to send command");
  }

  ret = Recv(&data_res_);

  /*
  std::cout<< "1" << std::endl;
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(data_res, &text_format);
  std::cout<< text_format << std::endl;
  std::cout<< "2" << std::endl;
 */
  if (!ret.ok()) {
    return Status::IOError(data_res_.msg());
  }
  if (data_res_.code() == client::StatusCode::kOk) {
    client::CmdResponse_Get info = data_res_.get();
    value->assign(info.value().data(), info.value().size());
    return Status::OK();
  } else if (data_res_.code() == client::StatusCode::kNotFound) {
    return Status::NotFound("key do not exist");
  } else {
    return Status::IOError(data_res_.msg());
  }
}

}  // namespace libzp
