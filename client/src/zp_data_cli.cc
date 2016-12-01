/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include <google/protobuf/text_format.h>

#include<iostream>
#include<string>


#include "include/zp_data_cli.h"

namespace libZp {
ZpDataCli::ZpDataCli(const std::string& ip, const int port)
  : data_ip_(ip), data_port_(port) {
}

ZpDataCli::~ZpDataCli() {
}


Status ZpDataCli::Set(const std::string& table, const std::string& key,
    const std::string& value) {
  client::CmdRequest data_cmd = client::CmdRequest();
  data_cmd.set_type(client::Type::SET);
  client::CmdRequest_Set* set_info = data_cmd.mutable_set();
  set_info->set_table_name(table);
  set_info->set_key(key);
  set_info->set_value(value);

  pink::Status ret = Send(&data_cmd);

  client::CmdResponse data_res;
  ret = Recv(&data_res);

  if (!ret.ok()) {
    return Status::IOError(data_res.msg());
  }
  if (data_res.code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::IOError(data_res.msg());
  }
}

Status ZpDataCli::Get(const std::string& table, const std::string& key,
    std::string& value) {
  client::CmdRequest data_cmd = client::CmdRequest();
  data_cmd.set_type(client::Type::GET);
  client::CmdRequest_Get* get_cmd = data_cmd.mutable_get();
  get_cmd->set_table_name(table);
  get_cmd->set_key(key);

  pink::Status ret = Send(&data_cmd);

  if (!ret.ok()) {
    return Status::IOError("fail to send command");
  }

  client::CmdResponse data_res;
  ret = Recv(&data_res);

  /*
  std::cout<< "1" << std::endl;
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(data_res, &text_format);
  std::cout<< text_format << std::endl;
  std::cout<< "2" << std::endl;
 */
  if (!ret.ok()) {
    return Status::IOError(data_res.msg());
  }
  if (data_res.code() == client::StatusCode::kOk) {
    client::CmdResponse_Get info = data_res.get();
    value = info.value();
    return Status::OK();
  } else if (data_res.code() == client::StatusCode::kNotFound) {
    return Status::NotFound("key do not exist");
  } else {
    return Status::IOError(data_res.msg());
  }
}

}  // namespace libZp
