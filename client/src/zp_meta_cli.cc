/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include <google/protobuf/text_format.h>

#include<iostream>
#include<string>

#include "include/zp_meta_cli.h"

namespace libzp {
ZpMetaCli::ZpMetaCli() {
}

ZpMetaCli::~ZpMetaCli() {
}

Status ZpMetaCli::ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull,
    std::unordered_map<std::string, Table*>* tables_, int64_t* epoch) {
  *epoch = pull.version();
  std::cout << "get " << pull.info_size() << " table info" << std::endl;
  for (int i = 0; i < pull.info_size(); i++) {
    std::cout << "reset table:" << pull.info(i).name() << std::endl;
    auto it = tables_->find(pull.info(i).name());
    if (it != tables_->end()) {
      delete it->second;
      tables_->erase(it);
    }
    Table* new_table = new Table(pull.info(i));
    std::string table_name = pull.info(i).name();
    tables_->insert(std::make_pair(pull.info(i).name(), new_table));
  }
  std::cout<< "pull done" <<  std::endl;
  return Status::OK();
}

Status ZpMetaCli::Pull(const std::string& table,
    std::unordered_map<std::string, Table*>* tables_, int64_t* epoch) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::PULL);
  ZPMeta::MetaCmd_Pull* pull = meta_cmd_.mutable_pull();
  pull->set_name(table);
  pink::Status ret = Send(&meta_cmd_);
  if (!ret.ok()) {
    return ret;
  }

  meta_res_.Clear();
  ret = Recv(&meta_res_);
  /*
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(meta_res_, &text_format);
  std::cout<< "text:" << text_format << std::endl;
  std::cout<< "text done:" << text_format << std::endl;
  */

  if (!ret.ok()) {
    return Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  }

  ZPMeta::MetaCmdResponse_Pull info = meta_res_.pull();

  // update clustermap now
  return ResetClusterMap(info, tables_, epoch);
}

Status ZpMetaCli::CreateTable(const std::string& table_name,
    const int partition_num) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::INIT);
  ZPMeta::MetaCmd_Init* init = meta_cmd_.mutable_init();
  init->set_name(table_name);
  init->set_num(partition_num);
  // @TODO all use slash status
  pink::Status ret = Send(&meta_cmd_);

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  // @TODO all pb_cli add timeout
  ret = Recv(&meta_res_);

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res_.msg());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::NotSupported(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

}  // namespace libzp
