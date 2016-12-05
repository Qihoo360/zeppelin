/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include <google/protobuf/text_format.h>

#include<iostream>
#include<string>

#include "include/zp_meta_cli.h"

namespace libZp {
ZpMetaCli::ZpMetaCli(const std::string& ip, const int port)
  : meta_ip_(ip), meta_port_(port) {
}

ZpMetaCli::~ZpMetaCli() {
}

Status ZpMetaCli::ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull,
    ClusterMap& cluster_map) {
  cluster_map.epoch = pull.version();
  for (int i = 0; i < pull.info_size(); i++) {
    std::cout << "reset table:" << pull.info(i).name() << std::endl;
    auto it = cluster_map.table_maps.find(pull.info(i).name());
    if (it != cluster_map.table_maps.end()) {
      cluster_map.table_maps.erase(it);
    }
    cluster_map.table_maps.emplace(pull.info(i).name(), pull.info(i));
  }
  cluster_map.table_num = cluster_map.table_maps.size();
  std::cout<< "pull done" <<  std::endl;
  return Status::OK();
}

Status ZpMetaCli::Pull(ClusterMap& cluster_map, const std::string& table) {
  ZPMeta::MetaCmd meta_cmd = ZPMeta::MetaCmd();
  meta_cmd.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_PULL);
  ZPMeta::MetaCmd_Pull* pull = meta_cmd.mutable_pull();
  pull->set_name(table);
  pink::Status ret = Send(&meta_cmd);

  ZPMeta::MetaCmdResponse meta_res;
  ret = Recv(&meta_res);

  if (!ret.ok()) {
    return Status::IOError(meta_res.status().msg());
  }
  if (meta_res.status().code() != ZPMeta::StatusCode::kOk) {
    return Status::IOError(meta_res.status().msg());
  }

  ZPMeta::MetaCmdResponse_Pull info = meta_res.pull();

  // update clustermap now
  return ResetClusterMap(info, cluster_map);
}

Status ZpMetaCli::CreateTable(const std::string& table_name,
    const int partition_num) {
  ZPMeta::MetaCmd meta_cmd = ZPMeta::MetaCmd();
  meta_cmd.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_INIT);
  ZPMeta::MetaCmd_Init* init = meta_cmd.mutable_init();
  init->set_name(table_name);
  init->set_num(partition_num);
  pink::Status ret = Send(&meta_cmd);

  ZPMeta::MetaCmdResponse meta_res;
  ret = Recv(&meta_res);

  if (!ret.ok()) {
    return pink::Status::IOError(meta_res.status().msg());
  }
  if (meta_res.status().code() != ZPMeta::StatusCode::kOk) {
    return Status::IOError(meta_res.status().msg());
  } else {
    return Status::OK();
  }
}

}  // namespace libZp
