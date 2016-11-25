#include "zp_meta_cli.h"
namespace libZp {
ZpMetaCli::ZpMetaCli(const std::string& ip, const int port)
  : meta_ip_(ip), meta_port_(port) {
}

ZpMetaCli::~ZpMetaCli() {
}


Status ZpMetaCli::ResetClusterMap(ZPMeta::MetaCmdResponse_Pull& pull, ClusterMap& cluster_map) {
  cluster_map.table_num = pull.info_size();
  cluster_map.table_maps.clear();
  for (int i = 0; i < pull.info_size(); i++) {
    cluster_map.table_maps.emplace_back(pull.info(i));
  }
  return Status::OK();
}

Status ZpMetaCli::Pull(ClusterMap& cluster_map) {
  ::ZPMeta::MetaCmd meta_cmd = ::ZPMeta::MetaCmd();
  meta_cmd.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_PULL);
  pink::Status ret = Send(&meta_cmd);

  ::ZPMeta::MetaCmdResponse meta_res;
  ret = Recv(&meta_res);

  if (!ret.ok()) {
    return Status::IOError(meta_res.status().msg());
  }
  if (meta_res.status().code() != ZPMeta::StatusCode::kOk) {
    return Status::IOError(meta_res.status().msg());
  }

  ZPMeta::MetaCmdResponse_Pull info = meta_res.pull();
  int64_t new_epoch = info.version();
  if (new_epoch <= cluster_map.epoch) {
    return Status::OK();
  }

  // update clustermap now
  return ResetClusterMap(info, cluster_map);
}

Status ZpMetaCli::CreateTable(std::string& table_name, int partition_num) {
  ::ZPMeta::MetaCmd meta_cmd = ::ZPMeta::MetaCmd();
  meta_cmd.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_INIT);
  ZPMeta::MetaCmd_Init* init = meta_cmd.mutable_init();
  init->set_name(table_name);
  init->set_num(partition_num);
  pink::Status ret = Send(&meta_cmd);

  ::ZPMeta::MetaCmdResponse meta_res;
  ret = Recv(&meta_res);

  if (ret.ok()) {
    return pink::Status::IOError(meta_res.status().msg());
  }
  if (meta_res.status().code() != ZPMeta::StatusCode::kOk) {
    return Status::IOError(meta_res.status().msg());
  } else {
    return Status::OK();
  }

}

}
