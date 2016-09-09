#include "zp_admin.h"

#include <glog/logging.h>
#include "zp_data_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPDataServer *zp_data_server;

void InitMetaCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
  // Update
  Cmd* updateptr = new UpdateCmd(kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_UPDATE), updateptr));
}

void UpdateCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res, bool readonly) {
  ZPMeta::MetaCmd* request = dynamic_cast<ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = dynamic_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmd_Update update = request->update();

  for (int i = 0; i < update.info_size(); i++) {
    const ZPMeta::Partitions& partition = update.info(i);
    DLOG(INFO) << "receive Update message, master is " << partition.master().ip() << ":" << partition.master().port();

    std::vector<Node> nodes;
    nodes.push_back(Node(partition.master().ip(), partition.master().port()));
    for (int j = 0; j < partition.slaves_size(); j++) {
      nodes.push_back(Node(partition.slaves(j).ip(), partition.slaves(j).port()));
    }

    bool result = zp_data_server->UpdateOrAddPartition(partition.id(), nodes);
    if (!result) {
      LOG(WARNING) << "AddPartition failed";
    }

    //const ZPMeta::Node& master = partition.master();

    //if ((master.ip() != zp_data_server->local_ip() || master.port() != zp_data_server->local_port())         // I'm not the told master
    //    && (master.ip() != zp_data_server->master_ip() || master.port() != zp_data_server->master_port())) { // and there's a new master
    //  zp_data_server->BecomeSlave(master.ip(), master.port());
    //}
    //if ((master.ip() == zp_data_server->local_ip() && master.port() && zp_data_server->local_port())         // I'm the told master and
    //    && !zp_data_server->is_master()) { 
    //  zp_data_server->BecomeMaster();
    //}
  }

  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_UPDATE);
  ZPMeta::MetaCmdResponse_Status* status = response->mutable_status();
  status->set_code(ZPMeta::StatusCode::kOk);
  result_ = slash::Status::OK();
  DLOG(INFO) << "Update ok";
}
//Status SyncCmd::Init(const void *buf, size_t count) {
//  return Status::OK();
//}

