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
    const ZPMeta::Node& master = partition.master();
    LOG(INFO) << "receive Update message, master ip: " << master.ip() << " master port: " << master.port();
    if (master.ip() != zp_data_server->local_ip() || master.port() != zp_data_server->local_port()) {
      zp_data_server->BecomeSlave(master.ip(), master.port());
    } else {
      zp_data_server->BecomeMaster();
    }
  }

  response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_UPDATE);
  ZPMeta::MetaCmdResponse_Status* status = response->mutable_status();
  status->set_code(ZPMeta::StatusCode::kOk);
  result_ = slash::Status::OK();
  DLOG(INFO) << "update ok";
}
//Status SyncCmd::Init(const void *buf, size_t count) {
//  return Status::OK();
//}

