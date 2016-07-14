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
  // Sync
  Cmd* syncptr = new SyncCmd(kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_SYNC), syncptr));
}

void UpdateCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
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
  status->set_code(0);
  result_ = slash::Status::OK();
  DLOG(INFO) << "update ok";
}
//Status SyncCmd::Init(const void *buf, size_t count) {
//  return Status::OK();
//}

void SyncCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  ZPMeta::MetaCmd* request = dynamic_cast<ZPMeta::MetaCmd*>(req);
  ZPMeta::MetaCmdResponse* response = dynamic_cast<ZPMeta::MetaCmdResponse*>(res);
  ZPMeta::MetaCmd_Sync sync = request->sync();

  slash::Status s;
  Node node(sync.node().ip(), sync.node().port());

 response->set_type(ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_SYNC);
 slash::MutexLock l(&(zp_data_server->slave_mutex_));
 if (!zp_data_server->FindSlave(node)) {
   SlaveItem si;
   si.node = node;
   gettimeofday(&si.create_time, NULL);
   si.sender = NULL;

   LOG(INFO) << "Sync a new node(" << node.ip << ":" << node.port << ") filenum " << sync.filenum() << ", offset " << sync.offset();
   s = zp_data_server->AddBinlogSender(si, sync.filenum(), sync.offset());

   ZPMeta::MetaCmdResponse_Status* status = response->mutable_status();
   if (!s.ok()) {
     status->set_code(1);
     status->set_msg(result_.ToString());
     result_ = slash::Status::Corruption(s.ToString());
     LOG(ERROR) << "command failed: Sync, caz " << s.ToString();
   } else {
     status->set_code(0);
     DLOG(INFO) << "Sync node ok";
     result_ = slash::Status::OK();
   }
 }
}
