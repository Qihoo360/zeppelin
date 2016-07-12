#include "zp_admin.h"

#include <glog/logging.h>
#include "zp_data_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPDataServer *zp_data_server;

void InitMetaCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
  // Sync
  Cmd* syncptr = new SyncCmd(kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_SYNC), syncptr));
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

   LOG(INFO) << "Sync a new node(" << node.ip << ", " << node.port << ")";
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
