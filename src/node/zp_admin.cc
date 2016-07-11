#include "zp_admin.h"

#include <glog/logging.h>
#include "zp_data_server.h"

#include "slash_string.h"
#include "nemo.h"

extern ZPDataServer *zp_data_server;

void InitDataControlCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
  // Sync
  Cmd* syncptr = new SyncCmd(kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(ZPDataControl::DataCmdRequest_TYPE::DataCmdRequest_TYPE_SYNC), syncptr));
}

//Status SyncCmd::Init(const void *buf, size_t count) {
//  return Status::OK();
//}

void SyncCmd::Do(google::protobuf::Message *req, google::protobuf::Message *res) {
  ZPDataControl::DataCmdRequest* request = dynamic_cast<ZPDataControl::DataCmdRequest*>(req);
  ZPDataControl::DataCmdResponse* response = dynamic_cast<ZPDataControl::DataCmdResponse*>(res);
  ZPDataControl::DataCmdRequest_Sync sync = request->sync();

  slash::Status s;
  Node node(sync.node().ip(), sync.node().port());

 response->set_type(ZPDataControl::DataCmdResponse_TYPE::DataCmdResponse_TYPE_SYNC);
 slash::MutexLock l(&(zp_data_server->slave_mutex_));
 if (!zp_data_server->FindSlave(node)) {
   SlaveItem si;
   si.node = node;
   si.sync_fd = fd_;
   gettimeofday(&si.create_time, NULL);
   si.sender = NULL;

   LOG(INFO) << "Sync a new node(" << node.ip << ", " << node.port << ")";
   s = zp_data_server->AddBinlogSender(si, sync.filenum(), sync.offset());

   ZPDataControl::DataCmdResponse_Status* status = response->mutable_status();
   if (!s.ok()) {
     status->set_status(1);
     status->set_msg(result_.ToString());
     result_ = slash::Status::Corruption(s.ToString());
     LOG(ERROR) << "command failed: Sync, caz " << s.ToString();
   } else {
     status->set_status(0);
     DLOG(INFO) << "Sync node ok";
     result_ = slash::Status::OK();
   }
 }
}
