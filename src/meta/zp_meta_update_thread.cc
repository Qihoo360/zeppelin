#include <google/protobuf/text_format.h>
#include "zp_const.h"
#include "zp_meta_update_thread.h"
#include "zp_meta.pb.h"
#include "zp_meta_server.h"

extern ZPMetaServer* zp_meta_server;

ZPMetaUpdateThread::ZPMetaUpdateThread() {
}

void ZPMetaUpdateThread::DoMetaUpdate(void *p) {
  ZPMetaUpdateThread::ZPMetaUpdateArgs *args = static_cast<ZPMetaUpdateThread::ZPMetaUpdateArgs*>(p);
  ZPMetaUpdateThread *thread = args->thread;
  thread->MetaUpdate(args->ip, args->port, args->op);

  delete args;
}

void ZPMetaUpdateThread::DoMetaBroadcast(void *p) {
  ZPMetaUpdateThread *thread = static_cast<ZPMetaUpdateThread*>(p);
  thread->MetaBroadcast();
}

slash::Status ZPMetaUpdateThread::MetaUpdate(const std::string ip, int port, ZPMetaUpdateOP op) {

  slash::Status s;

  // Change Clients
  s = UpdateSender(ip, port + 100, op);
  if (!s.ok()) {
    LOG(ERROR) << "update thread update sender failed. " << s.ToString();
  }
  LOG(INFO) << "update sender finished";

  ZPMeta::MetaCmd_Update ms_info;

  if (ZPMetaUpdateOP::OP_ADD == op) {
    if (zp_meta_server->PNums() == 0) {
      return s;
    }

    s = zp_meta_server->GetMSInfo(ms_info);
    if (!s.ok()) {
      LOG(ERROR) << "UpdateThread: GetMSInfo error, " << s.ToString();
      return s;
    }

    // Send Update Message
    SendUpdate(ip, port, ms_info);
    LOG(INFO) << "send update to data node success";

  } else if (ZPMetaUpdateOP::OP_REMOVE == op) {
    s = zp_meta_server->OffNode(ip, port);
    if (!s.ok()) {
      LOG(ERROR) << "UpdateThread: OffNode error, " << s.ToString();
      return s;
    }

    s = zp_meta_server->GetMSInfo(ms_info);
    if (!s.ok()) {
      LOG(ERROR) << "UpdateThread: GetMSInfo error, " << s.ToString();
      return s;
    }

    // BroadCast Update Message
    SendUpdate(ms_info);
    LOG(INFO) << "broadcast update to data node success";
  }
  return s;
}

slash::Status ZPMetaUpdateThread::MetaBroadcast() {
  slash::Status s;
  ZPMeta::MetaCmd_Update ms_info;
  s = zp_meta_server->GetMSInfo(ms_info);
  if (!s.ok()) {
    LOG(ERROR) << "UpdateThread: GetAllPartition error, " << s.ToString();
    return s;
  }

  // Send Update Message
  SendUpdate(ms_info);
  LOG(INFO) << "send update to data node success";
  return s;
}

slash::Status ZPMetaUpdateThread::UpdateSender(const std::string &ip, int port, ZPMetaUpdateOP op) {
  std::string ip_port = slash::IpPortString(ip, port);
  slash::Status s;
  if (ZPMetaUpdateOP::OP_ADD == op) {
    if (data_sender_.find(ip_port) == data_sender_.end()) {
      pink::PbCli *pb_cli = new pink::PbCli();
      pink::Status s = pb_cli->Connect(ip, port);
      if (!s.ok()) {
        LOG(INFO) << "connect to " << ip << ":" << port << " failed";
        return slash::Status::Corruption(s.ToString());
      }
      LOG(INFO) << "connect to " << ip << ":" << port << " success";
      pb_cli->set_send_timeout(1000);
      pb_cli->set_recv_timeout(1000);
      data_sender_.insert(std::pair<std::string, pink::PbCli*>(ip_port, pb_cli));
    }
  } else if (ZPMetaUpdateOP::OP_REMOVE == op) {
    data_sender_.erase(ip_port);
  } else {
      return slash::Status::Corruption("unknown ZPMetaUpdate OP");
  }
  return slash::Status::OK();
}

void ZPMetaUpdateThread::SendUpdate(ZPMeta::MetaCmd_Update &ms_info) {
  ZPMeta::MetaCmd request;
  request.set_type(ZPMeta::MetaCmd_Type_UPDATE);
  ZPMeta::MetaCmd_Update *update_cmd = request.mutable_update();
  update_cmd->CopyFrom(ms_info);
  
  ZPMeta::MetaCmdResponse response;

  int retry_num = 0;
  pink::Status s;
  DataCliMap::iterator it = data_sender_.begin();
  for (; it != data_sender_.end();) {
    s = (it->second)->Send(&request);
    if (s.ok()) {
      s = (it->second)->Recv(&response); 
    }
    if (s.ok() &&
        response.type() == ZPMeta::MetaCmdResponse_Type_UPDATE &&
        response.status().code() == 0) {
      //Success
    } else {
      LOG(ERROR) << "send update failed, error:" << s.ToString() << " retry_num:" <<retry_num << ", errno : " << errno << ", errnomsg" << strerror(errno);
      if (retry_num < ZP_META_UPDATE_RETRY_TIME) {
        ++retry_num;
      } else {
        retry_num = 0;
      }
    }
    // move next in non-retry loop
    if (retry_num == 0) {
      it++;
    }
  }
}

void ZPMetaUpdateThread::SendUpdate(const std::string& ip, int port, ZPMeta::MetaCmd_Update &ms_info) {
  ZPMeta::MetaCmd request;
  request.set_type(ZPMeta::MetaCmd_Type_UPDATE);
  ZPMeta::MetaCmd_Update *update_cmd = request.mutable_update();
  update_cmd->CopyFrom(ms_info);
  
  ZPMeta::MetaCmdResponse response;

  int retry_num = 0;
  pink::Status s;
  std::string ip_port = slash::IpPortString(ip, port);
  DataCliMap::iterator it = data_sender_.find(ip_port);
  if (it != data_sender_.end()) {
    s = (it->second)->Send(&request);
    if (s.ok()) {
      s = (it->second)->Recv(&response);
    }
    if (s.ok() &&
        response.type() == ZPMeta::MetaCmdResponse_Type_UPDATE &&
        response.status().code() == 0) {
      //Success
    } else {
      LOG(ERROR) << "send update failed, error:" << s.ToString() << " retry_num:" <<retry_num << ", errno : " << errno << ", errnomsg" << strerror(errno);
    }
  }
}
