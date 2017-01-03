#include "zp_ping_thread.h"
#include <glog/logging.h>
#include "zp_data_server.h"
#include "zp_meta.pb.h"
#include "zp_const.h"

extern ZPDataServer* zp_data_server;

ZPPingThread::~ZPPingThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  delete cli_;
  LOG(INFO) << " Ping thread " << pthread_self() << " exit!!!";
}

pink::Status ZPPingThread::Send() {
  ZPMeta::MetaCmd request;
  int64_t meta_epoch = zp_data_server->meta_epoch();
  ZPMeta::MetaCmd_Ping* ping = request.mutable_ping();
  ping->set_version(meta_epoch);
  ZPMeta::Node* node = ping->mutable_node();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());
  request.set_type(ZPMeta::Type::PING);
  
  std::unordered_map<std::string, std::vector<PartitionBinlogOffset>> all_offset;
  zp_data_server->DumpTableBinlogOffsets("", all_offset);
  for (auto& item : all_offset) {
    for(auto& p : item.second) {
      ZPMeta::SyncOffset *offset = ping->add_offset();
      offset->set_table_name(item.first);
      offset->set_partition(p.partition_id);
      offset->set_filenum(p.filenum);
      offset->set_offset(p.offset);
    }
  }

  DLOG(INFO) << "Ping Meta (" << zp_data_server->meta_ip()
    << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd
    << ") with Epoch: " << meta_epoch;
  return cli_->Send(&request);
}

pink::Status ZPPingThread::RecvProc() {
  pink::Status result;
  ZPMeta::MetaCmdResponse response;
  result = cli_->Recv(&response); 
  DLOG(INFO) << "Ping Recv from Meta (" << zp_data_server->meta_ip() << ":"
    << zp_data_server->meta_port() + kMetaPortShiftCmd << ")";
  if (!result.ok()) {
    return result;
  }
  if (response.code() != ZPMeta::StatusCode::OK) {
    return pink::Status::Corruption("Receive reponse with error code");
  }
  // StatusCode OK
  if (response.type() == ZPMeta::Type::PING) {
    zp_data_server->TryUpdateEpoch(response.ping().version());
    return pink::Status::OK();
  }
  return pink::Status::Corruption("Receive reponse whose type is not ping");
}

void* ZPPingThread::ThreadMain() {
  struct timeval now, last_interaction;
  pink::Status s;

  while (!should_exit_) {
    zp_data_server->PickMeta();
    std::string meta_ip = zp_data_server->meta_ip();
    int meta_port = zp_data_server->meta_port() + kMetaPortShiftCmd;
    // Connect with heartbeat port
    DLOG(INFO) << "Ping will connect ("<< meta_ip << ":" << meta_port << ")";
    s = cli_->Connect(meta_ip, meta_port);
    if (s.ok()) {
      DLOG(INFO) << "Ping connect ("<< meta_ip << ":" << meta_port << ") ok!";
      gettimeofday(&now, NULL);
      last_interaction = now;
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);

      // Send && Recv
      while (!should_exit_) {
        gettimeofday(&now, NULL);
        if (now.tv_sec - last_interaction.tv_sec > kNodeMetaTimeoutN) {
          LOG(WARNING) << "Ping meta ("<< meta_ip << ":" << meta_port << ") timeout, reconnect!";
          break;
        }
        sleep(kPingInterval);

        // Send ping to meta
        s = Send();
        if (!s.ok()) {
          LOG(WARNING) << "Ping send to ("<< meta_ip << ":" << meta_port << ") failed! caz: " << s.ToString();
          continue;
        }
        DLOG(INFO) << "Ping send to ("<< meta_ip << ":" << meta_port << ") success!";

        // Recv from meta
        s = RecvProc();
        if (!s.ok()) {
          LOG(WARNING) << "Ping recv from ("<< meta_ip << ":" << meta_port << ") failed! caz: " << s.ToString();
          continue;
        }

        gettimeofday(&last_interaction, NULL);
        DLOG(INFO) << "Ping recv from ("<< meta_ip << ":" << meta_port << ") success!";
      }

      cli_->Close();
    } else {
      LOG(WARNING) << "Ping connect ("<< meta_ip << ":" << meta_port << ") failed!";
    }
    sleep(kPingInterval);
  }
  return NULL;
}
