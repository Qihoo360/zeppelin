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
  DLOG(INFO) << " Ping thread " << pthread_self() << " exit!!!";
}

pink::Status ZPPingThread::Send() {
  ZPMeta::MetaCmd request;
  if (!is_first_send_) {
    ZPMeta::MetaCmd_Ping* ping = request.mutable_ping();
    int64_t meta_epoch = zp_data_server->meta_epoch();
    ping->set_version(meta_epoch);
    ZPMeta::Node* node = ping->mutable_node();
    node->set_ip(zp_data_server->local_ip());
    node->set_port(zp_data_server->local_port());
    request.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_PING);

    DLOG(INFO) << "Ping master(" << zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd
        << ") with Epoch: " << meta_epoch << " local("
        << zp_data_server->local_ip() << ":" << zp_data_server->local_port() << ")";
  } else {
    ZPMeta::MetaCmd_Join* join = request.mutable_join();
    ZPMeta::Node* node = join->mutable_node();
    node->set_ip(zp_data_server->local_ip());
    node->set_port(zp_data_server->local_port());
    request.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_JOIN);
    
    DLOG(INFO) << "PingThead Join MetaServer(" << zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd
      << ") with local("
      << zp_data_server->local_ip() << ":" << zp_data_server->local_port() << ")";
  }
  return cli_->Send(&request);
}

pink::Status ZPPingThread::RecvProc() {
  pink::Status result;
  ZPMeta::MetaCmdResponse response;
  result = cli_->Recv(&response); 
  DLOG(INFO) << "Ping recv: " << result.ToString();
  if (result.ok()) {
    if (response.status().code() == ZPMeta::StatusCode::kOk) {
      switch (response.type()) {
        case ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_JOIN:
          // Do Master-Slave sync
          is_first_send_ = false;
          break;
        case ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PING:
          zp_data_server->TryUpdateEpoch(response.ping().version());
          DLOG(INFO) << "ping_thread: receive pong(" << response.ping().version() << ") from meta server";
          break;
        default:
          break;
      }
    } else if (response.status().code() == ZPMeta::StatusCode::kNotFound) {
      result = pink::Status::NotFound("Unrecognized");
    } else {
      result = pink::Status::Corruption("Error happend when ping meta");
    }
  }
  return result;
}

void* ZPPingThread::ThreadMain() {
  struct timeval now, last_interaction;
  pink::Status s;

  while (!should_exit_) {
    if(!zp_data_server->ShouldJoinMeta()) {
      sleep(3);
      continue;
    }
    zp_data_server->PickMeta();
    // Connect with heartbeat port
    DLOG(INFO) << "Ping will connect ("<< zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd << ")";
    s = cli_->Connect(zp_data_server->meta_ip(), zp_data_server->meta_port() + kMetaPortShiftCmd);
    if (s.ok()) {
      gettimeofday(&now, NULL);
      last_interaction = now;
      DLOG(INFO) << "Ping connect ("<< zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd << ") ok!";
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);

      // Ping connect ok
      zp_data_server->MetaConnected();

      // Send && Recv
      while (!should_exit_) {
        gettimeofday(&now, NULL);
        if (now.tv_sec - last_interaction.tv_sec > kNodeMetaTimeoutN) {
          LOG(WARNING) << "Ping leader timeout, reconnect";
          break;
        }
        sleep(kPingInterval);

        s = Send();
        if (!s.ok()) {
          DLOG(WARNING) << "Ping send failed: " << s.ToString();
          continue;
        }
        DLOG(INFO) << "Ping send ok!";

        s = RecvProc();
        if (s.IsNotFound()) {
          // Try to join again
          is_first_send_ = true;
          continue;
        } else if (!s.ok()) {
          DLOG(WARNING) << "Ping recv failed: " << s.ToString();
          continue;
        }

        gettimeofday(&last_interaction, NULL);
        DLOG(INFO) << "Ping MetaServer success";
      }

      zp_data_server->MetaDisconnect();
      cli_->Close();
    } else {
      DLOG(WARNING) << "Ping connect failed: " << s.ToString();
    }
    sleep(3);
  }
  return NULL;
}
