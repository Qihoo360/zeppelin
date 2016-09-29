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
  std::string wbuf_str;
  if (!is_first_send_) {
    ZPMeta::MetaCmd request;
    ZPMeta::MetaCmd_Ping* ping = request.mutable_ping();
    int64_t meta_epoch = zp_data_server->meta_epoch();
    ping->set_version(meta_epoch);

    ZPMeta::Node* node = ping->mutable_node();
    node->set_ip(zp_data_server->local_ip());
    node->set_port(zp_data_server->local_port());

    DLOG(INFO) << "Ping master(" << zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd
        << ") with Epoch: " << meta_epoch << " local("
        << zp_data_server->local_ip() << ":" << zp_data_server->local_port() << ")";
    request.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_PING);
    return cli_->Send(&request);
  } else {
    ZPMeta::MetaCmd request;
    ZPMeta::MetaCmd_Join* join = request.mutable_join();
    ZPMeta::Node* node = join->mutable_node();
    node->set_ip(zp_data_server->local_ip());
    node->set_port(zp_data_server->local_port());

    DLOG(INFO) << "PingThead Join MetaServer(" << zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd << ") with local("<< zp_data_server->local_ip() << ":" << zp_data_server->local_port() << ")";
    request.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_JOIN);
    return cli_->Send(&request);
  }
}

pink::Status ZPPingThread::RecvProc() {
  pink::Status result;
  ZPMeta::MetaCmdResponse response;
  result = cli_->Recv(&response); 
  DLOG(INFO) << "Ping recv: " << result.ToString();
  if (result.ok()) {
    switch (response.type()) {
      case ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_JOIN: {
        // Do Master-Slave sync
        is_first_send_ = false;
        break;
      }
      case ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PING: {
        int64_t current_epoch = response.ping().version();
        zp_data_server->UpdateEpoch(response.ping().version());
        DLOG(INFO) << "ping_thread: receive pong(" << current_epoch << ") from meta server";
        break;
      }
      //case ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_UPDATE: {
      //  break;
      //}
      default:
        break;
    }
  }
  return result;
}

void* ZPPingThread::ThreadMain() {
  int connect_retry_times = 0;
  struct timeval last_interaction;
  struct timeval now;
  gettimeofday(&now, NULL);
  last_interaction = now;

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
      DLOG(INFO) << "Ping connect ("<< zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd << ") ok!";
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      connect_retry_times = 0;

      // TODO ping connect ok
      zp_data_server->PlusMetaServerConns();

      // Send && Recv
      while (!should_exit_) {
        gettimeofday(&now, NULL);
        if (now.tv_sec - last_interaction.tv_sec > NODE_META_TIMEOUT_N) {
          gettimeofday(&last_interaction, NULL);
          LOG(WARNING) << "Ping leader timeout, will resend Join";
          break;
        }

        sleep(kPingInterval);

        s = Send();
        if (!s.ok()) {
          DLOG(WARNING) << "Ping send failed once, " << s.ToString();
          continue;
        }
        DLOG(INFO) << "Ping send ok!";

        s = RecvProc();
        if (!s.ok()) {
          DLOG(WARNING) << "Ping recv failed once, " << s.ToString();
          continue;
        }

        gettimeofday(&last_interaction, NULL);
        DLOG(INFO) << "Ping MetaServer success";
      }

      //if (s.IsTimeout()) {
      //  LOG(WARNING) << "Ping timeout once";
      //  gettimeofday(&now, NULL);
      //  if (now.tv_sec - last_interaction.tv_sec > 30) {
      //    LOG(WARNING) << "Ping leader timeout, will resend Join";
      //    zp_data_server->MinusMetaServerConns();
      //    zp_data_server->zp_metacmd_worker_thread()->KillMetacmdConn();
      //    break;
      //  }
      //}

      zp_data_server->MinusMetaServerConns();
      //zp_data_server->zp_metacmd_worker_thread()->KillMetacmdConn();
      cli_->Close();
    } else {
      LOG(WARNING) << "PingThread Connect failed caz " << s.ToString();
      if ((++connect_retry_times) >= 30) {
        LOG(WARNING) << "PingThread, Connect failed 30 times, disconnect with meta server";
        connect_retry_times = 0;
        is_first_send_ = true;
      }
    }

    // TODO rm
    //int ret = close(cli_->fd());
    //printf ("pingthread close:cli_->fd()=%d ret=%d\n", cli_->fd(), ret);
    sleep(3);
  }
  return NULL;
}
