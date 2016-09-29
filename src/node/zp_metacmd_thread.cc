#include "zp_metacmd_thread.h"

#include <glog/logging.h>
#include "zp_data_server.h"

#include "slash_mutex.h"

extern ZPDataServer* zp_data_server;

ZPMetacmdThread::ZPMetacmdThread()
  : query_num_(0) {
  cli_ = new pink::PbCli();
  cli_->set_connect_timeout(1500);
}

ZPMetacmdThread::~ZPMetacmdThread() {
  // may be redunct
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  delete cli_;
  LOG(INFO) << "ZPMetacmd thread " << thread_id() << " exit!!!";
}

pink::Status ZPMetacmdThread::Send() {
  ZPMeta::MetaCmd request;
  ZPMeta::MetaCmd_Pull* pull = request.mutable_pull();

  DLOG(INFO) << "MetacmdThead Pull MetaServer(" << zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd << ") with local("<< zp_data_server->local_ip() << ":" << zp_data_server->local_port() << ")";
  request.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_PULL);
  return cli_->Send(&request);
}

pink::Status ZPMetacmdThread::Recv() {
  pink::Status result;
  ZPMeta::MetaCmdResponse response;
  result = cli_->Recv(&response); 
  DLOG(INFO) << "MetacmdThread recv: " << result.ToString();
  if (result.ok()) {
    switch (response.type()) {
      case ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_PULL: {
        if (response.status().code() != ZPMeta::StatusCode::kOk) {
          DLOG(INFO) << "receive Pull error: " << response.status().msg();
          return pink::Status::IOError(response.status().msg());
        }

        int64_t current_epoch = response.pull().version();
        ZPMeta::MetaCmdResponse_Pull pull = response.pull();

        DLOG(INFO) << "receive Pull message, will handle " << pull.info_size() << " Partitions.";
        for (int i = 0; i < pull.info_size(); i++) {
          const ZPMeta::Partitions& partition = pull.info(i);
          DLOG(INFO) << " - handle Partition " << partition.id() << ": master is " << partition.master().ip() << ":" << partition.master().port();

          std::vector<Node> nodes;
          nodes.push_back(Node(partition.master().ip(), partition.master().port()));
          for (int j = 0; j < partition.slaves_size(); j++) {
            nodes.push_back(Node(partition.slaves(j).ip(), partition.slaves(j).port()));
          }

          bool result = zp_data_server->UpdateOrAddPartition(partition.id(), nodes);
          if (!result) {
            LOG(WARNING) << "AddPartition failed";
          }
        }

        DLOG(INFO) << "MetacmdThread: receive pull(" << current_epoch << ") from meta server";
        break;
      }
      default:
        break;
    }
  }
  return result;
}

void* ZPMetacmdThread::ThreadMain() {
  int connect_retry_times = 0;
  struct timeval last_interaction;
  struct timeval now;
  gettimeofday(&now, NULL);
  last_interaction = now;

  pink::Status s;

  while (!should_exit_) {
    if (!zp_data_server->ShouldPullMeta()) {
      sleep(3);
      continue;
    }

    zp_data_server->PickMeta();
    // Connect with heartbeat port
    DLOG(INFO) << "MetacmdThread will connect ("<< zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd << ")";
    s = cli_->Connect(zp_data_server->meta_ip(), zp_data_server->meta_port() + kMetaPortShiftCmd);
    if (s.ok()) {
      DLOG(INFO) << "Metacmd connect ("<< zp_data_server->meta_ip() << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd << ") ok!";
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      connect_retry_times = 0;

      // TODO Metacmd connect ok
      //zp_data_server->PlusMetaServerConns();

      // Send && Recv
      while (!should_exit_) {
        gettimeofday(&now, NULL);
        if (now.tv_sec - last_interaction.tv_sec > NODE_META_TIMEOUT_N) {
          gettimeofday(&last_interaction, NULL);
          LOG(WARNING) << "Metacmd leader timeout, will resend Join";
          break;
        }

        sleep(kMetacmdInterval);

        s = Send();
        if (!s.ok()) {
          DLOG(WARNING) << "Metacmd send failed once, " << s.ToString();
          continue;
        }
        DLOG(INFO) << "Metacmd send ok!";

        s = Recv();
        if (!s.ok()) {
          DLOG(WARNING) << "Metacmd recv failed once, " << s.ToString();
          continue;
        }
        // TODO when we recv OK, we will FinishPullMeta
        zp_data_server->FinishPullMeta();
        gettimeofday(&last_interaction, NULL);
        DLOG(INFO) << "Metacmd MetaServer success";
        break;
      }

      //if (s.IsTimeout()) {
      //  LOG(WARNING) << "Metacmd timeout once";
      //  gettimeofday(&now, NULL);
      //  if (now.tv_sec - last_interaction.tv_sec > 30) {
      //    LOG(WARNING) << "Metacmd leader timeout, will resend Join";
      //    zp_data_server->MinusMetaServerConns();
      //    zp_data_server->zp_metacmd_worker_thread()->KillMetacmdConn();
      //    break;
      //  }
      //}

      cli_->Close();
    } else {
      LOG(WARNING) << "MetacmdThread Connect failed caz " << s.ToString();
      if ((++connect_retry_times) >= 30) {
        LOG(WARNING) << "MetacmdThread, Connect failed 30 times, disconnect with meta server";
        connect_retry_times = 0;
      }
    }

    // TODO rm
    //printf ("Metacmdthread close:cli_->fd()=%d ret=%d\n", cli_->fd(), ret);
    sleep(3);
  }
  return NULL;
}
