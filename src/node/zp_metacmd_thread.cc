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

        //s = Send();
        if (!s.ok()) {
          DLOG(WARNING) << "Metacmd send failed once, " << s.ToString();
          continue;
        }
        DLOG(INFO) << "Metacmd send ok!";

        //s = RecvProc();
        if (!s.ok()) {
          DLOG(WARNING) << "Metacmd recv failed once, " << s.ToString();
          continue;
        }

        gettimeofday(&last_interaction, NULL);
        DLOG(INFO) << "Metacmd MetaServer success";
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
