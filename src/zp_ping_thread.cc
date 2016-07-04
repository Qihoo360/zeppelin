#include "zp_ping_thread.h"

#include <glog/logging.h>
#include "zp_server.h"

extern ZPServer* zp_server;

ZPPingThread::~ZPPingThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  delete cli_;
  DLOG(INFO) << " Ping thread " << pthread_self() << " exit!!!";
}

pink::Status ZPPingThread::Send() {
  std::string wbuf_str;
  if (!is_first_send_) {
    ServerControl::Ping_Request request;
    ServerControl::Node* node = request.mutable_node();
    node->set_ip(zp_server->local_ip());
    node->set_port(zp_server->local_port());

    DLOG(INFO) << "Ping " << zp_server->local_ip() << ":" << zp_server->local_port();
    cli_->set_opcode(ServerControl::OPCODE::PING);
    return cli_->Send(&request);
  } else {
    ServerControl::Join_Request request;
    ServerControl::Node* node = request.mutable_node();
    node->set_ip(zp_server->local_ip());
    node->set_port(zp_server->local_port());

    uint32_t filenum = 0;
    uint64_t offset = 0;
    zp_server->logger_->GetProducerStatus(&filenum, &offset);
    request.set_filenum(filenum);
    request.set_offset(offset);

    cli_->set_opcode(ServerControl::OPCODE::JOIN);
    is_first_send_ = false;

    LOG(INFO) << "Join with SyncPoint (" << filenum << ", " << offset << ")";
    LOG(INFO) << "          Node (" << request.node().ip() << ":" << request.node().port() << ")";
    LOG(INFO) << "          Local (" << zp_server->local_ip() << ":" << zp_server->local_port() << ")";
    return cli_->Send(&request);
  }
}

pink::Status ZPPingThread::RecvProc() {
  pink::Status result;
  int32_t opcode = cli_->opcode();

  switch (opcode) {
    case ServerControl::OPCODE::JOIN: {
      ServerControl::Join_Response response;
      result = cli_->Recv(&response); 
      DLOG(INFO) << "Join recv: " << result.ToString();
      if (!result.ok()) {
        LOG(WARNING) << "Join recv failed " << result.ToString();
      }
      break;
    }
    case ServerControl::OPCODE::PING: {
      ServerControl::Ping_Response response;
      result = cli_->Recv(&response); 
      //DLOG(INFO) << "Ping recv: " << result.ToString();
      if (!result.ok()) {
        LOG(WARNING) << "Ping recv failed " << result.ToString();
      }
      break;
    }
    default:
      break;
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

  while (!should_exit_ && zp_server->ShouldJoin()) {
    // Connect with heartbeat port
    s = cli_->Connect(zp_server->seed_ip(), zp_server->seed_port() + kPortShiftHeartbeat);

    if (s.ok()) {
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      connect_retry_times = 0;

      // TODO ping connect ok
      //zp_server->PlusMasterConnection();

      // Send && Recv
      while (!should_exit_) {
        s = Send();
        if (!s.ok()) {
          DLOG(WARNING) << "Ping send failed once, " << s.ToString();
          break;
        }

        s = RecvProc();
        if (!s.ok()) {
          DLOG(WARNING) << "Ping recv failed once, " << s.ToString();
          break;
        }

        gettimeofday(&last_interaction, NULL);
        DLOG(INFO) << "Ping master success";
        sleep(kPingInterval);
      }

      if (s.IsTimeout()) {
        LOG(WARNING) << "Ping timeout once";
        gettimeofday(&now, NULL);
        if (now.tv_sec - last_interaction.tv_sec > 30) {
          LOG(WARNING) << "Ping leader timeout";
          //zp_server->zp_binlog_receiver_thread()->KillBinlogSender();
          break;
        }
      }

      //zp_server->MinusMasterConnection();
    } else if (s.IsTimeout()) {
      LOG(WARNING) << "PingThread, Connect timeout once";
      if ((++connect_retry_times) >= 30) {
        LOG(WARNING) << "PingThread, Connect timeout 30 times, disconnect with master";
        //zp_server->zp_binlog_receiver_thread()->KillBinlogSender();
        connect_retry_times = 0;
      }
    } else {
      LOG(ERROR) << "PingThread Connect failed caz " << s.ToString();
    }

    close(cli_->fd());
    sleep(3);
  }
  return NULL;
}
