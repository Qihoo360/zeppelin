#include "zp_trysync_thread.h"

#include <glog/logging.h>
#include "zp_data_server.h"

extern ZPDataServer* zp_data_server;

ZPTrySyncThread::~ZPTrySyncThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  delete cli_;
  DLOG(INFO) << " TrySync thread " << pthread_self() << " exit!!!";
}

pink::Status ZPTrySyncThread::Send() {
  std::string wbuf_str;

  ZPMeta::MetaCmd request;
  ZPMeta::MetaCmd_Sync* sync = request.mutable_sync();

  request.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_SYNC);

  ZPMeta::Node* node = sync->mutable_node();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());

  uint32_t filenum = 0;
  uint64_t offset = 0;
  zp_data_server->logger_->GetProducerStatus(&filenum, &offset);
  sync->set_filenum(filenum);
  sync->set_offset(offset);

  LOG(INFO) << "Join with SyncPoint (" << filenum << ", " << offset << ")";
  LOG(INFO) << "          Node (" << sync->node().ip() << ":" << sync->node().port() << ")";
  LOG(INFO) << "          Local (" << zp_data_server->local_ip() << ":" << zp_data_server->local_port() << ")";
  return cli_->Send(&request);
}

pink::Status ZPTrySyncThread::Recv() {
  ZPMeta::MetaCmdResponse response;
  pink::Status result = cli_->Recv(&response); 

  DLOG(INFO) << "TrySync receive: " << result.ToString();
  if (!result.ok()) {
    LOG(WARNING) << "TrySync recv failed " << result.ToString();
    return result;
  }

  switch (response.type()) {
    case ZPMeta::MetaCmdResponse_Type::MetaCmdResponse_Type_SYNC: {
      if (response.status().code() == 0) {
        DLOG(INFO) << "TrySync recv success.";
        return pink::Status::OK(); 
      } else {
        DLOG(INFO) << "TrySync failed caz " << response.status().msg();
        return pink::Status::Corruption(response.status().msg());
      }
      break;
    }
    default:
      break;
  }

  return result;
}

void* ZPTrySyncThread::ThreadMain() {
  int connect_retry_times = 0;
  pink::Status s;

  while (!should_exit_) {
    if (!zp_data_server->ShouldJoin()) {
      sleep(kTrySyncInterval);
      continue;
    }

    // Connect with Leader port
    s = cli_->Connect(zp_data_server->seed_ip(), zp_data_server->seed_port() + kPortShiftDataCmd);
    if (s.ok()) {
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      connect_retry_times = 0;

      // TODO TrySync connect ok
      //zp_data_server->PlusMasterConnection();

      // Send && Recv
      s = Send();
      if (s.ok()) {
        s = Recv();
        if (s.ok()) {
          zp_data_server->JoinDone();
        } else {
          DLOG(WARNING) << "TrySync recv failed once, " << s.ToString();
        }
      } else {
        DLOG(WARNING) << "TrySync send failed once, " << s.ToString();
      }
    } else {
      LOG(ERROR) << "TrySyncThread Connect failed caz " << s.ToString();
    }
    
  // TODO  retry limit
  //  if (s.IsTimeout()) {
  //    LOG(WARNING) << "TrySyncThread, timeout once";
  //    if ((++connect_retry_times) >= 30) {
  //      LOG(WARNING) << "TrySyncThread, timeout 30 times, disconnect with master";
  //      connect_retry_times = 0;
  //    }
  //  } else {
  //    LOG(ERROR) << "TrySyncThread Connect " << s.ToString();
  //  }

    close(cli_->fd());
    sleep(kTrySyncInterval);
  }

  return NULL;
}
