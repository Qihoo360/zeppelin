#include "zp_trysync_thread.h"

#include <fstream>
#include <glog/logging.h>
#include "zp_data_server.h"

#include "rsync.h"

extern ZPDataServer* zp_data_server;

ZPTrySyncThread::~ZPTrySyncThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  delete cli_;
  DLOG(INFO) << " TrySync thread " << pthread_self() << " exit!!!";
}

bool ZPTrySyncThread::Send() {
  std::string wbuf_str;

  client::CmdRequest request;
  client::CmdRequest_Sync* sync = request.mutable_sync();

  request.set_type(client::Type::SYNC);

  client::Node* node = sync->mutable_node();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());

  uint32_t filenum = 0;
  uint64_t offset = 0;
  zp_data_server->logger_->GetProducerStatus(&filenum, &offset);
  sync->set_filenum(filenum);
  sync->set_offset(offset);

  pink::Status s = cli_->Send(&request);
  LOG(INFO) << "TrySync with SyncPoint (" << filenum << ", " << offset << ")";
  LOG(INFO) << "          Node (" << sync->node().ip() << ":" << sync->node().port() << ")";
  LOG(INFO) << "          Local (" << zp_data_server->local_ip() << ":" << zp_data_server->local_port() << ")";
  if (!s.ok()) {
    LOG(WARNING) << "TrySync failed caz " << s.ToString();
    return false;
  }
  return true;
}

int ZPTrySyncThread::Recv() {
  client::CmdResponse response;
  pink::Status result = cli_->Recv(&response); 

  //DLOG(INFO) << "TrySync receive: " << result.ToString();
  if (!result.ok()) {
    LOG(WARNING) << "TrySync recv failed " << result.ToString();
    return -2;
  }

  if (response.type() == client::Type::SYNC) {
    if (response.sync().code() == client::StatusCode::kOk) {
      DLOG(INFO) << "TrySync recv success.";
    } else if (response.sync().code() == client::StatusCode::kWait) {
      zp_data_server->SetWaitDBSync();
      DLOG(INFO) << "TrySync recv kWait.";
      return -1;
    } else {
      DLOG(INFO) << "TrySync failed caz " << response.sync().msg();
      return -2;
    }
  }

  return 0;
}

void* ZPTrySyncThread::ThreadMain() {
  //int connect_retry_times = 0;
  pink::Status s;

  while (!should_exit_) {
    if (zp_data_server->ShouldWaitDBSync()) {

      if (!rsync_flag_) {
        rsync_flag_ = true;

        PrepareRsync();
        // Start rsync
        std::string dbsync_path = zp_data_server->db_sync_path();
        std::string ip_port = slash::IpPortString(zp_data_server->master_ip(), zp_data_server->master_port());
        // We append the master ip port after module name
        // To make sure only data from current master is received
        int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port, zp_data_server->local_port() + kPortShiftRsync);
        if (0 != ret) {
          LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
        }
        LOG(INFO) << "Finish to start rsync, path:" << dbsync_path;
      }

      if (TryUpdateMasterOffset()) {
        LOG(INFO) << "Success Update Master Offset";
      }
    }

    if (!zp_data_server->ShouldSync()) {
      sleep(10);
      //sleep(kTrySyncInterval);
      continue;
    }

    // Connect with Leader port
    s = cli_->Connect(zp_data_server->master_ip(), zp_data_server->master_port());
    DLOG(WARNING) << "TrySync connect(" << zp_data_server->master_ip() << ":" << zp_data_server->master_port() + kPortShiftDataCmd << ")" << s.ToString();
    if (s.ok()) {
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      //connect_retry_times = 0;

      // TODO TrySync connect ok
      //zp_data_server->PlusMasterConnection();

      // Send && Recv
      if (Send()) {
        int ret = Recv();
        if (ret == 0) {
          zp_data_server->SyncDone();
        } else if (ret == -1) {
        } else {
          //DLOG(WARNING) << "TrySync recv failed, " << s.ToString();
        }
      }
      cli_->Close();
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

    //close(cli_->fd());
    sleep(kTrySyncInterval);
  }

  return NULL;
}

void ZPTrySyncThread::PrepareRsync() {
  std::string db_sync_path = zp_data_server->db_sync_path();
  slash::StopRsync(db_sync_path);
  slash::CreatePath(db_sync_path + "kv");
  slash::CreatePath(db_sync_path + "hash");
  slash::CreatePath(db_sync_path + "list");
  slash::CreatePath(db_sync_path + "set");
  slash::CreatePath(db_sync_path + "zset");
}

bool ZPTrySyncThread::TryUpdateMasterOffset() {
  // Check dbsync finished
  std::string info_path = zp_data_server->db_sync_path() + kBgsaveInfoFile;
  if (!slash::FileExists(info_path)) {
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Failed to open info file after db sync";
    return false;
  }
  std::string line, master_ip;
  int lineno = 0;
  int64_t filenum = 0, offset = 0, tmp = 0, master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 6) {
      if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
        LOG(WARNING) << "Format of info file after db sync error, line : " << line;
        is.close();
        return false;
      }
      if (lineno == 3) { master_port = tmp; }
      else if (lineno == 4) { filenum = tmp; }
      else { offset = tmp; }

    } else if (lineno > 5) {
      LOG(WARNING) << "Format of info file after db sync error, line : " << line;
      is.close();
      return false;
    }
  }
  is.close();
  LOG(INFO) << "Information from dbsync info. master_ip: " << master_ip
    << ", master_port: " << master_port
    << ", filenum: " << filenum
    << ", offset: " << offset;

  // Sanity check
  if (master_ip != zp_data_server->master_ip() ||
      master_port != zp_data_server->master_port()) {
    LOG(ERROR) << "Error master ip port: " << master_ip << ":" << master_port;
    return false;
  }

  // Replace the old db
  slash::StopRsync(zp_data_server->db_sync_path());
  rsync_flag_ = false;
  slash::DeleteFile(info_path);
  if (!zp_data_server->ChangeDb(zp_data_server->db_sync_path())) {
    LOG(WARNING) << "Failed to change db";
    return false;
  }

  // Update master offset
  zp_data_server->logger_->SetProducerStatus(filenum, offset);
  zp_data_server->WaitDBSyncDone();
  return true;
}
