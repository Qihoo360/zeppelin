#include <glog/logging.h>

#include "rsync.h"
#include "zp_data_server.h"
#include "zp_data_partition.h"
#include "zp_trysync_thread.h"

extern ZPDataServer* zp_data_server;

ZPTrySyncThread::ZPTrySyncThread():
  should_exit_(false),
  rsync_flag_(0) {
    bg_thread_ = new pink::BGThread();
}

ZPTrySyncThread::~ZPTrySyncThread() {
  should_exit_ = true;
  delete bg_thread_;
  for (auto &kv: client_pool_) {
    kv.second->Close();
  }
  slash::StopRsync(zp_data_server->db_sync_path());
  LOG(INFO) << " TrySync thread " << pthread_self() << " exit!!!";
}

void ZPTrySyncThread::TrySyncTaskSchedule(const std::string &table_name, int partition_id) {
  slash::MutexLock l(&bg_thread_protector_);
  bg_thread_->StartIfNeed();
  TrySyncTaskArg *targ = new TrySyncTaskArg(this, table_name, partition_id);
  bg_thread_->Schedule(&DoTrySyncTask, static_cast<void*>(targ));
}

void ZPTrySyncThread::DoTrySyncTask(void* arg) {
  TrySyncTaskArg* targ = static_cast<TrySyncTaskArg*>(arg);
  (targ->thread)->TrySyncTask(targ->table_name, targ->partition_id);
  delete targ;
}

void ZPTrySyncThread::TrySyncTask(const std::string& table_name, int partition_id) {
  // Wait until the server is availible
  while (!should_exit_ && !zp_data_server->Availible()) {
    sleep(kTrySyncInterval);
  }
  if (should_exit_) {
    return;
  }

  //Get Partiton by id
  Partition* partition = zp_data_server->GetTablePartitionById(table_name, partition_id);
  
  // Do try sync
  if (!SendTrySync(partition)) {
    // Need one more trysync, since error happenning or waiting for db sync
    sleep(kTrySyncInterval);
    zp_data_server->AddSyncTask(table_name, partition_id);
  }
}

bool ZPTrySyncThread::Send(Partition* partition, pink::PbCli* cli) {
  std::string wbuf_str;
  client::CmdRequest request;
  client::CmdRequest_Sync* sync = request.mutable_sync();

  request.set_type(client::Type::SYNC);

  client::Node* node = sync->mutable_node();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());

  uint32_t filenum = 0;
  uint64_t offset = 0;
  partition->GetBinlogOffset(&filenum, &offset);
  sync->set_filenum(filenum);
  sync->set_offset(offset);
  sync->set_partition_id(partition->partition_id());

  pink::Status s = cli->Send(&request);
  DLOG(INFO) << "TrySync: Partition " << partition->partition_id() << " with SyncPoint ("
    << sync->node().ip() << ":" << sync->node().port() << ", " << filenum << ", " << offset << ")";
  if (!s.ok()) {
    LOG(WARNING) << "TrySync send failed, Partition:" << partition->partition_id() << ",caz " << s.ToString();
    return false;
  }
  return true;
}

int ZPTrySyncThread::Recv(int partition_id, pink::PbCli* cli) {
  client::CmdResponse response;
  pink::Status result = cli->Recv(&response); 

  if (!result.ok()) {
    LOG(WARNING) << "TrySync recv failed, Partition:" << partition_id << ",caz " << result.ToString();
    return -2;
  }

  if (response.type() == client::Type::SYNC) {
    if (response.code() == client::StatusCode::kOk) {
      DLOG(INFO) << "TrySync receive ok, Partition:" << partition_id;;
    } else if (response.code() == client::StatusCode::kWait) {
      LOG(INFO) << "TrySync receive kWait, Partition:" << partition_id;
      return -1;
    } else {
      LOG(WARNING) << "TrySync receive error, Partition:" << partition_id;
      return -2;
    }
  } else {
    LOG(WARNING) << "TrySync recv error type reponse, Partition:" << partition_id;
    return -2;
  }
  return 0;
}

pink::PbCli* ZPTrySyncThread::GetConnection(const Node& node) {
  std::string ip_port = slash::IpPortString(node.ip, node.port);
  pink::PbCli* cli;
  auto iter = client_pool_.find(ip_port);
  if (iter == client_pool_.end()) {
    cli = new pink::PbCli();
    cli->set_connect_timeout(1500);
    pink::Status s = cli->Connect(node.ip, node.port);
    if (!s.ok()) {
      LOG(WARNING) << "Connect failed caz" << s.ToString();
      return NULL;
    }
    client_pool_[ip_port] = cli;
  } else {
    cli = iter->second;
  }
  return cli;
}

void ZPTrySyncThread::DropConnection(const Node& node) {
  std::string ip_port = slash::IpPortString(node.ip, node.port);
  auto iter = client_pool_.find(ip_port);
  if (iter != client_pool_.end()) {
    pink::PbCli* cli = iter->second;
    delete cli;
    client_pool_.erase(iter);
  }
}

/*
 * Return false if one more trysync is needed
 */
bool ZPTrySyncThread::SendTrySync(Partition *partition) {
  if (partition->ShouldWaitDBSync()) {
    if (!partition->TryUpdateMasterOffset()) {
      return false;
    }
    partition->WaitDBSyncDone();
    RsyncUnref();
    LOG(INFO) << "Success Update Master Offset for Partition " << partition->partition_id();
  }

  if (!partition->ShouldTrySync()) {
    return true;
  }

  Node master_node = partition->master_node();
  DLOG(INFO) << "TrySync will connect(" << partition->partition_id() << "_" << master_node.ip << ":" << master_node.port << ")";
  pink::PbCli* cli = GetConnection(master_node);
  DLOG(INFO) << "TrySync connect(" << partition->partition_id() << "_" << master_node.ip << ":" << master_node.port << ") " << (cli != NULL ? "ok" : "failed");
  if (cli) {
    cli->set_send_timeout(1000);
    cli->set_recv_timeout(1000);

    PrepareRsync(partition);
    RsyncRef();
    // Send && Recv
    if (Send(partition, cli)) {
      int ret = Recv(partition->partition_id(), cli);
      if (ret == 0) {
        RsyncUnref();
        partition->TrySyncDone();
        return true;
      } else if (ret == -1) {
        partition->SetWaitDBSync();
      } else {
        RsyncUnref();
        DropConnection(master_node);
      }
    }
  } else {
    LOG(WARNING) << "TrySyncThread Connect failed(" 
      << partition->partition_id() << "_" << master_node.ip << ":" << master_node.port << ")";
}
  return false;
}

void ZPTrySyncThread::PrepareRsync(Partition *partition) {
  std::string p_sync_path = partition->sync_path() + "/";
  slash::CreatePath(p_sync_path + "kv");
  slash::CreatePath(p_sync_path + "hash");
  slash::CreatePath(p_sync_path + "list");
  slash::CreatePath(p_sync_path + "set");
  slash::CreatePath(p_sync_path + "zset");
}

void ZPTrySyncThread::RsyncRef() {
  assert(rsync_flag_ >= 0);
  // Start Rsync
  if (0 == rsync_flag_) {
    slash::StopRsync(zp_data_server->db_sync_path());
    std::string dbsync_path = zp_data_server->db_sync_path();
    //std::string ip_port = slash::IpPortString(zp_data_server->master_ip(), zp_data_server->master_port());

    // We append the master ip port after module name
    // To make sure only data from current master is received
    //int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port, zp_data_server->local_port() + kPortShiftRsync);
    int ret = slash::StartRsync(dbsync_path, kDBSyncModule,
        zp_data_server->local_ip(), zp_data_server->local_port() + kPortShiftRsync);
    if (0 != ret) {
      LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
    }
    LOG(INFO) << "Finish to start rsync, path:" << dbsync_path;
  }
  rsync_flag_++;
}

void ZPTrySyncThread::RsyncUnref() {
  assert(rsync_flag_ >= 0);
  rsync_flag_--;
  if (0 == rsync_flag_) {
    slash::StopRsync(zp_data_server->db_sync_path());
  }
}
