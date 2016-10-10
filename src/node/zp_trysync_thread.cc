#include "zp_trysync_thread.h"
#include <glog/logging.h>
#include "rsync.h"
#include "zp_data_server.h"

extern ZPDataServer* zp_data_server;

ZPTrySyncThread::~ZPTrySyncThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  for (auto &kv: client_pool_) {
    kv.second->Close();
  }
  DLOG(INFO) << " TrySync thread " << pthread_self() << " exit!!!";
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
  DLOG(INFO) << "TrySync: Partition " << partition->partition_id() << " with SyncPoint (" << filenum << ", " << offset << ")";
  DLOG(INFO) << "         Node (" << sync->node().ip() << ":" << sync->node().port() << ")";
  if (!s.ok()) {
    LOG(WARNING) << "TrySync send failed caz " << s.ToString();
    return false;
  }
  return true;
}

int ZPTrySyncThread::Recv(pink::PbCli* cli) {
  client::CmdResponse response;
  pink::Status result = cli->Recv(&response); 

  if (!result.ok()) {
    LOG(WARNING) << "TrySync recv failed " << result.ToString();
    return -2;
  }

  if (response.type() == client::Type::SYNC) {
    if (response.code() == client::StatusCode::kOk) {
      DLOG(INFO) << "TrySync recv success.";
    } else if (response.code() == client::StatusCode::kWait) {
      DLOG(INFO) << "TrySync recv kWait.";
      return -1;
    } else {
      DLOG(INFO) << "TrySync failed caz " << response.msg();
      return -2;
    }
  } else {
    LOG(WARNING) << "TrySync recv error type reponse";
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
      LOG(ERROR) << "Connect failed caz" << s.ToString();
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

void ZPTrySyncThread::SendTrySync(Partition *partition) {
  if (partition->ShouldWaitDBSync() && partition->TryUpdateMasterOffset()) {
    partition->WaitDBSyncDone();
    FlagUnref();
    LOG(INFO) << "Success Update Master Offset for Partition " << partition->partition_id();
  }

  if (!partition->ShouldTrySync()) {
    return;
  }
  PrepareRsync(partition);
  FlagRef();

  Node master_node = partition->master_node();
  DLOG(WARNING) << "TrySync will connect(" << partition->partition_id() << "_" << master_node.ip << ":" << master_node.port << ")";
  pink::PbCli* cli = GetConnection(master_node);
  DLOG(WARNING) << "TrySync connect(" << partition->partition_id() << "_" << master_node.ip << ":" << master_node.port << ") " << (cli != NULL ? "ok" : "failed");
  if (cli) {
    cli->set_send_timeout(1000);
    cli->set_recv_timeout(1000);

    // Send && Recv
    if (Send(partition, cli)) {
      int ret = Recv(cli);
      if (ret == 0) {
        FlagUnref();
        partition->TrySyncDone();
      } else if (ret == -1) {
        partition->SetWaitDBSync();
      } else {
        DropConnection(master_node);
        DLOG(WARNING) << "TrySync recv failed";
      }
    }
  } else {
    LOG(ERROR) << "TrySyncThread Connect failed";
  }
}

void* ZPTrySyncThread::ThreadMain() {
  while (!should_exit_) {
    DLOG(INFO) << "TrySyncThread hold partition_rw ->";
    zp_data_server->WalkPartitions(trysync_functor_);
    DLOG(INFO) << "TrySyncThread release partition_rw <-";
    sleep(kTrySyncInterval);
  }
  return NULL;
}

void ZPTrySyncThread::PrepareRsync(Partition *partition) {
  std::string db_sync_path = partition->sync_path() + "/";
  slash::CreatePath(db_sync_path + "kv");
  slash::CreatePath(db_sync_path + "hash");
  slash::CreatePath(db_sync_path + "list");
  slash::CreatePath(db_sync_path + "set");
  slash::CreatePath(db_sync_path + "zset");
}

void ZPTrySyncThread::FlagRef() {
  assert(rsync_flag_ >= 0);
  // Start Rsync
  if (0 == rsync_flag_) {
    slash::StopRsync(zp_data_server->db_sync_path());
    std::string dbsync_path = zp_data_server->db_sync_path();
    //std::string ip_port = slash::IpPortString(zp_data_server->master_ip(), zp_data_server->master_port());

    // We append the master ip port after module name
    // To make sure only data from current master is received
    //int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port, zp_data_server->local_port() + kPortShiftRsync);
    int ret = slash::StartRsync(dbsync_path, kDBSyncModule, zp_data_server->local_port() + kPortShiftRsync);
    if (0 != ret) {
      LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
    }
    LOG(INFO) << "Finish to start rsync, path:" << dbsync_path;
  }
  rsync_flag_++;
}

void ZPTrySyncThread::FlagUnref() {
  assert(rsync_flag_ >= 0);
  rsync_flag_--;
  if (0 == rsync_flag_) {
    slash::StopRsync(zp_data_server->db_sync_path());
  }
}
void ZPTrySyncFunctor::operator() (std::pair<int, Partition*> partition_pair) {
  trysync_thread_->SendTrySync(partition_pair.second);
}

