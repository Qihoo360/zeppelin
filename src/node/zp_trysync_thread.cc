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
    bg_thread_->set_thread_name("ZPDataTrySync");
}

ZPTrySyncThread::~ZPTrySyncThread() {
  should_exit_ = true;
  delete bg_thread_;
  for (auto &kv: client_pool_) {
    kv.second->Close();
    delete kv.second;
  }
  slash::StopRsync(zp_data_server->db_sync_path());
  LOG(INFO) << " TrySync thread " << pthread_self() << " exit!!!";
}

void ZPTrySyncThread::TrySyncTaskSchedule(const std::string& table, int partition_id) {
  slash::MutexLock l(&bg_thread_protector_);
  bg_thread_->StartIfNeed();
  TrySyncTaskArg *targ = new TrySyncTaskArg(this, table, partition_id);
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

  // Do try sync
  if (!SendTrySync(table_name, partition_id)) {
    // Need one more trysync, since error happenning or waiting for db sync
    sleep(kTrySyncInterval);
    LOG(WARNING) << "SendTrySync ReSchedule for table:" << table_name
      << ", partition:" << partition_id;
    zp_data_server->AddSyncTask(table_name, partition_id);
  }
}

bool ZPTrySyncThread::Send(std::shared_ptr<Partition> partition, pink::PbCli* cli) {
  // Generate Request 
  client::CmdRequest request;
  client::CmdRequest_Sync* sync = request.mutable_sync();
  request.set_type(client::Type::SYNC);
  client::Node* node = sync->mutable_node();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());

  uint32_t filenum = 0;
  uint64_t offset = 0;
  partition->GetBinlogOffset(&filenum, &offset);
  sync->set_table_name(partition->table_name());
  client::SyncOffset *sync_offset = sync->mutable_sync_offset();
  sync_offset->set_partition(partition->partition_id());
  sync_offset->set_filenum(filenum);
  sync_offset->set_offset(offset);

  // Send through client
  pink::Status s = cli->Send(&request);
  DLOG(INFO) << "TrySync: Partition " << partition->table_name() << "_"
    << partition->partition_id() << " with SyncPoint ("
    << sync->node().ip() << ":" << sync->node().port()
    << ", " << filenum << ", " << offset << ")";
  if (!s.ok()) {
    LOG(WARNING) << "TrySync send failed, Partition " << partition->table_name()
      << "_" << partition->partition_id() << ", caz " << s.ToString();
    return false;
  }
  return true;
}

bool ZPTrySyncThread::Recv(std::shared_ptr<Partition> partition, pink::PbCli* cli, RecvResult* res) {
  // Recv from client
  client::CmdResponse response;
  pink::Status result = cli->Recv(&response); 
  if (!result.ok()) {
    LOG(WARNING) << "TrySync recv failed, Partition:"
      << partition->partition_id() << ", caz " << result.ToString();
    return false;
  }

  res->code = response.code();
  res->message = response.msg();
  if (response.type() != client::Type::SYNC) {
    LOG(WARNING) << "TrySync recv error type reponse, Partition:"
      << partition->partition_id();
    return false;
  }
  if (response.has_sync())  {
    res->filenum = response.sync().sync_offset().filenum();
    res->offset = response.sync().sync_offset().offset();
  }
  return true;
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
      delete cli;
      return NULL;
    }
    cli->set_send_timeout(1000);
    cli->set_recv_timeout(1000);
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
    cli->Close();
    delete cli;
    client_pool_.erase(iter);
  }
}

/*
 * Return false if one more trysync is needed
 */
bool ZPTrySyncThread::SendTrySync(const std::string& table_name, int partition_id) {
  std::shared_ptr<Partition> partition =
    zp_data_server->GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    // Partition maybe deleted, no need to rescheule again
    return true;
  }

  if (partition->ShouldWaitDBSync()) {
    if (!partition->TryUpdateMasterOffset()) {
      return false;
    }
    partition->WaitDBSyncDone();
    RsyncUnref();
    LOG(INFO) << "Success Update Master Offset for Partition "
      << partition->table_name() << "_" << partition->partition_id();
  }

  if (!partition->ShouldTrySync()) {
    // Return true so that the trysync will not be reschedule
    return true;
  }

  Node master_node = partition->master_node();
  pink::PbCli* cli = GetConnection(master_node);
  DLOG(INFO) << "TrySync connect(" << partition->table_name() << "_"
    << partition->partition_id() << "_" << master_node.ip << ":"
    << master_node.port << ") " << (cli != NULL ? "ok" : "failed");
  if (cli) {
    cli->set_send_timeout(1000);
    cli->set_recv_timeout(1000);

    slash::CreatePath(partition->sync_path());
    RsyncRef();
    // Send && Recv
    if (Send(partition, cli)) {
      Status s;
      RecvResult res;
      if (Recv(partition, cli, &res)) {
        switch (res.code) {
          case client::StatusCode::kOk:
            partition->TrySyncDone();
            RsyncUnref();
            return true;
          case client::StatusCode::kFallback:
            LOG(INFO) << "Receive sync offset fallback to : "
              << res.filenum << "_" << res.offset;
            s = partition->SetBinlogOffset(res.filenum, res.offset);
            if (!s.ok()) {
              LOG(WARNING) << "Set sync offset fallback to : "
                << res.filenum << "_" << res.offset
                << ", Faliled: " << s.ToString();
            }
            break;
          case client::StatusCode::kWait:
            LOG(INFO) << "Receive wait dbsync wait";
            RsyncRef(); // Keep the rsync deamon for sync file receive
            partition->SetWaitDBSync();
            break;
          default:
            LOG(WARNING) << "TrySyncThread failed, " 
              << partition->table_name() << "_" << partition->partition_id()
              << "_" << master_node.ip << ":" << master_node.port << "), Msg: "
              << res.message;
        }
      } else {
        LOG(WARNING) << "TrySyncThread Recv failed, " 
          << partition->table_name() << "_" << partition->partition_id()
          << "_" << master_node.ip << ":" << master_node.port << ")";
        DropConnection(master_node);
      }
    } else {
      LOG(WARNING) << "TrySyncThread Send failed, " 
        << partition->table_name() << "_" << partition->partition_id()
        << "_" << master_node.ip << ":" << master_node.port << ")";
      DropConnection(master_node);
    }
    RsyncUnref();
  } else {
    LOG(WARNING) << "TrySyncThread Connect failed (" 
      << partition->table_name() << "_" << partition->partition_id()
      << "_" << master_node.ip << ":" << master_node.port << ")";
  }
  return false;
}

void ZPTrySyncThread::RsyncRef() {
  assert(rsync_flag_ >= 0);
  // Start Rsync
  if (0 == rsync_flag_) {
    slash::StopRsync(zp_data_server->db_sync_path());
    std::string dbsync_path = zp_data_server->db_sync_path();

    // We append the master ip port after module name
    // To make sure only data from current master is received
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
