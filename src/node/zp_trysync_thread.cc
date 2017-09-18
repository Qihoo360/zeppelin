// Copyright 2017 Qihoo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "src/node/zp_trysync_thread.h"

#include <glog/logging.h>
#include "slash/include/rsync.h"
#include "src/node/zp_data_server.h"
#include "src/node/zp_data_partition.h"

extern ZPDataServer* zp_data_server;

static std::string TablePartitionString(const std::string& table, int id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s_%u", table.c_str(), id);
  return std::string(buf);
}

ZPTrySyncThread::ZPTrySyncThread() {
    bg_thread_ = new pink::BGThread(1024 * 1024 * 256);
    bg_thread_->set_thread_name("ZPDataTrySync");
}

ZPTrySyncThread::~ZPTrySyncThread() {
  bg_thread_->StopThread();
  delete bg_thread_;
  for (auto &kv : client_pool_) {
    kv.second->Close();
    delete kv.second;
  }
  slash::StopRsync(zp_data_server->db_sync_path());
  LOG(INFO) << " TrySync thread " << pthread_self() << " exit!!!";
}

void ZPTrySyncThread::TrySyncTaskSchedule(const std::string& table,
    int partition_id, uint64_t delay) {
  slash::MutexLock l(&bg_thread_protector_);
  bg_thread_->StartThread();
  TrySyncTaskArg *targ = new TrySyncTaskArg(this, table, partition_id);
  if (delay == 0) {  // no delay
    bg_thread_->Schedule(&DoTrySyncTask, static_cast<void*>(targ));
  } else {
    bg_thread_->DelaySchedule(delay, &DoTrySyncTask, static_cast<void*>(targ));
  }
}

void ZPTrySyncThread::DoTrySyncTask(void* arg) {
  TrySyncTaskArg* targ = static_cast<TrySyncTaskArg*>(arg);
  (targ->thread)->TrySyncTask(targ->table_name, targ->partition_id);
  delete targ;
}

void ZPTrySyncThread::TrySyncTask(const std::string& table_name,
    int partition_id) {

  if (!zp_data_server->Availible()  // server is not availible now
      || !SendTrySync(table_name, partition_id)) {
    // Need one more trysync, since error happenning or waiting for db sync
    LOG(WARNING) << "SendTrySync delay " << kTrySyncInterval
      << "(ms) to ReSchedule for table:" << table_name
      << ", partition:" << partition_id
      << ",  meta_epoch:" << zp_data_server->meta_epoch();
    zp_data_server->AddSyncTask(table_name, partition_id, kTrySyncInterval);
  }
}

bool ZPTrySyncThread::Send(std::shared_ptr<Partition> partition,
    pink::PinkCli* cli) {
  // Generate Request
  client::CmdRequest request;
  client::CmdRequest_Sync* sync = request.mutable_sync();
  request.set_type(client::Type::SYNC);
  client::Node* node = sync->mutable_node();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());

  BinlogOffset boffset;
  partition->GetBinlogOffsetWithLock(&boffset);
  sync->set_table_name(partition->table_name());
  client::SyncOffset *sync_offset = sync->mutable_sync_offset();
  sync_offset->set_partition(partition->partition_id());
  sync_offset->set_filenum(boffset.filenum);
  sync_offset->set_offset(boffset.offset);
  int64_t epoch = zp_data_server->meta_epoch();  // just use current epoch
  sync->set_epoch(epoch);

  // Send through client
  slash::Status s = cli->Send(&request);
  LOG(INFO) << "TrySync: Partition " << partition->table_name() << "_"
    << partition->partition_id() << " with SyncPoint ("
    << sync->node().ip() << ":" << sync->node().port()
    << ", " << boffset.filenum << ", " << boffset.offset << ")"
    << ", epoch: " << epoch;
  if (!s.ok()) {
    LOG(WARNING) << "TrySync send failed, Partition "
      << partition->table_name()
      << "_" << partition->partition_id() << ", caz " << s.ToString();
    return false;
  }
  return true;
}

bool ZPTrySyncThread::Recv(std::shared_ptr<Partition> partition,
    pink::PinkCli* cli, RecvResult* res) {
  // Recv from client
  client::CmdResponse response;
  Status result = cli->Recv(&response);
  if (!result.ok()) {
    LOG(WARNING) << "TrySync recv failed, Partition:"
      << partition->table_name() << "_"
      << partition->partition_id() << ", caz " << result.ToString();
    return false;
  }

  res->code = response.code();
  res->message = response.msg();
  if (response.type() != client::Type::SYNC) {
    LOG(WARNING) << "TrySync recv error type reponse, Partition:"
      << partition->table_name() << "_" << partition->partition_id();
    return false;
  }
  if (response.has_sync()) {
    res->filenum = response.sync().sync_offset().filenum();
    res->offset = response.sync().sync_offset().offset();
  }
  return true;
}

pink::PinkCli* ZPTrySyncThread::GetConnection(const Node& node) {
  std::string ip_port = slash::IpPortString(node.ip, node.port);
  pink::PinkCli* cli;
  auto iter = client_pool_.find(ip_port);
  if (iter == client_pool_.end()) {
    cli = pink::NewPbCli();
    cli->set_connect_timeout(1500);
    Status s = cli->Connect(node.ip, node.port);
    if (!s.ok()) {
      LOG(WARNING) << "ZpTrySync Thread connect node: " << ip_port
        <<" failed caz" << s.ToString();
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
    pink::PinkCli* cli = iter->second;
    cli->Close();
    delete cli;
    client_pool_.erase(iter);
  }
}

/*
 * Return false if one more trysync is needed
 */
bool ZPTrySyncThread::SendTrySync(const std::string& table_name,
    int partition_id) {
  std::string index = TablePartitionString(table_name, partition_id);
  std::shared_ptr<Partition> partition =
    zp_data_server->GetTablePartitionById(table_name, partition_id);
  if (!partition
      || !partition->opened()) {
    // Partition maybe deleted or closed, no need to rescheule again
    LOG(INFO) << "SendTrySync closed or deleted Partition "
      << table_name << "_" << partition_id;
    RsyncUnref(index);
    return true;
  }

  if (partition->ShouldWaitDBSync()) {
    LOG(INFO) << " Partition: " << partition->table_name()
      << "_" << partition->partition_id() << " ShouldWaitDBSync.";
    
    if (!partition->TryUpdateMasterOffset()) {
      return false;
    }
    partition->WaitDBSyncDone();
    RsyncUnref(index);
    LOG(INFO) << "Success Update Master Offset for Partition "
      << partition->table_name() << "_" << partition->partition_id();
  }

  if (!partition->ShouldTrySync()) {
    // Return true so that the trysync will not be reschedule
    RsyncUnref(index);
    return true;
  }

  Node master_node = partition->master_node();
  pink::PinkCli* cli = GetConnection(master_node);
  LOG(INFO) << "TrySync connect(" << partition->table_name() << "_"
    << partition->partition_id() << "_" << master_node.ip << ":"
    << master_node.port << ") " << (cli != NULL ? "ok" : "failed");
  if (cli) {
    cli->set_send_timeout(1000);
    cli->set_recv_timeout(1000);

    slash::CreatePath(partition->sync_path());
    RsyncRef(index);
    // Send && Recv
    if (Send(partition, cli)) {
      Status s;
      RecvResult res;
      if (Recv(partition, cli, &res)) {
        switch (res.code) {
          case client::StatusCode::kOk:
            LOG(INFO) << "TrySync ok, Partition: "
              << partition->table_name() << "_" << partition->partition_id();
            partition->TrySyncDone();
            RsyncUnref(index);
            return true;
          case client::StatusCode::kFallback:
            LOG(WARNING) << "Receive sync offset fallback to : "
              << res.filenum << "_" << res.offset << ", Partition: "
              << partition->table_name() << "_" << partition->partition_id()
              << ", back from: " << master_node.ip << ":" << master_node.port;
            s = partition->SetBinlogOffsetWithLock(BinlogOffset(res.filenum,
                  res.offset));
            if (!s.ok()) {
              LOG(WARNING) << "Set sync offset fallback to : "
                << res.filenum << "_" << res.offset << ", Partition: "
                << partition->table_name() << "_" << partition->partition_id()
                << ", Faliled: " << s.ToString();
            }
            break;
          case client::StatusCode::kWait:
            LOG(INFO) << "Receive wait dbsync wait, Partition: "
              << partition->table_name() << "_" << partition->partition_id()
              << ", back from: " << master_node.ip << ":" << master_node.port;
            partition->SetWaitDBSync();
            return false;  // Keep the rsync deamon for sync file receive
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
    RsyncUnref(index);
  } else {
    LOG(WARNING) << "TrySyncThread Connect failed ("
      << partition->table_name() << "_" << partition->partition_id()
      << "_" << master_node.ip << ":" << master_node.port << ")";
  }
  return false;
}

void ZPTrySyncThread::RsyncRef(const std::string& index) {
  // Start Rsync
  if (rsync_ref_.empty()) {
    slash::StopRsync(zp_data_server->db_sync_path());
    std::string dbsync_path = zp_data_server->db_sync_path();

    // We append the master ip port after module name
    // To make sure only data from current master is received
    int ret = slash::StartRsync(dbsync_path, kDBSyncModule,
        zp_data_server->local_ip(),
        zp_data_server->local_port() + kPortShiftRsync);
    if (0 != ret) {
      LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path
        << " error : " << ret;
      return;
    } else {
      LOG(INFO) << "Success start rsync, path:" << dbsync_path;
    }
  }
  
  if (rsync_ref_.find(index) == rsync_ref_.end()) {
    rsync_ref_.insert(index);
  }
}

void ZPTrySyncThread::RsyncUnref(const std::string& index) {
  if (rsync_ref_.empty()) {
    return;
  }

  rsync_ref_.erase(index);
  if (rsync_ref_.empty()) {
    std::string dbsync_path = zp_data_server->db_sync_path();
    int ret = slash::StopRsync(dbsync_path);
    if (0 != ret) {
      LOG(WARNING) << "Failed to stop rsync, path:" << dbsync_path
        << " error : " << ret;
    } else {
      LOG(INFO) << "Success stop rsync, path:" << dbsync_path;
    }
  }
}
