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
#include "src/node/zp_metacmd_bgworker.h"
#include <string.h>
#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <set>
#include <string>
#include <memory>

#include "src/node/zp_data_server.h"
#include "include/zp_command.h"

extern ZPDataServer* zp_data_server;

ZPMetacmdBGWorker::ZPMetacmdBGWorker() {
    cli_ = pink::NewPbCli();
    cli_->set_connect_timeout(1500);
    bg_thread_ = new pink::BGThread();
  }

ZPMetacmdBGWorker::~ZPMetacmdBGWorker() {
  bg_thread_->StopThread();
  delete bg_thread_;
  cli_->Close();
  delete cli_;
  LOG(INFO) << "ZPMetacmd thread " << bg_thread_->thread_id() << " exit!!!";
}

void ZPMetacmdBGWorker::AddTask() {
  bg_thread_->StartThread();
  bg_thread_->Schedule(&MetaUpdateTask, static_cast<void*>(this));
}

void ZPMetacmdBGWorker::MetaUpdateTask(void* task) {
  ZPMetacmdBGWorker* worker =  static_cast<ZPMetacmdBGWorker*>(task);
  int64_t receive_epoch = 0;
  if (!zp_data_server->ShouldPullMeta()) {
    // Avoid multiple invalid Pull
    return;
  }

  if (worker->FetchMetaInfo(&receive_epoch)) {
    // When we fetch OK, we will FinishPullMeta
    zp_data_server->FinishPullMeta(receive_epoch);
  } else {
    // Sleep and try again
    sleep(kMetacmdInterval);
    zp_data_server->AddMetacmdTask();
  }
}

Status ZPMetacmdBGWorker::Send() {
  ZPMeta::MetaCmd request;

  LOG(INFO) << "MetacmdThread send pull to MetaServer("
    << zp_data_server->meta_ip() << ":"
    << zp_data_server->meta_port() + kMetaPortShiftCmd
    << ") with local("<< zp_data_server->local_ip() << ":"
    << zp_data_server->local_port() << ")";

  request.set_type(ZPMeta::Type::PULL);
  ZPMeta::MetaCmd_Pull* pull = request.mutable_pull();
  ZPMeta::Node* node = pull->mutable_node();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());

  std::string text_format;
  google::protobuf::TextFormat::PrintToString(request, &text_format);
  LOG(INFO) << "MetacmdThread send pull: [" << text_format << "]";

  return cli_->Send(&request);
}

Status ZPMetacmdBGWorker::Recv(int64_t* receive_epoch) {
  Status result;
  ZPMeta::MetaCmdResponse response;
  std::string meta_ip = zp_data_server->meta_ip();
  int meta_port = zp_data_server->meta_port() + kMetaPortShiftCmd;
  result = cli_->Recv(&response);
  if (result.ok()) {
    std::string text_format;
    google::protobuf::TextFormat::PrintToString(response, &text_format);
    LOG(INFO) << "Receive from meta(" << meta_ip << ":" << meta_port
      << "), size: " << response.pull().info().size()
      << " Response:[" << text_format << "]";

    switch (response.type()) {
      case ZPMeta::Type::PULL:
        return ParsePullResponse(response, receive_epoch);
        break;
      default:
        break;
    }
  }
  return result;
}

Status ZPMetacmdBGWorker::ParsePullResponse(
    const ZPMeta::MetaCmdResponse &response, int64_t* receive_epoch) {
  if (response.code() != ZPMeta::StatusCode::OK) {
    return Status::IOError(response.msg());
  }

  *receive_epoch = response.pull().version();
  int64_t current_epoch = zp_data_server->meta_epoch();
  if (*receive_epoch <= current_epoch) {
    // May already finished
    LOG(WARNING) << "Recv meta epoch isn't larger than mine, recv: "
      << *receive_epoch << ", mine: " << current_epoch;
    return Status::OK();
  }

  ZPMeta::MetaCmdResponse_Pull pull = response.pull();

  LOG(INFO) << "Receive Pull message, new epoch: " << pull.version()
    << ", will handle " << pull.info_size() << " tables.";
  std::set<std::string> miss_tables;  // response for before but not any more
  zp_data_server->GetAllTableName(&miss_tables);

  for (int i = 0; i < pull.info_size(); i++) {
    const ZPMeta::Table& table_info = pull.info(i);
    if (table_info.name().empty()) {
      continue;  // table name not null
    }
    DLOG(INFO) << " - handle Table " << table_info.name();

    // Record tables no longer response for
    miss_tables.erase(table_info.name());

    // Add or Update table info
    std::shared_ptr<Table> table
      = zp_data_server->GetOrAddTable(table_info.name());
    assert(table != NULL);

    table->SetPartitionCount(table_info.partitions_size());
    for (int j = 0; j < table_info.partitions_size(); j++) {
      const ZPMeta::Partitions& partition = table_info.partitions(j);
      DLOG(INFO) << " - - handle Partition " << partition.id()
        << ": master is " << partition.master().ip()
        << ":" << partition.master().port();

      Node master_node(partition.master().ip(), partition.master().port());
      if (master_node.empty()) {
        // No master patitions, simply ignore
        continue;
      }
      std::set<Node> slave_nodes;
      for (int j = 0; j < partition.slaves_size(); j++) {
        slave_nodes.insert(Node(partition.slaves(j).ip(),
              partition.slaves(j).port()));
      }

      bool result = table->UpdateOrAddPartition(partition.id(),
          partition.state(), master_node, slave_nodes);
      if (!result) {
        LOG(WARNING) << "Failed to AddPartition "
          << table_info.name() << "_" << partition.id()
          << ", State: " << static_cast<int>(partition.state())
          << ", partition master is " << partition.master().ip()
          << ":" << partition.master().port();
      }
    }
  }

  // Delete expired tables
  for (auto miss : miss_tables) {
    LOG(WARNING) << "ZPMetaCmd delete expired table after recv pull: " << miss;
    zp_data_server->DeleteTable(miss);
  }

  // Print partitioin info
  zp_data_server->DumpTablePartitions();
  return Status::OK();
}

bool ZPMetacmdBGWorker::FetchMetaInfo(int64_t* receive_epoch) {
  std::string meta_ip = zp_data_server->meta_ip();
  int meta_port = zp_data_server->meta_port() + kMetaPortShiftCmd;
  // No more PickMeta, which should be done by ping thread
  assert(!zp_data_server->meta_ip().empty()
      && zp_data_server->meta_port() != 0);
  LOG(INFO) << "MetacmdThread will connect ("
    << meta_ip << ":" << meta_port << ")";
  Status s = cli_->Connect(meta_ip, meta_port);
  if (s.ok()) {
    cli_->set_send_timeout(5000);
    cli_->set_recv_timeout(5000);
    LOG(INFO) << "Metacmd connect (" << meta_ip << ":" << meta_port << ") ok!";

    s = Send();
    if (!s.ok()) {
      LOG(WARNING) << "Metacmd send to (" << meta_ip << ":" << meta_port
        << ") failed! caz:" << s.ToString();
      cli_->Close();
      return false;
    }
    LOG(INFO) << "Metacmd send to (" << meta_ip << ":" << meta_port << ") ok";

    s = Recv(receive_epoch);
    if (!s.ok()) {
      LOG(WARNING) << "Metacmd recv from (" << meta_ip << ":" << meta_port
        << ") failed! errno:" << errno << " strerr:" << strerror(errno);
      cli_->Close();
      return false;
    }
    LOG(INFO) << "Metacmd recv from (" << meta_ip << ":" << meta_port
      << ") ok";
    cli_->Close();
    return true;
  } else {
    LOG(WARNING) << "Metacmd connect (" << meta_ip << ":" << meta_port
      << ") failed! caz:" << s.ToString();
    return false;
  }
}
