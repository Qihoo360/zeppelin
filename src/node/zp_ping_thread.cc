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
#include "src/node/zp_ping_thread.h"

#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include "src/node/zp_data_server.h"
#include "include/zp_meta.pb.h"
#include "include/zp_const.h"

extern ZPDataServer* zp_data_server;

ZPPingThread::~ZPPingThread() {
  StopThread();
  delete cli_;
  LOG(INFO) << " Ping thread " << pthread_self() << " exit!!!";
}

/*
 * Try to update last offset, return true if has changed
 */
bool ZPPingThread::CheckOffsetDelta(const std::string table_name,
    int partition_id, const BinlogOffset &new_offset) {
  if (last_offsets_.find(table_name) == last_offsets_.end()  // no such table
      || last_offsets_[table_name].find(partition_id) == last_offsets_[table_name].end()  // no such partition
      || last_offsets_[table_name][partition_id] != new_offset) {  // offset changed
    return true;
  }
  return false;
}

slash::Status ZPPingThread::Send(bool all) {
  if (all) {
    LOG(INFO) << "send all offset in ping";
  }
  ZPMeta::MetaCmd request;
  int64_t meta_epoch = zp_data_server->meta_epoch();
  ZPMeta::MetaCmd_Ping* ping = request.mutable_ping();
  ping->set_version(meta_epoch);
  ZPMeta::Node* node = ping->mutable_node();
  node->set_ip(zp_data_server->local_ip());
  node->set_port(zp_data_server->local_port());
  request.set_type(ZPMeta::Type::PING);

  TablePartitionOffsets all_offset;
  zp_data_server->DumpTableBinlogOffsets("", &all_offset);
  for (auto& item : all_offset) {
    for (auto& p : item.second) {
      if (!all && !CheckOffsetDelta(item.first, p.first, p.second)) {
        // no change happend
        continue;
      }
      ZPMeta::SyncOffset *offset = ping->add_offset();
      offset->set_table_name(item.first);
      offset->set_partition(p.first);
      offset->set_filenum(p.second.filenum);
      offset->set_offset(p.second.offset);
    }
  }

  std::string text_format;
  google::protobuf::TextFormat::PrintToString(request, &text_format);
  DLOG(INFO) << "Ping Meta (" << zp_data_server->meta_ip()
    << ":" << zp_data_server->meta_port() + kMetaPortShiftCmd
    << ") with Epoch: " << meta_epoch
    << " offset content: [" << text_format << "]";

  Status s = cli_->Send(&request);

  // Update last_offsets only when send succ
  if (s.ok()) {
    for (auto& off : ping->offset()) {
      last_offsets_[off.table_name()][off.partition()]
        = BinlogOffset(off.filenum(), off.offset());
    }
  }
  return s;
}

slash::Status ZPPingThread::RecvProc() {
  slash::Status result;
  ZPMeta::MetaCmdResponse response;
  result = cli_->Recv(&response);
  DLOG(INFO) << "Ping Recv from Meta (" << zp_data_server->meta_ip() << ":"
    << zp_data_server->meta_port() + kMetaPortShiftCmd << ")";
  if (!result.ok()) {
    return result;
  }
  if (response.code() != ZPMeta::StatusCode::OK) {
    return slash::Status::Corruption("Receive reponse with error code");
  }
  // StatusCode OK
  if (response.type() == ZPMeta::Type::PING) {
    zp_data_server->TryUpdateEpoch(response.ping().version());
    return slash::Status::OK();
  }
  return slash::Status::Corruption("Receive reponse whose type is not ping");
}

void* ZPPingThread::ThreadMain() {
  struct timeval now, last_interaction;
  slash::Status s;

  while (!should_stop()) {
    zp_data_server->PickMeta();
    std::string meta_ip = zp_data_server->meta_ip();
    int meta_port = zp_data_server->meta_port() + kMetaPortShiftCmd;
    // Connect with heartbeat port
    LOG(INFO) << "Ping will connect ("<< meta_ip << ":" << meta_port << ")";
    s = cli_->Connect(meta_ip, meta_port);
    bool is_first = true;  // First Ping after connect should send full message
    if (s.ok()) {
      DLOG(INFO) << "Ping connect ("<< meta_ip << ":" << meta_port << ") ok!";
      gettimeofday(&now, NULL);
      last_interaction = now;
      last_offsets_.clear();  // should resend full dose offset after reconnect
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);

      // Send && Recv
      while (!should_stop()) {
        gettimeofday(&now, NULL);
        if (now.tv_sec - last_interaction.tv_sec > kNodeMetaTimeoutN) {
          LOG(WARNING) << "Ping meta ("<< meta_ip << ":" << meta_port
            << ") timeout, reconnect!";
          break;
        }
        sleep(kPingInterval);

        // Send ping to meta
        s = Send(is_first);
        if (!s.ok()) {
          LOG(WARNING) << "Ping send to ("<< meta_ip << ":" << meta_port
            << ") failed! caz: " << s.ToString();
          continue;
        }
        if (is_first) {
          is_first = false;
        }
        DLOG(INFO) << "Ping send to ("<< meta_ip << ":" << meta_port
          << ") success!";

        // Recv from meta
        s = RecvProc();
        if (!s.ok()) {
          LOG(WARNING) << "Ping recv from ("<< meta_ip << ":" << meta_port
            << ") failed! caz: " << s.ToString();
          continue;
        }

        gettimeofday(&last_interaction, NULL);
        DLOG(INFO) << "Ping recv from ("<< meta_ip << ":" << meta_port
          << ") success!";
      }

      cli_->Close();
    } else {
      LOG(WARNING) << "Ping connect ("<< meta_ip << ":" << meta_port
        << ") failed!";
    }
    sleep(kPingInterval);
  }
  return NULL;
}
