#include "zp_meta_update_thread.h"
#include "zp_meta.pb.h"
#include "zp_meta_server.h"

extern ZPMetaServer* g_zp_meta_server;

const int UPDATE_RETRY_TIME = 3;

void ZPMetaUpdateThread::DoMetaUpdate(void *p) {
  ZPMetaUpdateThread::ZPMetaUpdateArgs *args = static_cast<ZPMetaUpdateThread::ZPMetaUpdateArgs*>(p);
  ZPMetaUpdateThread *thread = args->thread;
  thread->MetaUpdate(args->ip, args->port, args->op);

  delete args;
}

slash::Status ZPMetaUpdateThread::MetaUpdate(const std::string ip, int port, ZPMetaUpdateOP op) {
  
  // Write floyd
  ZPMeta::Partitions partitions;
  slash::Status s = UpdateFloyd(ip, port, op, partitions);
  if (!s.ok()) {
    return s;
  }

  // Change Clients
  s = UpdateSender(ip, port, op);
  if (!s.ok()) {
    //TODO error log
  }

  // Send Update Message
  SendUpdate(partitions);
  return s;
}

slash::Status ZPMetaUpdateThread::UpdateFloyd(const std::string &ip, int port, ZPMetaUpdateOP op, ZPMeta::Partitions &partitions) {
  // Load from Floyd
  std::string key(ZP_META_KEY_PREFIX), value;
  key += "1"; //Only one partition now
  slash::Status s = g_zp_meta_server->Get(key, value);
  if (!s.ok()) {
    //TODO error log
    return s;
  }

  // Deserialization
  if (!value.empty()) {
    if (!partitions.ParseFromString(value)) {
      //TODO error log
      return slash::Status::Corruption("Parse failed");
    }
    assert(partitions.id() == 1);
  }

  // Update Partition
  UpdatePartition(partitions, ip, port + 100, op);

  // Serialization and Dump to Floyd
  std::string new_value;
  if (!partitions.SerializeToString(&new_value)) {
    //TODO error log
    return Status::Corruption("Serialize error");
  }
  return g_zp_meta_server->Set(key, new_value);
}

slash::Status ZPMetaUpdateThread::UpdateSender(const std::string &ip, int port, ZPMetaUpdateOP op) {
  std::string ip_port = slash::IpPortString(ip, port);
  slash::Status s;
  if (ZPMetaUpdateOP::OP_ADD == op) {
    if (data_sender_.find(ip_port) == data_sender_.end()) {
      pink::PbCli *pb_cli = new pink::PbCli();
      pink::Status s = pb_cli->Connect(ip, port);
      if (!s.ok()) {
        return slash::Status::Corruption(s.ToString());
      }
      pb_cli->set_send_timeout(1000);
      pb_cli->set_recv_timeout(1000);
      data_sender_.insert(std::pair<std::string, pink::PbCli*>(ip_port, pb_cli));
    }
  } else if (ZPMetaUpdateOP::OP_REMOVE == op) {
    data_sender_.erase(ip_port);
  } else {
      return slash::Status::Corruption("unknown ZPMetaUpdate OP");
  }
  return slash::Status::OK();
}

void ZPMetaUpdateThread::SendUpdate(ZPMeta::Partitions &partitions) {
  ZPMeta::MetaCmd request;
  ZPMeta::MetaCmd_Update *update_cmd = request.mutable_update();
  ZPMeta::Partitions *p = update_cmd->add_info();
  p->CopyFrom(partitions);
  
  ZPMeta::MetaCmdResponse response;

  int retry_num = 0;
  pink::Status s;
  DataCliMap::iterator it = data_sender_.begin();
  for (; it != data_sender_.end();) {
    s = (it->second)->Send(&request);
    if (s.ok()) {
      s = (it->second)->Recv(&response); 
    }
    if (s.ok() &&
        response.type() == ZPMeta::MetaCmdResponse_Type_UPDATE &&
        response.status().code() == 0) {
      //Success
    } else {
      //TODO error log
      if (retry_num < UPDATE_RETRY_TIME) {
        ++retry_num;
      } else {
        retry_num = 0;
      }
    }
    // move next in non-retry loop
    if (retry_num == 0) {
      it++;
    }
  }
}

/*
 * Something indicated by op has happend
 * So we need to Modify the partition information
 */
void ZPMetaUpdateThread::UpdatePartition(ZPMeta::Partitions &partitions,
    const std::string& ip, int port, ZPMetaUpdateOP op) {
  
  // First one
  if (!partitions.IsInitialized()) {
    if (ZPMetaUpdateOP::OP_ADD == op) {
      SetMaster(partitions, ip, port);
    }
    return;
  }

  // Is master
  ZPMeta::Node master = partitions.master();
  if (IsTheOne(master, ip, port)) {
    if (ZPMetaUpdateOP::OP_ADD == op) {
      return;
    }
    assert(ZPMetaUpdateOP::OP_REMOVE == op);
    partitions.clear_master();
    if (partitions.slaves_size() > 0) { 
      const ZPMeta::Node& last = partitions.slaves(partitions.slaves_size() - 1);
      partitions.mutable_slaves()->RemoveLast();
      SetMaster(partitions, last.ip(), last.port());
    }
    return;
  }

  // Is slave
  for (int i = 0; i < partitions.slaves_size(); ++i) {
    const ZPMeta::Node& node = partitions.slaves(i);
    if (!IsTheOne(node, ip, port)) {
      continue;
    }
    if (ZPMetaUpdateOP::OP_REMOVE == op) {
      // Move the one to last
      if (i != (partitions.slaves_size() - 1)) 
      {
        const ZPMeta::Node& last = partitions.slaves(partitions.slaves_size() - 1);
        ZPMeta::Node* to_remove = partitions.mutable_slaves(i);
        to_remove->CopyFrom(last);
      }
      partitions.mutable_slaves()->RemoveLast();
    }
    return;
  }

  // New salve
  ZPMeta::Node* new_slave = partitions.add_slaves();
  new_slave->set_ip(ip);
  new_slave->set_port(port);
}

