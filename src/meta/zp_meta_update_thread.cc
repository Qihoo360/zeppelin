#include "zp_meta_update_thread.h"
#include "zp_mete.pb.h"
#include "zp_meta_server.h"

extern ZPMetaServer* g_zp_meta_server;

const int UPDATE_RETRY_TIME = 3;
const int NODE_ALIVE_LEASE = 3;

void ZPMetaUpdateThread::DoMetaUpdate(void *p) {
  ZPMetaUpdateArgs *args = static_cast<ZPMetaUpdateArgs*>(p);
  ZPMetaUpdateThread thread = args->thread;
  thread.MetaUpdate(args->ip, args->port, args->op);

  delete args;
}

Status ZPMetaUpdateThread::MetaUpdate(const std::string ip, int port, ZPMetaUpdateOP op) {
  
  // Write floyd
  ZPMeta::Partitions partitions;
  Status s = UpdateFloyd(ip, port, op, partitions);
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

Status ZPMetaUpdateThread::UpdateFloyd(const std::string &ip, int port, ZPMetaUpdateOP op, ZPMeta::Partitions &partitions) {
  // Load from Floyd
  std::string key(ZP_META_KEY_PREFIX), value;
  key += "1"; //Only one partition now
  Status s = g_zp_meta_server->Get(key, value);
  if (!s.ok()) {
    //TODO error log
    return s;
  }

  // Deserialization
  if (!value.empty()) {
    if (!meta_map.ParseFromString(value)) {
      //TODO error log
      return Status::Corruption("Parse failed");
    }
    assert(partitions.id() == 1);
  }

  // Update Partition
  UpdatePartition(partitions, ip, port + 100, op);

  // Serialization and Dump to Floyd
  std::string new_value;
  s = partitions.SerializeToString(new_value);
  if (!s.ok()) {
    //TODO error log
    return s;
  }
  return g_zp_meta_server->Set(key, new_value);
}

Status ZPMetaUpdateThread::UpdateSender(const std::string &ip, int port, ZPMetaUpdateOP op) {
  std::string ip_port = slash::IpPortString(ip, port);
  if (ZPMetaUpdateOP::OP_ADD == op) {
    if (data_sender_.find(ip_port) == data_sender_.end()) {
      PbCli *pb_cli = new PbCli();
      s = pb_cli->Connect(ip, port);
      if (!s.ok()) {
        return s;
      }
      pb_cli_->set_send_timeout(1000);
      pb_cli_->set_recv_timeout(1000);
      data_sender_.insert(std::pair<std::string, pink::PbCli*>(ip_port, pb_cli));
    }
  } else if (ZPMetaUpdateOP::OP_REMOVE == op) {
    data_sender_.erase(ip_port);
  } else {
      return Status::Corruption("unknown ZPMetaUpdate OP");
  }
}

void ZPMetaUpdateThread::SendUpdate(ZPMeta::Partitions &Partitions) {
  ZPMeta::MetaCmd request;
  ZPMeta::MetaCmd_Update *update_cmd = request.mutable_update();
  ZPMeta::Partitions *p = update_cmd->add_info();
  p->CopyFrom(partitions);
  
  ZPMeta::MetaCmdResponse response;

  int retry_num = 0;
  DataCliMap::Iterator it = data_sender_.begin();
  for (; it != data_sender_.end(); ++it) {
    s = (it->second)->Send(&request);
    if (s.ok()) {
      s = (it->second)->Recv(&response); 
    }
    if (s.ok() &&
        response.type() == ZPMeta::MetaCmdResponse_Type_UPDATE &&
        response.status() == 0) {
      //Success
      retry_num = 0;
    } else {
      //TODO error log
      if (retry_num < UPDATE_RETRY_TIME) {
        ++retry_num;
        --it;
      }
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
    const ZPMeta::Node& last = partitions.slaves(partitions.size() - 1);
    partitions.mutable_slaves()->RemoveLast();
    SetMaster(partitions, last.ip(), last.port());
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
      if (i != (partitions.slave_size() - 1)) 
      {
        const ZPMeta::Node& last = partitions.slaves(partitions.size() - 1);
        ZPMeta::Node* to_remove = partitions.mutable_slaves(i);
        to_remove.CopyFrom(last);
      }
      partitions.mutable_slaves()->RemoveLast();
    }
    return;
  }

  // New salve
  ZPMeta::Node* new_slave = partitions.add_slaves();
  new_slave.set_ip(ip);
  new_slave.set_port(port);
}

inline void ZPMetaUpdateThread::SetMaster(ZPMeta::Partitions &partitions, const std::string &ip, int port) {
  ZPMeta::Node *master = partitions.mutable_master();
  master->set_ip(ip);
  master->set_port(port);
}

inline bool ZPMetaUpdateThread::IsTheOne(const ZPMeta::Node &node, const std::string &ip, int port) {
  return (node.ip() == ip && node.port() == port);
}



