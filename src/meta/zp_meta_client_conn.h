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
#ifndef SRC_META_ZP_META_CLIENT_CONN_H_
#define SRC_META_ZP_META_CLIENT_CONN_H_

#include <string>
#include "pink/include/pb_conn.h"
#include "pink/include/server_thread.h"
#include "include/zp_meta.pb.h"

class ZPMetaClientConn;
class ZPMetaClientConnFactory;

class ZPMetaClientConn : public pink::PbConn {
 public:
  ZPMetaClientConn(int fd, const std::string& ip_port,
      pink::ServerThread* server_thread);
  virtual ~ZPMetaClientConn();
  virtual int DealMessage();

 private:
  ZPMeta::MetaCmd request_;
  ZPMeta::MetaCmdResponse response_;
};

class ZPMetaClientConnFactory : public pink::ConnFactory {
 public:
  ZPMetaClientConnFactory() {}

  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string &ip_port, pink::ServerThread *server_thread,
      void* worker_private_data) const {
    return new ZPMetaClientConn(connfd, ip_port, server_thread);
  }
};

#endif  // SRC_META_ZP_META_CLIENT_CONN_H_
