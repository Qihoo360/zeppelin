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
#include "src/meta/zp_meta_client_conn.h"
#include <glog/logging.h>
#include <vector>
#include <string>
#include <algorithm>
#include "src/meta/zp_meta_server.h"

extern ZPMetaServer* g_meta_server;

////// ZPDataClientConn ///// /
ZPMetaClientConn::ZPMetaClientConn(int fd, const std::string& ip_port,
    pink::ServerThread* server_thread)
  : PbConn(fd, ip_port, server_thread) {
}

ZPMetaClientConn::~ZPMetaClientConn() {
}

// Msg is  [ length (int32) | pb_msg (length bytes) ]
int ZPMetaClientConn::DealMessage() {
  response_.Clear();
  if (!request_.ParseFromArray(rbuf_ + 4, header_len_)) {
    LOG(INFO) << "DealMessage, Invalid pb message";
    return -1;
  }

  Cmd* cmd = g_meta_server->GetCmd(static_cast<int>(request_.type()));
  if (cmd == NULL) {
    response_.set_type(request_.type());
    response_.set_code(ZPMeta::StatusCode::ERROR);
    response_.set_msg("Unknown command");
    res_ = &response_;
    return -1;
  }

  // Redirect to leader if needed
  set_is_reply(true);

  // Server ensure leader has been elect here
  slash::MutexLock l(&(g_meta_server->leader_mutex));
  if (cmd->is_redirect()
      && !g_meta_server->IsLeader()) {
    Status s = g_meta_server->RedirectToLeader(request_, &response_);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to redirect to leader : " << s.ToString();
      response_.set_type(request_.type());
      response_.set_code(ZPMeta::StatusCode::ERROR);
      response_.set_msg(s.ToString());
      res_ = &response_;
      return -1;
    }
    res_ = &response_;
    return 0;
  }

  g_meta_server->PlusQueryNum();

  cmd->Do(&request_, &response_);
  res_ = &response_;
  return 0;
}
