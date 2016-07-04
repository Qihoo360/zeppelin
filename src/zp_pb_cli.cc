#include "zp_pb_cli.h"

#include <glog/logging.h>

void ZPPbCli::BuildWbuf() {
  uint32_t len;
  wbuf_len_ = msg_->ByteSize();
  len = htonl(wbuf_len_ + 4);
  memcpy(wbuf_, &len, sizeof(uint32_t));
  len = htonl(opcode_);
  memcpy(wbuf_ + 4, &len, sizeof(uint32_t));
  msg_->SerializeToArray(wbuf_ + 8, wbuf_len_);
  DLOG(INFO) << "ZPPbCli Wbuf [ wbuf_len(" << wbuf_len_ << ") | opcode(" << opcode_ << ") en_opcode(" << len << ") | msg ]";
  wbuf_len_ += 8;
}

pink::Status ZPPbCli::SendRaw(const void *msg, size_t size) {
  DLOG(INFO) << "SendRaw with buffer size:" << size;

  pink::Status s;

  //void *buf = msg;
  size_t pos = 0;
  size_t nleft = size;
  ssize_t nwritten;
  while (nleft > 0) {
    if ((nwritten = write(fd(), msg + pos, nleft)) <= 0) {
      if (errno == EINTR) {
        nwritten = 0;
        continue;
      } else {
        s = pink::Status::IOError("write error: " + std::string(strerror(errno)));
        return s;
      }
    }
    nleft -= nwritten;
    pos += nwritten;
  }

  return s;
}
