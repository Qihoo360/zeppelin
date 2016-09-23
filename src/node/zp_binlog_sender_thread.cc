#include "zp_binlog_sender_thread.h"

#include <glog/logging.h>
#include <poll.h>

#include "zp_data_server.h"
#include "zp_const.h"
#include "pb_cli.h"

using slash::Status;
using slash::Slice;
using pink::PbCli;

extern ZPDataServer* zp_data_server;

ZPBinlogSenderThread::ZPBinlogSenderThread(Partition *partition, const std::string &ip, int port, slash::SequentialFile *queue, uint32_t filenum, uint64_t con_offset)
  : partition_(partition),
  ip_(ip),
  port_(port),
  con_offset_(con_offset),
  filenum_(filenum),
  initial_offset_(0),
  end_of_buffer_offset_(kBlockSize),
  queue_(queue),
  backing_store_(new char[kBlockSize]),
  buffer_() {
    last_record_offset_ = con_offset % kBlockSize;
    pthread_rwlock_init(&rwlock_, NULL);
  }

ZPBinlogSenderThread::~ZPBinlogSenderThread() {
  should_exit_ = true;

  pthread_join(thread_id(), NULL);

  delete queue_;
  pthread_rwlock_destroy(&rwlock_);
  delete [] backing_store_;

  LOG(INFO) << "a BinlogSender thread " << thread_id() << " exit!";
}

int ZPBinlogSenderThread::trim() {
  slash::Status s;
  uint64_t start_block = (con_offset_ / kBlockSize) * kBlockSize;
  s = queue_->Skip((con_offset_ / kBlockSize) * kBlockSize);
  uint64_t block_offset = con_offset_ % kBlockSize;
  uint64_t ret = 0;
  uint64_t res = 0;
  bool is_error = false;

  while (true) {
    if (res >= block_offset) {
      con_offset_ = start_block + res;
      break;
    }
    ret = get_next(is_error);
    if (is_error == true) {
      return -1;
    }
    res += ret;
  }
  last_record_offset_ = con_offset_ % kBlockSize;

  return 0;
}

uint64_t ZPBinlogSenderThread::get_next(bool &is_error) {
  uint64_t offset = 0;
  slash::Status s;
  is_error = false;

  while (true) {
    buffer_.clear();
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
    if (!s.ok()) {
      is_error = true;
    }

    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[3];
    const uint32_t length = a | (b << 8) | (c << 16);

    if (type == kFullType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
      break;
    } else if (type == kFirstType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
    } else if (type == kMiddleType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
    } else if (type == kLastType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
      break;
    } else {
      is_error = true;
      break;
    }
  }
  return offset;
}

unsigned int ZPBinlogSenderThread::ReadPhysicalRecord(slash::Slice *result) {
  slash::Status s;
  if (end_of_buffer_offset_ - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(end_of_buffer_offset_ - last_record_offset_);
    con_offset_ += (end_of_buffer_offset_ - last_record_offset_);
    last_record_offset_ = 0;
  }
  buffer_.clear();
  s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kBadRecord;
  }

  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[3];
  const uint32_t length = a | (b << 8) | (c << 16);
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  //std::cout<<"2 --> con_offset_: "<<con_offset_<<" last_record_offset_: "<<last_record_offset_<<std::endl;
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    con_offset_ += (kHeaderSize + length);
  }
  return type;
}

Status ZPBinlogSenderThread::Consume(std::string &scratch) {
  Status s;
  if (last_record_offset_ < initial_offset_) {
    return slash::Status::IOError("last_record_offset exceed");
  }

  slash::Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    switch (record_type) {
      case kFullType:
        scratch = std::string(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kFirstType:
        scratch.assign(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kMiddleType:
        scratch.append(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kLastType:
        scratch.append(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kEof:
        return Status::EndFile("Eof");
      case kBadRecord:
        return Status::IOError("Data Corruption");
      case kOldRecord:
        return Status::EndFile("Eof");
      default:
        return Status::IOError("Unknow reason");
    }
    // TODO:do handler here
    if (s.ok()) {
      break;
    }
  }
  //DLOG(INFO) << "Binlog Sender consumer a msg: " << scratch;
  return Status::OK();
}

// Get a whole message; 
// the status will be OK, IOError or Corruption;
Status ZPBinlogSenderThread::Parse(std::string &scratch) {
  scratch.clear();
  Status s;
  uint32_t pro_num;
  uint64_t pro_offset;

  Binlog* logger = partition_->logger_;
  while (!should_exit_) {
    logger->GetProducerStatus(&pro_num, &pro_offset);
    if (filenum_ == pro_num && con_offset_ == pro_offset) {
      //DLOG(INFO) << "BinlogSender Parse no new msg, filenum_" << filenum_ << ", con_offset " << con_offset_;
      usleep(10000);
      continue;
    }

    //DLOG(INFO) << "BinlogSender start Parse a msg               filenum_" << filenum_ << ", con_offset " << con_offset_;
    s = Consume(scratch);

    //DLOG(INFO) << "BinlogSender after Parse a msg return " << s.ToString() << " filenum_" << filenum_ << ", con_offset " << con_offset_;
    if (s.IsEndFile()) {
      std::string confile = NewFileName(logger->filename, filenum_ + 1);

      // Roll to next File
      if (slash::FileExists(confile)) {
        DLOG(INFO) << "BinlogSender roll to new binlog" << confile;
        delete queue_;
        queue_ = NULL;

        slash::NewSequentialFile(confile, &(queue_));

        filenum_++;
        con_offset_ = 0;
        initial_offset_ = 0;
        end_of_buffer_offset_ = kBlockSize;
        last_record_offset_ = con_offset_ % kBlockSize;
      } else {
        usleep(10000);
      }
    } else {
      break;
    }
  }

  if (should_exit_) {
    return Status::Corruption("should exit");
  }
  return s;
}

uint32_t ZPBinlogSenderThread::ParseMsgCode(std::string* scratch) {
  uint32_t buf;
  memcpy((char *)(&buf), scratch->data() + 4, sizeof(uint32_t));
  uint32_t msg_code = ntohl(buf);
  DLOG(INFO) << "ParseMsgCode msg_code:" << msg_code;
  return msg_code;
}

void* ZPBinlogSenderThread::ThreadMain() {
  std::string scratch;
  scratch.reserve(1024 * 1024);

  Status s;
  bool send_next = true;
  while (!should_exit_) {
    if (send_next) {
      // Parse binglog
      s = Parse(scratch);
      if (!s.ok()) {
        LOG(WARNING) << "BinlogSender Parse error, " << s.ToString();
        usleep(10000);
        continue;
      }
    }
    // Send binlog
    s = zp_data_server->SendToPeer(ip_, port_, scratch);
    //s = zp_data_server->SendToPeer(ip_, port_ + kPortShiftSync, data);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to send to peer " << ip_ << ":" << port_ + kPortShiftSync << ", Error: " << s.ToString();
      send_next = false;
      sleep(1);
      continue;
    }
    send_next = true;
  }

  return NULL;
}
