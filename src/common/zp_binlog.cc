#include "zp_binlog.h"

#include <iostream>
#include <string>
#include <glog/logging.h>

using slash::RWLock;

std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

uint64_t BinlogBlockStart(uint64_t offset) {
  return ((offset / kBlockSize) * kBlockSize);
}

/*
 * Version
 */
Version::Version(slash::RWFile *save)
  : pro_num_(0),
    pro_offset_(0),
    save_(save) {
  pthread_rwlock_init(&rwlock_, NULL);
  assert(save_ != NULL);
  StableLoad();
}

Version::~Version() {
  StableSave();
  pthread_rwlock_destroy(&rwlock_);
}

void Version::Save(uint32_t num, uint64_t offset) {
  slash::RWLock(&rwlock_, true);
  pro_num_ = num;
  pro_offset_ = offset;
  StableSave();
}

void Version::Fetch(uint32_t *num, uint64_t *offset) {
  slash::RWLock(&rwlock_, false);
  *num = pro_num_;
  *offset = pro_offset_;
}

inline void Version::Inc(uint64_t go_head) {
  slash::RWLock(&rwlock_, true);
  pro_offset_ += go_head;
  StableSave();
}

Status Version::StableLoad() {
  Status s;
  if (save_->GetData() != NULL) {
    memcpy((char*)(&pro_num_), save_->GetData(), sizeof(uint32_t));
    memcpy((char*)(&pro_offset_), save_->GetData() + sizeof(uint32_t), sizeof(uint64_t));
    DLOG(INFO) << "Load Binlog Version pro_num"<< pro_num_ << " pro_offset " << pro_offset_;;
    return Status::OK();
  } else {
    return Status::Corruption("Version load error");
  }
}

void Version::StableSave() {
  char *p = save_->GetData();
  memcpy(p, &pro_num_, sizeof(uint32_t));
  p += sizeof(uint32_t);
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  DLOG(INFO) << "Save to Version pro_num "<< pro_num_ << " pro_offset " << pro_offset_;;
}

void Version::Debug() {
  slash::RWLock(&rwlock_, false);
  DLOG(INFO) << "Current pro_num: " << pro_num_
    << " pro_offset: "<< pro_offset_;
}


/**
 * BinlogWriter
 */
BinlogWriter::BinlogWriter(slash::WritableFile *queue)
  :queue_(queue),
  block_offset_(0) {
    Load();
  }

BinlogWriter::~BinlogWriter() {
}

void BinlogWriter::Load() {
  assert(queue_ != NULL);
  uint64_t filesize = queue_->Filesize();
  block_offset_ = filesize % kBlockSize;
}

Status BinlogWriter::Fallback(uint64_t offset) {
  if (offset > queue_->Filesize()) {
    return Status::EndFile("offset beyond file size");
  }
  Status s = queue_->Trim(offset);
  if (s.ok()) {
    Load();
  }
  return s;
}
 
Status BinlogWriter::Produce(const Slice &item, int64_t *write_size) {
  Status s;
  const char *ptr = item.data();
  size_t left = item.size();
  bool begin = true;

  *write_size = 0;
  do {
    const int leftover = static_cast<int>(kBlockSize) - block_offset_;
    assert(leftover >= 0);
    if (static_cast<size_t>(leftover) <= kHeaderSize) {
      if (leftover > 0) {
        queue_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
        *write_size += leftover;
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;
    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, write_size);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);

  return s;
}

Status BinlogWriter::EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int64_t *write_size) {
    Status s;
    assert(n <= 0xffffff);
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);

    char buf[kHeaderSize];

    buf[0] = static_cast<char>(n & 0xff);
    buf[1] = static_cast<char>((n & 0xff00) >> 8);
    buf[2] = static_cast<char>(n >> 16);
    buf[3] = static_cast<char>(t);

    s = queue_->Append(Slice(buf, kHeaderSize));
    if (s.ok()) {
        s = queue_->Append(Slice(ptr, n));
        if (s.ok()) {
            s = queue_->Flush();
        }
    }
    block_offset_ += static_cast<int>(kHeaderSize + n);

    *write_size += kHeaderSize + n;
    return s;
}

Status BinlogWriter::AppendBlank(uint64_t len, int64_t* write_size) {
  Status s;
  if (len < kHeaderSize) {
    return Status::InvalidArgument("Blank len too small");
  }

  size_t left = len - kHeaderSize;

  *write_size = 0;
  char tmp[kBlockSize] = {'\x00'};
  do {
    const int leftover = static_cast<int>(kBlockSize) - block_offset_;
    assert(leftover >= 0);
    if (static_cast<size_t>(leftover) <= kHeaderSize) {
      if (leftover > 0) {
        queue_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
        *write_size += leftover;
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    s = EmitPhysicalRecord(kEmptyType, tmp, fragment_length, write_size);
    left -= fragment_length;
  } while (s.ok() && left > 0);

  return s;
}


/**
 * BinlogReader
 */
BinlogReader::BinlogReader(slash::SequentialFile *queue)
  :queue_(queue),
  backing_store_(new char[kBlockSize]),
  buffer_(),
  last_record_offset_(0) {
  }

BinlogReader::~BinlogReader() {
  delete [] backing_store_;
}

void BinlogReader::SkipNextBlock(uint64_t* size) {
  int leftover = kBlockSize - last_record_offset_;
  queue_->Skip(leftover);
  *size += leftover;
  last_record_offset_ = 0;
}

// Comsume one record to scratch
// size show how many byte moving forward
// Return OK if success
//        Incomplete: miss record begin or end
//        EndFile
//        IOError: data corruption or unknown type
Status BinlogReader::Consume(uint64_t* size, std::string* scratch) {
  assert(size != NULL);

  Status s;
  bool inside_record = false;
  slash::Slice fragment;
  while (true) {
    const uint32_t record_type = ReadPhysicalRecord(size, &fragment);

    switch (record_type) {
      case kFullType:
        if (inside_record) {
          return Status::Incomplete("Not found end item");
        }
        *scratch = std::string(fragment.data(), fragment.size());
        return Status::OK();
      case kFirstType:
        if (inside_record) {
          return Status::Incomplete("Not found end item");
        }
        inside_record = true;
        scratch->assign(fragment.data(), fragment.size());
        break;
      case kMiddleType:
        if (!inside_record) {
          return Status::Incomplete("Not found first item");
        }
        scratch->append(fragment.data(), fragment.size());
        break;
      case kLastType:
        if (!inside_record) {
          return Status::Incomplete("Not found first item");
        }
        scratch->append(fragment.data(), fragment.size());
        return Status::OK();
      case kEof:
        return Status::EndFile("Eof");
      case kBadRecord:
        return Status::IOError("Data Corruption");
      case kEmptyType:
        return Status::Incomplete("Not found whole item");
      default:
        return Status::IOError("Unknow reason");
    }
  }
  return Status::OK();
}

// Seek to the nearest item begin before offset
// pre_item_offset record the nearest item begin
// Seek to a offset larger than the filesize will return Status::EOF
Status BinlogReader::Seek(uint64_t offset) {
  uint64_t start_block = BinlogBlockStart(offset);
  Status s = queue_->Skip(start_block);
  if (!s.ok()) {
    return s;
  }
  int64_t block_offset = offset % kBlockSize;

  while (block_offset > 0) {
    uint64_t size = 0;
    std::string tmp;
    s = Consume(&size, &tmp);
    if (s.ok() || s.IsIncomplete()) {
      // Do nothing
    } else if (s.IsEndFile()) {
      return Status::InvalidArgument("Binlog offset beyond enf of file");
    } else {
      SkipNextBlock(&size);
    }
    block_offset -= size;
  }
  if (block_offset != 0) {
    // offset not availible
    return Status::InvalidArgument("Binlog offset not available");
  }
  return Status::OK();
}

unsigned int BinlogReader::ReadPhysicalRecord(uint64_t *size, slash::Slice *result) {
  slash::Status s;

  int leftover = kBlockSize - last_record_offset_;
  if (leftover <= static_cast<int>(kHeaderSize)) {
    queue_->Skip(leftover);
    *size += leftover;
    last_record_offset_ = 0;
  }

  buffer_.clear();
  // TODO wk slash may return the actual read bytes
  //uint64_t actual_read = 0;
  //s = queue_->Read(kHeaderSize, &buffer_, backing_store_, &actual_read);
  //*size += actual_read;
  s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kBadRecord;
  }
  *size += kHeaderSize;
  last_record_offset_ += kHeaderSize;

  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[3];
  const uint32_t length = a | (b << 8) | (c << 16);

  buffer_.clear();
  //s = queue_->Read(length, &buffer_, backing_store_, &actual_read);
  //*size += actual_read;
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kBadRecord;
  }
  *size += length;
  last_record_offset_ += length;

  return type;
}


/*
 * Binlog
 */
Status Binlog::Create(const std::string& binlog_path,
    int file_size, Binlog** bptr) {
  *bptr = NULL;
  Binlog* binlog = new Binlog(binlog_path, file_size);
  Status s = binlog->Init();
  if (s.ok()) {
    *bptr = binlog;
  } else {
    delete binlog;
  }
  return s;
}

Status Binlog::Init() {
  // Create env need
  slash::CreateDir(binlog_path_);
  
  Status s;
  std::string binlog_name;
  const std::string manifest = binlog_path_ + kManifest;
  if (!slash::FileExists(manifest)) {
    // No Manifest file exist, may be the first time
    DLOG(INFO) << "Binlog Manifest file not exist, create a new one.";
    
    // Create Manifest
    s = slash::NewRWFile(manifest, &manifest_);
    if (!s.ok()) {
      LOG(WARNING) << "Failed to create new manifest file:" << s.ToString();
      return s;
    }
    version_ = new Version(manifest_);

    // Create Binlog
    binlog_name = NewFileName(filename_, 0);
    s = slash::NewWritableFile(binlog_name, &queue_);
    if (!s.ok()) {
      LOG(WARNING) << "Failed to create new binlog file: "
        << binlog_name << " " << s.ToString();
      return s;
    }
    writer_ = new BinlogWriter(queue_);

  } else {
    // Manifest exist
    DLOG(INFO) << "Binlog Manifest file exist.";

    // Open Manifest
    s = slash::NewRWFile(manifest, &manifest_);
    if (!s.ok()) {
      LOG(WARNING) << "Failed to open manifest file: " << s.ToString();
      return s;
    }
    version_ = new Version(manifest_);
    //version_->Debug();

    // Open Binlog
    uint32_t file_num = 0;
    uint64_t file_offset = 0;
    version_->Fetch(&file_num, &file_offset);
    binlog_name = NewFileName(filename_, file_num);
    s = slash::AppendWritableFile(binlog_name, &queue_, file_offset);
    if (!s.ok()) {
      LOG(WARNING) << "Failed to open binlog file: "
        << binlog_name << " " << s.ToString();
      return s;
    }
    writer_ = new BinlogWriter(queue_);
  }
  return Status::OK();
}

Binlog::Binlog(const std::string& binlog_path, const int file_size)
  : binlog_path_(binlog_path),
  file_size_(file_size),
  manifest_(NULL),
  version_(NULL),
  queue_(NULL),
  writer_(NULL) {
    if (binlog_path_.back() != '/') {
      binlog_path_.append(1, '/');
    }
    filename_ = binlog_path_ + kBinlogPrefix;
}

Binlog::~Binlog() {
  delete writer_;
  delete queue_;
  delete version_;
  delete manifest_;
}

// Required hold mutex_
void Binlog::MaybeRoll() {
  /* Check to roll log file */
  uint64_t filesize = queue_->Filesize();
  if (filesize > file_size_) {
    delete queue_;
    delete writer_;

    uint32_t pro_num = version_->pro_num() + 1;
    std::string profile = NewFileName(filename_, pro_num);
    slash::NewWritableFile(profile, &queue_);
    writer_ = new BinlogWriter(queue_);
    version_->Save(pro_num, 0);
  }
}

Status Binlog::Put(const std::string &item) {
  slash::MutexLock l(&mutex_);
  MaybeRoll();

  int64_t go_ahead = 0;
  Status s = writer_->Produce(Slice(item.data(), item.size()), &go_ahead);
  version_->Inc(go_ahead);
  if (!s.ok()) {
    LOG(WARNING) << "Binlog write failed: " << s.ToString();
  }
  return s;
}

// Fill binlog with emtpy record whose length is len
Status Binlog::PutBlank(uint64_t len) {
  slash::MutexLock l(&mutex_);
  MaybeRoll();
  
  int64_t go_ahead = 0;
  Status s = writer_->AppendBlank(len, &go_ahead);
  version_->Inc(go_ahead);
  if (!s.ok()) {
    LOG(WARNING) << "Binlog write blank failed: " << s.ToString();
  }
  return s;
}

// Set binlog to point pro_num pro_offset
// Actual offset may small than pro_offset
// since we don't want to append any blank content into binlog
// instead of that, this could be fill by the subsequence sync
Status Binlog::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset,
    uint64_t* actual_offset) {
  slash::MutexLock l(&mutex_);

  // offset smaller than the first header
  if (pro_offset < kHeaderSize) {
    pro_offset = 0;
  }
  
  // Avoid to append any blank content into binlog
  uint32_t cur_num = 0;
  uint64_t cur_offset = 0;
  version_->Fetch(&cur_num, &cur_offset);
  if (cur_num != pro_num) {
    delete queue_;
    delete writer_;

    std::string profile = NewFileName(filename_, pro_num);
    slash::NewWritableFile(profile, &queue_);
    writer_ = new BinlogWriter(queue_);
    cur_offset = 0;
  }
  pro_offset = (pro_offset > cur_offset) ? cur_offset : pro_offset;
  Status s = writer_->Fallback(pro_offset);
  if (s.ok()) {
    version_->Save(pro_num, pro_offset);
  }
  *actual_offset = pro_offset;

  return s;
}
