#include "zp_binlog.h"

#include <iostream>
#include <string>
#include <glog/logging.h>

#include "zp_const.h"

using slash::RWLock;

std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
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
  StableSave();
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

Status Version::Load() {
  Status s;
  if (save_->GetData() != NULL) {
    memcpy((char*)(&pro_num_), save_->GetData(), sizeof(uint32_t));
    memcpy((char*)(&pro_offset_), save_->GetData() + 4, sizeof(uint64_t));
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
  }

BinlogWriter::~BinlogWriter() {
}

void BinlogWriter::Load() {
  assert(queue_ != NULL);
  uint64_t filesize = queue_->Filesize();
  block_offset_ = filesize % kBlockSize;
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
    if (static_cast<size_t>(leftover) < kHeaderSize) {
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

Status BinlogWriter::AppendBlank(uint64_t len) {
  if (len < kHeaderSize) {
    return Status::OK();
  }

  uint64_t pos = 0;

  std::string blank(kBlockSize, ' ');
  for (; pos + kBlockSize < len; pos += kBlockSize) {
    queue_->Append(Slice(blank.data(), blank.size()));
  }

  // Append a msg which occupy the remain part of the last block
  // We simply increase the remain length to kHeaderSize when remain part < kHeaderSize
  uint32_t n;
  if (len % kBlockSize < kHeaderSize) {
    n = 0;
  } else {
    n = (uint32_t) ((len % kBlockSize) - kHeaderSize);
  }

  char buf[kBlockSize];
  buf[0] = static_cast<char>(n & 0xff);
  buf[1] = static_cast<char>((n & 0xff00) >> 8);
  buf[2] = static_cast<char>(n >> 16);
  buf[3] = static_cast<char>(kFullType);

  Status s = queue_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = queue_->Append(Slice(blank.data(), n));
    if (s.ok()) {
      s = queue_->Flush();
    }
  }
  return s;
}


/**
 * BinlogReader
 */
BinlogReader::BinlogReader(slash::SequentialFile *queue)
  :queue_(queue),
  backing_store_(new char[kBlockSize]),
  buffer_(),
  last_record_offset_(0),
  last_error_happened_(false) {
  }

BinlogReader::~BinlogReader() {
  delete [] backing_store_;
}

Status BinlogReader::Consume(uint64_t* size, std::string& scratch) {
  assert(size != NULL);
  if (last_error_happened_) {
    // BadRecord happend, skip to the next Block
    LOG(WARNING) << "Skip to the next Block since the BadRecord the last time";
    int leftover = kBlockSize - last_record_offset_;
    queue_->Skip(leftover);
    *size += leftover;
    last_record_offset_ = 0;
    last_error_happened_ = false;
  }

  Status s;
  slash::Slice fragment;
  while (true) {
    const uint32_t record_type = ReadPhysicalRecord(size, &fragment);

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
        last_error_happened_ = true;
        return Status::IOError("Data Corruption");
      case kOldRecord:
        return Status::EndFile("Eof");
      default:
        last_error_happened_ = true;
        return Status::IOError("Unknow reason");
    }
    if (s.ok()) {
      break;
    }
  }
  return Status::OK();
}

Status BinlogReader::Seek(uint64_t offset) {
  uint64_t start_block = (offset / kBlockSize) * kBlockSize;
  Status s = queue_->Skip(start_block);
  if (!s.ok()) {
    return s;
  }
  uint64_t block_offset = offset % kBlockSize;

  while (block_offset > 0) {
    uint64_t size = 0;
    std::string tmp;
    s = Consume(&size, tmp);
    if (!s.ok()) {
      return s;
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
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  if (!s.ok()) {
    return kBadRecord;
  }

  last_record_offset_ += kHeaderSize + length;
  *size += kHeaderSize + length;
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
    version_->Load();
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
    writer_->Load();
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

Status Binlog::Put(const std::string &item) {
  slash::MutexLock l(&mutex_);
  Status s;
  /* Check to roll log file */
  uint64_t filesize = queue_->Filesize();
  if (filesize > file_size_) {
    delete queue_;
    delete writer_;

    uint32_t pro_num = version_->pro_num() + 1;
    std::string profile = NewFileName(filename_, pro_num);
    writer_ = new BinlogWriter(queue_);
    slash::NewWritableFile(profile, &queue_);
    writer_->Load();
    version_->Save(pro_num, 0);
  }
 
  int64_t go_ahead = 0;
  s = writer_->Produce(Slice(item.data(), item.size()), &go_ahead);
  if (s.ok()) {
    uint32_t filenum = 0;
    uint64_t offset = 0;
    version_->Fetch(&filenum, &offset);
    version_->Save(filenum, offset + go_ahead);
  }
  return s;
}

Status Binlog::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset) {
  slash::MutexLock l(&mutex_);

  // offset smaller than the first header
  if (pro_offset < 4) {
    pro_offset = 0;
  }

  delete queue_;
  delete writer_;

  std::string init_profile = NewFileName(filename_, 0);
  if (slash::FileExists(init_profile)) {
    slash::DeleteFile(init_profile);
  }

  std::string profile = NewFileName(filename_, pro_num);
  if (slash::FileExists(profile)) {
    slash::DeleteFile(profile);
  }

  slash::NewWritableFile(profile, &queue_);
  writer_ = new BinlogWriter(queue_);
  writer_->AppendBlank(pro_offset);
  writer_->Load();
  version_->Save(pro_num, pro_offset);

  return Status::OK();
}
