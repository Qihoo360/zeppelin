#ifndef ZP_BINLOG_H
#define ZP_BINLOG_H

#include <cstdio>
#include <list>
#include <string>
#include <deque>
#include <pthread.h>

#ifndef __STDC_FORMAT_MACROS
# define __STDC_FORMAT_MACROS
# include <inttypes.h>
#endif 

#include "env.h"
#include "slash_status.h"
#include "slash_mutex.h"

using slash::Status;
using slash::Slice;

std::string NewFileName(const std::string name, const uint32_t current);

enum RecordType {
  kZeroType = 0,
  kFullType = 1,
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
  kEof = 5,
  kBadRecord = 6,
};

/**
 * Version
 */
class Version {
 public:
  Version(slash::RWFile *save);
  ~Version();

  uint32_t pro_num() {
    slash::RWLock(&rwlock_, false);
    return pro_num_;
  }

  Status Load();
  void Save(uint32_t num, uint64_t offset);
  void Fetch(uint32_t *num, uint64_t *offset);

  void Debug();


 private:
  pthread_rwlock_t rwlock_;
  uint32_t pro_num_;
  uint64_t pro_offset_;

  slash::RWFile *save_;
  // Should hold write lock on rwlock_
  void StableSave();

  // No copying allowed;
  Version(const Version&);
  void operator=(const Version&);
};



/**
 * BinlogWriter
 */
class BinlogWriter {
public:
  BinlogWriter(slash::WritableFile *queue);
  ~BinlogWriter(); 
  Status Produce(const Slice &item, int64_t *write_size);
  Status AppendBlank(uint64_t len);
  void Load();

private:
  slash::WritableFile *queue_;
  int block_offset_;
  Status EmitPhysicalRecord(RecordType t,
      const char *ptr, size_t n, int64_t *write_size);

  // No copying allowed
  BinlogWriter(const BinlogWriter&);
  void operator=(const BinlogWriter&);
};



/**
 * BinlogReader
 */
class BinlogReader {
public:
  BinlogReader(slash::SequentialFile *queue);
  ~BinlogReader(); 
  Status Seek(uint64_t offset);
  Status Consume(uint64_t *size, std::string *item);

private:
  slash::SequentialFile *queue_;
  char* const backing_store_;
  slash::Slice buffer_;
  int last_record_offset_;
  bool last_error_happened_;
  uint32_t ReadPhysicalRecord(uint64_t *size, slash::Slice *result);

  // No copying allowed
  BinlogReader(const BinlogReader&);
  void operator=(const BinlogReader&);
};


/**
 * Binlog
 */
class Binlog {
public:
  static Status Create(const std::string& binlog_path,
      int file_size, Binlog** bptr);

  Binlog(const std::string& binlog_path, const int file_size = 100 * 1024 * 1024);
  ~Binlog();

  uint64_t file_size() {
    return file_size_;
  }

  std::string filename() {
    return filename_;
  }

  Status Put(const std::string &item);

  void GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset) const {
    version_->Fetch(filenum, pro_offset);
  }
  Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset);

private:
  slash::Mutex mutex_;
  std::string binlog_path_;
  uint64_t file_size_;
  std::string filename_;

  slash::RWFile *manifest_;
  Version* version_;
  slash::WritableFile *queue_;
  BinlogWriter* writer_;

  Status Init();
  
  
  // No copying allowed
  Binlog(const Binlog&);
  void operator=(const Binlog&);

};

#endif
