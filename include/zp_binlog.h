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

#include "zp_const.h"

#include "slash/include/env.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

using slash::Status;
using slash::Slice;

std::string NewFileName(const std::string& name, uint32_t current);

// Find the nearest block start offset
uint64_t BinlogBlockStart(uint64_t offset);

enum RecordType {
  kZeroType = 0,
  kFullType = 1,
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
  kEof = 5,
  kBadRecord = 6,
  kEmptyType = 7,
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

  void Save(uint32_t num, uint64_t offset);
  void Fetch(uint32_t *num, uint64_t *offset);
  void Inc(uint64_t go_head);

  void Debug();


 private:
  pthread_rwlock_t rwlock_;
  uint32_t pro_num_;
  uint64_t pro_offset_;

  slash::RWFile *save_;

  // StableSave and StableLoad Should hold write lock on rwlock_
  void StableSave();
  Status StableLoad();

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
  Status Fallback(uint64_t offset);
  Status Produce(const Slice &item, int64_t *write_size);
  Status AppendBlank(uint64_t len, int64_t* write_size);

private:
  slash::WritableFile *queue_;
  int block_offset_;
  Status EmitPhysicalRecord(RecordType t,
      const char *ptr, size_t n, int64_t *write_size);
  void Load();

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
  void SkipNextBlock(uint64_t* size);

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
  Status PutBlank(uint64_t len);

  void GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset) {
    slash::MutexLock l(&mutex_);
    version_->Fetch(filenum, pro_offset);
  }
  Status SetProducerStatus(uint32_t pro_num, uint64_t pro_offset,
      uint64_t* actual_offset, uint32_t* cur_num, uint64_t* cur_offset,
      uint32_t* start_num);

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
  void MaybeRoll();
  Status RemoveBetween(int lbound, int rbound);
  
  
  // No copying allowed
  Binlog(const Binlog&);
  void operator=(const Binlog&);

};

#endif
