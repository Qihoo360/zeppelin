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
#include "src/meta/zp_meta_migrate_register.h"
#include <glog/logging.h>
#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_string.h"

static const char kMigrateHeadKey[] = "##migrate";
std::string DiffKey(const ZPMeta::RelationCmdUnit& diff) {
  return DiffKey(diff.table(), diff.partition(),
      slash::IpPortString(diff.left().ip(), diff.left().port()),
      slash::IpPortString(diff.right().ip(), diff.right().port()));
}

std::string DiffKey(const std::string& table, int partition,
    const std::string& left_ip_port,
    const std::string& right_ip_port) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s_%u_%s_%s",
      table.c_str(), partition,
      left_ip_port.c_str(),
      right_ip_port.c_str());
  return std::string(buf);
}

ZPMetaMigrateRegister::ZPMetaMigrateRegister(floyd::Floyd* f)
  : ctime_(0),
  total_size_(0),
  refer_(0),
  floyd_(f) {
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr,
        PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&migrate_rw_, &attr);
  }

bool ZPMetaMigrateRegister::ExistWithLock() {
  slash::RWLock l(&migrate_rw_, false);
  return ctime_ != 0;
}

// Required hold lock of migrate_rw_
inline bool ZPMetaMigrateRegister::Exist() const {
  return ctime_ != 0;
}

// Register a new Meta Migrate Task, which is make up by diffs
Status ZPMetaMigrateRegister::Init(
    const std::vector<ZPMeta::RelationCmdUnit>& diffs) {
  slash::RWLock l(&migrate_rw_, true);
  if (Exist()) {
    return Status::Complete("Migrate already exist");
  }
  if (diffs.empty()) {
    return Status::InvalidArgument("Migrate registe with no diff item");
  }

  // Write DIFF items
  Status fs = Status::OK();
  std::string diff_key, diff_value;
  for (const auto& diff : diffs) {
    diff_key = DiffKey(diff);
    if (!diff.SerializeToString(&diff_value)) {
      LOG(WARNING) << "Serialization diff item failed";
      return Status::Corruption("Serialization diff item failed");
    }

    fs = floyd_->Write(diff_key, diff_value);
    if (!fs.ok()) {
      LOG(WARNING) << "Init migrate diff item failed: " << fs.ToString();
      return fs;
    }

    // Record in memory
    diff_keys_.insert(diff_key);
  }

  // Write migrate head
  uint64_t tmp_time = slash::NowMicros();
  total_size_ = diff_keys_.size();
  ZPMeta::MigrateHead migrate_head;
  migrate_head.set_begin_time(tmp_time);
  migrate_head.set_init_size(total_size_);
  for (const auto& key : diff_keys_) {
    migrate_head.add_diff_name(key);
  }

  std::string head_value;
  if (!migrate_head.SerializeToString(&head_value)) {
    LOG(WARNING) << "Serialization migrate head failed";
    return Status::Corruption("Serialization migrate head failed");
  }
  fs = floyd_->Write(kMigrateHeadKey, head_value);
  if (!fs.ok()) {
    return fs;
  }

  // Update ctime_ at last, by which we judge the Migrate Task exist or not
  ctime_ = tmp_time;
  return Status::OK();
}

// Check current Migrate Status
Status ZPMetaMigrateRegister::Check(ZPMeta::MigrateStatus* status) {
  slash::RWLock l(&migrate_rw_, false);
  if (!Exist()) {
    return Status::NotFound("No migrate exist");
  }

  status->set_begin_time(ctime_);
  if (total_size_ == 0) {
    return Status::Corruption("totol size be zero");
  }
  status->set_complete_proportion(100 - diff_keys_.size() * 100 / total_size_);
  return Status::OK();
}

// Erase one diff item and minus refer count
Status ZPMetaMigrateRegister::Erase(const std::string& diff_key) {
  slash::RWLock l(&migrate_rw_, true);
  if (!Exist()) {
    return Status::NotFound("No migrate exist");
  }

  if (diff_keys_.find(diff_key) == diff_keys_.end()) {
    return Status::Complete("diff not found, may finished");
  }

  refer_--;
  // Update MigrateHead
  ZPMeta::MigrateHead migrate_head;
  migrate_head.set_begin_time(ctime_);
  migrate_head.set_init_size(total_size_);
  for (const auto& key : diff_keys_) {
    if (key == diff_key) {
      continue;
    }
    migrate_head.add_diff_name(key);
  }

  if (migrate_head.diff_name_size() == 0) {
    // Finished
    return CancelWithoutLock();
  }

  std::string head_value;
  if (!migrate_head.SerializeToString(&head_value)) {
    LOG(WARNING) << "Serialization migrate head failed";
    return Status::Corruption("Serialization migrate head failed");
  }
  Status fs = floyd_->Write(kMigrateHeadKey, head_value);
  if (!fs.ok()) {
    return fs;
  }

  diff_keys_.erase(diff_key);

  // Remove diff item
  floyd_->Delete(diff_key);  // non-critical
  return Status::OK();
}

// Get some diff item and add refer count
// Return NotFound if the no migrate exist
// Return Incomplete if some task has already fetch out
// Notice the actually diff item may less than count
Status ZPMetaMigrateRegister::GetN(uint32_t count,
    std::vector<ZPMeta::RelationCmdUnit>* diff_items) {
  slash::RWLock l(&migrate_rw_, true);
  if (!Exist()) {
    return Status::NotFound("No migrate exist");
  }

  if (diff_keys_.size() < count) {
    count = diff_keys_.size();
  }

  if (refer_ > 0) {
    return Status::Incomplete("some task is not completed");
  }

  std::string diff_value;
  ZPMeta::RelationCmdUnit diff;
  int index = 0;
  for (const auto& dk : diff_keys_) {
    if (index++ >= count) {
      break;
    }
    Status fs = floyd_->Read(dk, &diff_value);
    if (!fs.ok()) {
      LOG(ERROR) << "Read diff item failed: " << fs.ToString()
        << ", diff item: " << dk;
      return fs;
    }
    diff.Clear();
    if (!diff.ParseFromString(diff_value)) {
      LOG(ERROR) << "Parse diff item failed, value: " << diff_value;
      return Status::Corruption("Parse diff item failed");
    }

    diff_items->push_back(diff);
  }
  refer_ = count;
  return Status::OK();
}

// Minus the refer count
void ZPMetaMigrateRegister::PutN(uint32_t count) {
  slash::RWLock l(&migrate_rw_, true);
  if (!Exist()) {
    return;
  }
  refer_ -= count;
}

Status ZPMetaMigrateRegister::Cancel() {
  slash::RWLock l(&migrate_rw_, true);
  return CancelWithoutLock();
}

// Required: hold the write lock of migrate_rw_
Status ZPMetaMigrateRegister::CancelWithoutLock() {
  if (!Exist()) {
    return Status::OK();
  }

  Status fs = floyd_->Delete(kMigrateHeadKey);
  if (!fs.ok()) {
    return fs;
  }

  for (const auto& dk : diff_keys_) {
    floyd_->Delete(dk);
  }
  diff_keys_.clear();
  total_size_ = 0;
  ctime_ = 0;
  refer_ = 0;

  return Status::OK();
}

Status ZPMetaMigrateRegister::Load() {
  slash::RWLock l(&migrate_rw_, true);

  std::string head_value;
  Status fs = floyd_->Read(kMigrateHeadKey, &head_value);
  if (fs.IsNotFound()) {
    return Status::OK();  // no migrate task exist
  } else if (!fs.ok()) {
    LOG(ERROR) << "Read migrate head failed: " << fs.ToString();
    return fs;
  }

  ZPMeta::MigrateHead migrate_head;
  if (!migrate_head.ParseFromString(head_value)) {
    LOG(ERROR) << "Parse migrate head failed, value: " << head_value;
    return Status::Corruption("Parse migrate head failed");
  }

  std::string diff_value;
  ZPMeta::RelationCmdUnit tmp_diff;
  total_size_ = migrate_head.init_size();
  for (const auto& dk : migrate_head.diff_name()) {
    fs = floyd_->Read(dk, &diff_value);
    if (!fs.ok()
        || !tmp_diff.ParseFromString(diff_value)) {
      LOG(ERROR) << "Check diff item failed, error: " << fs.ToString()
        << ", value: "<< diff_value;
      return Status::Corruption("Check diff item failed");
    }
    diff_keys_.insert(dk);
  }
  ctime_ = migrate_head.begin_time();
  refer_ = 0;
  return Status::OK();
}

