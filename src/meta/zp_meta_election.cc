#include "src/meta/zp_meta_election.h"

#include <glog/logging.h>
#include "slash/include/env.h"
#include "slash/include/slash_string.h"

#include "include/zp_const.h"
#include "include/zp_conf.h"

extern ZpConf* g_zp_conf;
const std::string kElectLockKey = "##elect_lock";
const std::string kLeaderKey = "##meta_leader11111";

static bool IsLeaderTimeout(uint64_t last_active, uint64_t timeout) {
  return last_active + timeout * 1000 * 1000 <= slash::NowMicros();
}

ZPMetaElection::ZPMetaElection(floyd::Floyd* f)
  : floyd_(f) {
    pthread_rwlock_init(&last_leader_rw_, NULL);
  }

ZPMetaElection::~ZPMetaElection() {
  pthread_rwlock_destroy(&last_leader_rw_);
}

Status ZPMetaElection::ReadLeaderRecord(ZPMeta::MetaLeader* cleader) {
  std::string value;
  Status s = floyd_->Read(kLeaderKey, &value);
  if (!s.ok()) {
    LOG(WARNING) << "Read Leader Key failed: " << s.ToString();
    return s;
  }
  if (!cleader->ParseFromString(value)) {
    LOG(WARNING) << "Parse MetaLeader failed";
    return Status::Corruption("Parse MetaLeader failed");
  }
  return Status::OK();
}

Status ZPMetaElection::WriteLeaderRecord(const ZPMeta::MetaLeader& cleader) {
  std::string value;
  // Write back
  if (!cleader.SerializeToString(&value)) {
    LOG(WARNING) << "SerializeToString ZPMeta::MetaLeader failed.";
    return Status::Corruption("Serialize MetaLeader failed");
  }
  Status s = floyd_->Write(kLeaderKey, value);
  if (!s.ok()) {
    LOG(WARNING) << "Write MetaLeader failed." << s.ToString();
    return s;
  }
  return Status::OK();
}

bool ZPMetaElection::Jeopardy(std::string* ip, int* port) {
  slash::RWLock l(&last_leader_rw_, false);
  if (!last_leader_.IsInitialized()) {
    // no last
    LOG(WARNING) << "Jeopardy finished since no last leader";
    return false;
  }
  uint64_t timeout = kMetaLeaderTimeout;
  if (last_leader_.leader().ip() == g_zp_conf->local_ip()
      && last_leader_.leader().port() == g_zp_conf->local_port()) {
    // Smaller timeout for leader so that it could give up leadership on time
    // before any follower think it could be leader
    timeout -= kMetaLeaderRemainThreshold;
  }
  if (IsLeaderTimeout(last_leader_.last_active(), timeout)) {
    LOG(WARNING) << "Jeopardy finished since timeout";
    return false;
  }
  *ip = last_leader_.leader().ip();
  *port = last_leader_.leader().port();
  return true;
}

// Check leader ip and port
// return false means faled to check leader for some time
bool ZPMetaElection::GetLeader(std::string* ip, int* port) {
  std::string local_ip = g_zp_conf->local_ip();
  int local_port = g_zp_conf->local_port();
  std::string mine = slash::IpPortString(local_ip, local_port);

  // Read first to avoid follower locking everytime
  ZPMeta::MetaLeader cleader; 
  Status s = ReadLeaderRecord(&cleader);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(WARNING) << "pre-ReadLeaderRecord failed: " << s.ToString()
      << ", check jeopardy";
    return Jeopardy(ip, port);  
  } else if (s.ok()) {
    {
    slash::RWLock l(&last_leader_rw_, true);
    last_leader_.CopyFrom(cleader);
    }
    if ((local_ip != cleader.leader().ip()
          || local_port != cleader.leader().port())
        && !IsLeaderTimeout(cleader.last_active(), kMetaLeaderTimeout)) {
      // I'm not leader and current leader is not timeout
      *ip = cleader.leader().ip();
      *port = cleader.leader().port();
      return true;
    }
  }

  // Lock and update
  s = floyd_->TryLock(kElectLockKey, mine,
      kMetaLeaderLockTimeout * 1000);
  if (!s.ok()) {
    LOG(WARNING) << "TryLock ElectLock failed." << s.ToString();
    return Jeopardy(ip, port);  
  }

  cleader.Clear();
  s = ReadLeaderRecord(&cleader);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(WARNING) << "ReadLeaderRecord after lock failed, Jeopardy then. Error:"
      << s.ToString();
    floyd_->UnLock(kElectLockKey, mine);
    LOG(WARNING) << "ReadLeaderRecord failed: " << s.ToString()
      << ", check jeopardy";
    return Jeopardy(ip, port);  
  } else if (s.ok()) {
    slash::RWLock l(&last_leader_rw_, true);
    last_leader_.CopyFrom(cleader);
  }

  // 1. NotFound
  // 2, Ok, leader need refresh timeout
  // 3, Ok, follwer try elect
  if (s.IsNotFound()  // No leader yet
      || (cleader.leader().ip() == local_ip
        && cleader.leader().port() == local_port)     // I'm Leader
      || IsLeaderTimeout(cleader.last_active(),
        kMetaLeaderTimeout)) {  // Old leader timeout
    if (cleader.leader().ip() != local_ip
        || cleader.leader().port() != local_port) {
      LOG(INFO) << "Take over the leadership, since: "
        << (s.IsNotFound() ? "no leader record" : "old leader timeout"); 
    }

    // Update
    cleader.mutable_leader()->set_ip(local_ip);
    cleader.mutable_leader()->set_port(local_port);
    cleader.set_last_active(slash::NowMicros());
    
    s = WriteLeaderRecord(cleader);
    if (s.ok()) {
      // Refresh cache
      slash::RWLock l(&last_leader_rw_, true);
      last_leader_.CopyFrom(cleader);
    }
  }

  *ip = cleader.leader().ip();
  *port = cleader.leader().port();
  floyd_->UnLock(kElectLockKey, mine);
  return true;
}
