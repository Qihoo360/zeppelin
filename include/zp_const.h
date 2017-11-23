#ifndef INCLUDE_ZP_CONST_H_
#define INCLUDE_ZP_CONST_H_
#include <string>

#define dstr(a) #a
#define dxstr(a) dstr(a)
const std::string kZPVersion = dxstr(_GITVER_);
const std::string kZPCompileDate = dxstr(_COMPILEDATE_);
const std::string kZpPidFile = "pid";
const std::string kZpLockFile = "lock";

/* Port shift */
const int kPortShiftSync = 200;
const int kPortShiftRsync = 300;
const int kMetaPortShiftCmd = 0;
const int kMetaPortShiftFY = 100;

/* Binlog related */
// the block size that we read and write from write2file
// the default size is 64KB
const size_t kBlockSize = 64 * 1024;
// The size of Binlogfile
//const uint64_t kBinlogSize = 1024 * 100;
const uint64_t kBinlogSize = 1024 * 1024 * 100;
// Header is Type(1 byte), length (2 bytes)
const size_t kHeaderSize = 1 + 3;
const std::string kBinlogPrefix = "binlog";
const size_t kBinlogPrefixLen = 6;
const std::string kManifest = "manifest";

/* DBSync related */
const uint32_t kDBSyncMaxGap = 1000;
const std::string kDBSyncModule = "document";
const uint32_t kDBSyncSpeedLimit = 126; //MBPS
const int kDBSyncRetryTime = 5;    // retry time to send single file for DBSync
const std::string kBgsaveInfoFile = "info";

/* Purge Log related */
const uint32_t kBinlogRemainMinCount = 10;
const uint32_t kBinlogRemainMaxCount = 60;
const uint32_t kBinlogRemainMaxDay = 30;

/* Migrate related */
// how many diff item handled one time
const int kMetaMigrateOnceCount = 2;
const int kConditionCronInterval= 3000; // millisecond

/* Sync related */
// TrySync Delay time := kRecoverSyncDelayCronCount * (kNodeCronInterval * kNodeCronWaitCount)
const int kRecoverSyncDelayCronCount = 7;
const int kStuckRecoverSyncDelayCronCount = 450; // for slave stuck out of kConnected
const int kTrySyncInterval = 5000;  // mili seconds
const int kBinlogSendInterval = 1;
const int kBinlogRedundantLease = 10;  // some more lease time for redundance
const int kBinlogMinLease = 20;
const int kBinlogDefaultLease = 20;
const int kBinlogTimeSlice = 5;    // should larger than kBinlogSendInterval
const int kBinlogReceiverCronInterval = 6000;
const int kBinlogReceiveBgWorkerFull = 100;

/* Heartbeat related */
const int kPingInterval = 5;
// timeout between node and meta server
// the one for meta should large than for node
// and both larger than kPingInterval
const int kNodeMetaTimeoutN = 10;
const int kNodeMetaTimeoutM = 30;

/* Dispatch related */
const int kDispatchCronInterval = 5000;
const int kDispatchQueueSize = 1000;
const int kMetaDispathCronInterval = 1000;
const int kMetaDispathQueueSize = 1000;
const int kKeepAlive = 60;  // seconds
const int kMetacmdInterval = 6;

/* Server cron related */
// Server cron wait kNodeCronInterval * kNodeCronWaitCount every time
const int kNodeCronInterval = 1000;
const int kNodeCronWaitCount = 2;
const int kMetaCronInterval = 1000;
const int kMetaCronWaitCount = 5;

/* Meta elect */
const int kMetaLeaderLockTimeout = 5;
const int kMetaLeaderTimeout = 60;
const int kMetaLeaderRemainThreshold = 10; // Should large than kMetaCronInterval * kMetaCronWaitCount

const int kMetaOffsetStuckDist =  1024 * 100;  // when begin to stuck parititon, should small than kBinlogSize
const int kSlowdownDelayRatio = 60;  // Percent of write request to delay

#endif  // INCLUDE_ZP_CONST_H_
