#ifndef ZP_CONST_H
#define ZP_CONST_H

#include <string>

const int kMaxWorkerThread = 100;
const int kMaxMetaWorkerThread = 16;
const int kNumBinlogSendThread = 6;

const std::string kZPVersion = "0.0.1";
const std::string kZpPidFile = "zp.pid";
const std::string kZpLockFile = "zp.lock";

////// Server State /////
enum Role {
  kNodeSingle = 0,
  kNodeMaster = 1,
  kNodeSlave = 2,
};
const std::string RoleMsg[] {
  "kNodeSingle",
  "kNodeMaster",
  "kNodeSlave"
};
enum ReplState {
  kNoConnect = 0,
  kShouldConnect = 1,
  kConnected = 2,
  kWaitDBSync = 3,
};
// debug only
const std::string ReplStateMsg[] = {
  "kNoConnect",
  "kShouldConnect",
  "kConnected",
  "kWaitDBSync"
};
const std::string MetaStateMsg[] = {
  "kMetaConnect",
  "kMetaConnected"
};

// Data port shift
// TODO DataPort is stall
//const int kPortShiftDataCmd = 100;
const int kPortShiftSync = 200;
const int kPortShiftRsync = 300;

// Meta port shift
const int kMetaPortShiftCmd = 0;
const int kMetaPortShiftFY = 100;


// TrySync Delay time := kRecoverSyncDelayCronCount * (kNodeCronInterval * kNodeCronWaitCount)
const int kRecoverSyncDelayCronCount = 7;
const int kTrySyncInterval = 3;
const int kBinlogSendInterval = 2;
const int kBinlogTimeSlice = 10; //should larger than kBinlogSendInterval
const int kPingInterval = 3;
const int kMetacmdInterval = 3;
const int kDispatchCronInterval = 5000;
const int kMetaDispathCronInterval = 3000;
const int kWorkerCronInterval = 5000;
const int kMetaWorkerCronInterval = 1000;
const int kBinlogReceiverCronInterval = 6000;
// Server cron wait kNodeCronInterval * kNodeCronWaitCount every time
const int kNodeCronInterval = 1000;
const int kNodeCronWaitCount = 2;
//const int kBinlogReceiverCronInterval = 1000;
const int kBinlogReceiveBgWorkerCount = 4;
const int kBinlogReceiveBgWorkerFull = 100;



////// Binlog related //////
// the block size that we read and write from write2file
// the default size is 64KB
const size_t kBlockSize = 64 * 1024;

// Header is Type(1 byte), length (2 bytes)
const size_t kHeaderSize = 1 + 3;

const std::string kBinlogPrefix = "binlog";
const size_t kBinlogPrefixLen = 6;

const std::string kManifest = "manifest";

//#define SLAVE_ITEM_STAGE_ONE 1
//#define SLAVE_ITEM_STAGE_TWO 2

//
// The size of Binlogfile
//
//const uint64_t kBinlogSize = 128; 
//const uint64_t kBinlogSize = 256;
const uint64_t kBinlogSize = 1024 * 1024 * 100;


//
// define reply between master and slave
//
const std::string kInnerReplOk = "ok";
const std::string kInnerReplWait = "wait";

const unsigned int kMaxBitOpInputKey = 12800;
const int kMaxBitOpInputBit = 21;

// DBSync
//const uint32_t kDBSyncMaxGap = 200;
const uint32_t kDBSyncMaxGap = 1000;
const std::string kDBSyncModule = "document";
const uint32_t kDBSyncSpeedLimit = 126; //MBPS
const std::string kBgsaveInfoFile = "info";

// Purge binlog
const uint32_t kBinlogRemainMinCount = 3;
const uint32_t kBinlogRemainMaxCount = 20;
const uint32_t kBinlogRemainMaxDay = 7;


//
//meta related
//
const std::string kMetaTables = "##tables";
const std::string kMetaNodes = "##nodes";
const std::string kMetaVersion = "##version";

// timeout between node and meta server, the one for meta should large than node
const int kNodeMetaTimeoutN = 10;
const int kNodeMetaTimeoutM= 15;

#endif
