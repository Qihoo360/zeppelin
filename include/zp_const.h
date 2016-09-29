#ifndef ZP_CONST_H
#define ZP_CONST_H

#include <string>

const int kMaxWorkerThread = 4;
const int kMaxMetaWorkerThread = 16;

const std::string kZPVersion = "0.0.1";
const std::string kZPPidFile = "zp.pid";

////// Server State /////
enum Role {
  kNodeSingle = 0,
  kNodeMaster = 1,
  kNodeSlave = 2,
};
enum ReplState {
  kNoConnect = 0,
  kShouldConnect = 1,
  kConnected = 2,
  kWaitDBSync = 3,
};
enum MetaState {
  kMetaConnect = 1,
  kMetaConnecting = 2,
  kMetaConnected = 3,
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
  "kMetaConnecting",
  "kMetaConnected"
};
const std::string RoleMsg[] {
  "kNodeSingle",
  "kNodeMaster",
  "kNodeSlave"
};

// Data port shift
const int kPortShiftDataCmd = 100;
const int kPortShiftSync = 200;
const int kPortShiftRsync = 300;

// Meta port shift
const int kMetaPortShiftCmd = 100;
const int kMetaPortShiftFY = 200;


const int kTrySyncInterval = 2;
const int kPingInterval = 3;
const int kMetacmdInterval = 3;
const int kDispatchCronInterval = 5000;
const int kMetaDispathCronInterval = 3000;
const int kWorkerCronInterval = 5000;
const int kMetaWorkerCronInterval = 1000;
const int kBinlogReceiverCronInterval = 6000;
//const int kBinlogReceiverCronInterval = 1000;



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
//uint64_t kBinlogSize = 128; 
//const uint64_t kBinlogSize = 1024 * 1024 * 100;


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
const uint32_t kBinlogRemainMaxCount = 20;
const uint32_t kBinlogRemainMaxDay = 7;


//
//meta related
//key in floyd is zpmeta##id
//
const std::string ZP_META_KEY_PREFIX = "zpmeta##";
const std::string ZP_META_KEY_PN = "##partition_num";
const std::string ZP_META_KEY_MT = "##full_meta";
const std::string ZP_META_KEY_ND = "##nodes";
const int ZP_META_UPDATE_RETRY_TIME = 3;

// timeout between node and meta server, the one for meta should large than node
const int NODE_META_TIMEOUT_N = 10;
const int NODE_META_TIMEOUT_M = 15;

#endif
