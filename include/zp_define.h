#ifndef ZP_DEFINE_H
#define ZP_DEFINE_H

#define ZP_MAX_WORKER_THREAD_NUM 24

const std::string kZPVersion = "0.0.1";
const std::string kZPPidFile = "zp.pid";

// the block size that we read and write from write2file
// the default size is 64KB
const size_t kBlockSize = 64 * 1024;

// Header is Type(1 byte), length (2 bytes)
const size_t kHeaderSize = 1 + 3;

const std::string kBinlogPrefix = "binlog";
//const size_t kBinlogPrefixLen = 6;

const std::string kManifest = "manifest";

//struct ClientInfo {
//  int fd;
//  std::string ip_port;
//  int last_interaction;
//};
//
////slave item
//struct SlaveItem {
//  int64_t sid;
//  std::string ip_port;
//  int port;
//  pthread_t sender_tid;
//  int hb_fd;
//  int stage;
//  void* sender;
//  struct timeval create_time;
//};

//#define SLAVE_ITEM_STAGE_ONE 1
//#define SLAVE_ITEM_STAGE_TWO 2
//
////repl_state_
//#define ZP_REPL_NO_CONNECT 0
//#define ZP_REPL_CONNECT 1
//#define ZP_REPL_CONNECTING 2
//#define ZP_REPL_CONNECTED 3
//#define ZP_REPL_WAIT_DBSYNC 4
//
////role
//#define ZP_ROLE_SINGLE 0
//#define ZP_ROLE_SLAVE 1
//#define ZP_ROLE_MASTER 2


/*
 * The size of Binlogfile
 */
//uint64_t kBinlogSize = 128; 
//const uint64_t kBinlogSize = 1024 * 1024 * 100;




/*
 * define reply between master and slave
 *
 */
const std::string kInnerReplOk = "ok";
const std::string kInnerReplWait = "wait";

const unsigned int kMaxBitOpInputKey = 12800;
const int kMaxBitOpInputBit = 21;
/*
 * db sync
 */
const uint32_t kDBSyncMaxGap = 50;
const std::string kDBSyncModule = "document";

const std::string kBgsaveInfoFile = "info";
#endif
