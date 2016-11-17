#ifndef ZP_CLUSTER_CLIENT_H
#define ZP_CLUSTER_CLIENT_H

#include <string>
#include <vector>
#include <list>
#include <map>

#include <arpa/inet.h>

#include "client.pb.h"
#include "zp_meta.pb.h"
#include "status.h"

#define MAX_SINGLE_MESSAGE_LEN  102400
#define MESSAGE_HEADER_LEN 4

enum ServerType {
	kDataServer,
	kMetaServer
};

struct IpPort {
	std::string hostname;
	int32_t port;
};

//struct PartitionRange {
//	int32_t low;
//	int32_t high;
//};

struct Socket {
	int32_t socket_fd;
};

struct Node {
	IpPort host;
	std::list<Node*> slaves;
	Socket sock;
};

struct ClusterInfo {
//	std::string table_;
	int32_t total_partition;
	std::map<int32_t, Node*> masters; // (partition, master) pairs
	std::map<std::string, Node*> nodes; // (ip:port, master) pairs, the slaves can be obtained from the master
};

class ZPClusterClient {

	public:
		ZPClusterClient(const std::vector<IpPort>& meta_hosts, int32_t connect_timeout_ms = -1, int32_t rw_timeout_ms = -1);
		virtual ~ZPClusterClient();
		// To data node
		Status Set(const std::string& key, const std::string& value, const std::string& uuid = "");
		Status Get(const std::string& key, const std::string& uuid = "");
		Status SendDataCommand(int32_t partition);
		// To meta node
		Status Pull(::ZPMeta::MetaCmdResponse_Pull* pull_resp);
		Status Init(int32_t parition_num);
//		Status SendCommand(const ::ZPMeta::MetaCmd& command);	
	private:

		Status Connect(const IpPort& server, int32_t* socket_fd = NULL);
		Status GetClusterInfo();
		int32_t GetPartition(const std::string& key) {
			return std::hash<std::string>()(key) % (cluster_.total_partition);
		}

		::google::protobuf::Message* ConstructCommand(ServerType serverType, int32_t commandType);
		::google::protobuf::Message* ConstructMetaCommand(::ZPMeta::MetaCmd_Type commandType);
		::google::protobuf::Message* ConstructDataCommand(::client::Type commandType);

		Status SerializeMessage(::google::protobuf::Message* cmd);
    
		inline void SerializeMessageHeader(char* dst_p, const char* src_p, int32_t header_len = 4) {
      (void)header_len;
      *reinterpret_cast<uint32_t*>(dst_p) = htonl(reinterpret_cast<const ::google::protobuf::Message*>(src_p)->ByteSize());
    }

    inline void ParseMessageHeader(const char* src_p, char* dst_p, int32_t header_len = 4) {
      (void)header_len;
      *reinterpret_cast<uint32_t*>(dst_p) = ntohl(*reinterpret_cast<const uint32_t*>(src_p));
    }

		int Send();
		int Recv();

    void ClearWbuf() {
      bzero(wbuf_, sizeof(wbuf_));
      wlen_ = 0;
    }

    void ClearRbuf() {
      bzero(rbuf_, sizeof(rbuf_));
      rlen_ = 0;
    }
		Status RestorePullResponse(const ::ZPMeta::MetaCmdResponse_Pull& pull_resp, ClusterInfo* cluster_info);

		std::vector<IpPort> meta_hosts_;
		ClusterInfo cluster_;

		int32_t current_fd_; //current fd
		char wbuf_[MAX_SINGLE_MESSAGE_LEN];
		int32_t wlen_;
		char rbuf_[MAX_SINGLE_MESSAGE_LEN];	
		int32_t rlen_;

		int32_t connect_timeout_ms_; // -1 means block, >=0 means timedout nonblock TODO
		int32_t rw_timeout_ms_; // -1 means block, >=0 means timedout nonblock TODO

		ZPClusterClient& operator=(ZPClusterClient&);
		ZPClusterClient(ZPClusterClient&);
};

#endif
