#ifndef ZP_TRYSYNC_THREAD_H
#define ZP_TRYSYNC_THREAD_H
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "pink/include/pink_cli.h"
#include "pink/include/bg_thread.h"

#include "include/zp_const.h"
#include "src/node/zp_data_partition.h"

class ZPTrySyncThread {
 public:
  ZPTrySyncThread();
  virtual ~ZPTrySyncThread();
  void TrySyncTaskSchedule(const std::string& table,
      int partition_id, uint64_t delay = 0);
  void TrySyncTask(const std::string& table_name, int partition_id);

 private:
  // BGThread related
  struct TrySyncTaskArg {
    ZPTrySyncThread* thread;
    std::string table_name;
    int partition_id;
    TrySyncTaskArg(ZPTrySyncThread* t, const std::string& table, int id)
        : thread(t), table_name(table), partition_id(id){}
  };
  slash::Mutex bg_thread_protector_;
  pink::BGThread* bg_thread_;
  static void DoTrySyncTask(void* arg);
  bool SendTrySync(const std::string& table_name, int partition_id);
  bool Send(std::shared_ptr<Partition> partition, pink::PinkCli* cli);
  
  struct RecvResult {
    client::StatusCode code;
    std::string message;
    uint32_t filenum;
    uint64_t offset;
  };
  bool Recv(std::shared_ptr<Partition> partition, pink::PinkCli* cli,
      RecvResult* res);

  // Rsync related
  int rsync_flag_;
  void RsyncRef();
  void RsyncUnref();
  
  // Connection related
  std::map<std::string, pink::PinkCli*> client_pool_;
  pink::PinkCli* GetConnection(const Node& node);

  void DropConnection(const Node& node);
};

#endif //ZP_TRYSYNC_THREAD_H
