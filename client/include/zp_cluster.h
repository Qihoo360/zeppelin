#ifndef ZP_CLUSTER_H
#define ZP_CLUSTER_H

#include <string>
#include <vector>
#include <list>
#include <map>
#include <vector>
#include <memory>


#include "zp_types.h"
#include "zp_meta_cli.h"

namespace libZp {
    class Cluster;
    class IoCtx {
      public :
        IoCtx() {};
        ~IoCtx() {};
      private :
        Cluster* cluster;
        std::string table_;
    };


    class Cluster {
    public :

      Cluster (const Options& options);
      virtual ~Cluster ();
      Status Connect();
      IoCtx CreateIoCtx (const std::string &table);
      Status ListMetaNode (std::vector<IpPort> &node_list);
      //Status ListDataNode (std::vector<IpPort> &node_list);

    private :

      Status Pull();
      IpPort GetRandomMetaAddr();
      std::shared_ptr<ZpMetaCli> GetMetaCli();
      Status GetRandomMetaCli();
      std::shared_ptr<ZpMetaCli> CreateMetaCli(IpPort& ipPort);

      ClusterMap cluster_map_;
      std::map<IpPort, std::shared_ptr<ZpMetaCli>> meta_cli_;
      //std::map<IpPort, std::shared_ptr<DataCli>> data_cli_;
      std::vector<IpPort> meta_addr_;
      std::vector<IpPort> data_addr_;
    };


}
#endif
