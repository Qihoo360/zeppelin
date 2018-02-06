#include "include/zp_binlog.h"
#include "src/node/client.pb.h"

#include <functional>
#include <string>

using slash::Status;

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Usage: ./binlog_reader binlog_name [offset]");
  }

  uint64_t offset = 0;
  std::string confile(argv[1]);
  if (argc == 3) {
    offset = atoll(argv[2]);
  }
  printf("Binlog file: %s, offset: %lu\n", confile.c_str(), offset);

  slash::SequentialFile *file_queue;
  if (!slash::NewSequentialFile(confile, &file_queue).ok()) {
    printf("ZPBinlogSendTask Init new sequtial file failed");
  }
  BinlogReader* reader = new BinlogReader(file_queue);
  Status s = reader->Seek(offset);
  if (!s.ok()) {
    printf("Seek error: %s\n", s.ToString().c_str());
    return -1;
  }

  client::CmdRequest request;
  uint64_t size;
  std::string item;
  while (true) {
    s = reader->Consume(&size, &item);
    if (!s.ok()) {
      printf("Consume error: %s\n", s.ToString().c_str());
      break;
    }
    if (!request.ParseFromArray(item.data(), item.size())) {
      printf("Parser protobuf error\n");
    }
    std::string record_key, record_value;
    if (request.type() == client::Type::SET) {
      record_key = request.set().key();
      record_value = request.set().value();
    } else if (request.type() == client::Type::DEL) {
      record_key = request.del().key();
    }
    int partition_id = std::hash<std::string>()(record_key) % 16;
    if (partition_id == 15) {
      printf("15 key: ");
      for (size_t i = 0; i < record_key.size(); i++) {
        printf("%c", record_key[i]);
      }
      printf("\n");
    }
    printf("type: %d, partition_id: %d, key_size: %lu, value_size: %lu\n",
           request.type(), partition_id, record_key.size(), record_value.size());
  }

  delete file_queue;
  delete reader;

  return 0;
}
