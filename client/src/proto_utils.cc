#include <string>

#include "proto_utils.h"

namespace utils {

Message* NewMessage(const std::string& MsgType) {
  Message* msg_p = NULL;
  const Descriptor* des_p = DescriptorPool::generated_pool()->FindMessageTypeByName(MsgType);
  if (des_p) {
    const Message* protype_p = MessageFactory::generated_factory()->GetPrototype(des_p);
    if (protype_p) {
      msg_p = protype_p->New();
    }
  }
  return msg_p;
}

void DelMessage(Message* msg_p) {
  if (msg_p) {
    msg_p->Clear();
    delete msg_p;
  }
}

}
