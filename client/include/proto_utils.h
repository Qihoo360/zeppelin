#ifndef PROTO_UTILS_H
#define PROTO_UTILS_H

#include <google/protobuf/message.h>
#include <google/protobuf/message_lite.h>
#include <google/protobuf/descriptor.h>

#include <string>

namespace utils {

typedef ::google::protobuf::Message Message;
typedef ::google::protobuf::Descriptor Descriptor;
typedef ::google::protobuf::DescriptorPool DescriptorPool;
typedef ::google::protobuf::MessageFactory MessageFactory;

Message* NewMessage(const std::string& MsgType);
void DelMessage(Message* msg_p);

}

#endif
