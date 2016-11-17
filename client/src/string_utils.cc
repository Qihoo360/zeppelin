#include <string>
#include <vector>
#include <algorithm>
#include <string.h>

#include "string_utils.h"

namespace utils {

void GetSeparateArgs(const std::string& raw_str, std::vector<std::string>* argv_p) {
  const char* p = raw_str.data();  
  const char *pch = strtok(const_cast<char*>(p), " ");
  while (pch != NULL) {
    argv_p->push_back(std::string(pch));
    pch = strtok(NULL, " ");
  }
}


}
