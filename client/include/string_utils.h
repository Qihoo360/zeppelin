#ifndef STRING_UTILS_H
#define STRING_UTILS_H

#include <string>

namespace utils {

void GetSeparateArgs(const std::string& raw_str, std::vector<std::string>* argv_p);
inline bool StrCaseEqual(const std::string& s1, const std::string& s2) {
	return !strcasecmp(s1.c_str(), s2.c_str());
}

}

#endif
