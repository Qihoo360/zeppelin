#ifndef NET_UTILS_H
#define NET_UTILS_H

namespace utils {
  int EpollWait(int fd, int tm_ms, uint32_t en_v);
  inline int SetNonblocking(int32_t fd);
  inline int SetBlocking(int32_t fd);
  int Connect(int32_t socket_fd, const struct sockaddr* addr_p, socklen_t addrlen, int32_t timeout);
	int CheckLive(int socket_fd);
};

#endif
