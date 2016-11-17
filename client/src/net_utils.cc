#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/epoll.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "net_utils.h"

namespace utils {
/* this function is just to check is there is specifiyed events happening on the single fd within the given time, not caring about what events */
int EpollWait(int fd, int tm_ms, uint32_t ev_u) {
  int ret, epoll_fd = epoll_create(1024);
  struct epoll_event events[8], ev;
  ev.events = ev_u;
  ev.data.fd = fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
  ret = epoll_wait(epoll_fd, events, 8, tm_ms);
  close(epoll_fd);
  return ret;
}

int SetNonblocking(int32_t fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int SetBlocking(int32_t fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  return fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}

int Connect(int32_t socket_fd, const struct sockaddr* addr_p, socklen_t addrlen, int32_t timeout) { // TODO:bind to a local (IP:port first)
  int error = 0, len = sizeof(error);
  if (timeout != -1 && SetNonblocking(socket_fd) == -1) {
    return -1;
  }
  if (connect(socket_fd, addr_p, addrlen) == -1) {
    if (errno != EINPROGRESS) {
      return -1;
    }
    /* Here then be nonblocking and connection is in operating */
    if (EpollWait(socket_fd, timeout*1000, EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP) == 0) {
      errno = ETIMEDOUT;
      return -1;
    }
    if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), reinterpret_cast<socklen_t*>(&len)) == -1 
        || error != 0) {
      return -1;
    }
  }
  if (timeout != -1 && SetBlocking(socket_fd) == -1) {
    return -1;
  } 
  return 0; 
}

}
