
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>

int epoll_fd;                          // mechanism for querying file descriptors asyncronously
void * (* actions[16])(void * arg);    // the registered actions to perform repeatedly

void * task_a(void * nil) { printf("A\n"); return NULL; }    // example tasks to repeat
void * task_b(void * nil) { printf("B\n"); return NULL; }    // -----------------------
void * task_c(void * nil) { printf("C\n"); return NULL; }    // -----------------------

void repeat(void * (* action)(void *), long ms) {
  
  int timer;
  
  struct itimerspec timer_spec = {
      .it_interval.tv_sec  = 0,
      .it_value   .tv_sec  = 0,
      .it_interval.tv_nsec = ms * 1000 * 1000,
      .it_value   .tv_nsec = ms * 1000 * 1000,
  };
  
  if ((timer = timerfd_create(CLOCK_MONOTONIC, 0)) < 0) exit(1);
  if (timerfd_settime(timer, 0, &timer_spec, NULL) < 0) exit(1);
  
  struct epoll_event event_info = {
      .events  = EPOLLIN,
      .data.fd = timer,
  };
  
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timer, &event_info) < 0) exit(1);
  
  actions[timer] = action;
}

int main() {
  
  if ((epoll_fd = epoll_create1(0)) < 0) exit(1);
  
  // Add the things you want to repeat every so ofter (must be less than 1 second)
  repeat(task_a, 750);
  repeat(task_b, 500);
  repeat(task_c, 450);
  
  struct epoll_event events[16] = { 0 };
  
  for (;;) {
    
    int ready;    // number of descriptors (not all necessarily timers) that are ready for read
    
    if ((ready = epoll_wait(epoll_fd, events, 16, -1)) < 0)
      continue;
    
    while (ready --> 0) {
      
      int timer = events[ready].data.fd;
      
      (actions[timer])(NULL);
      
      uint64_t dummy;                        // disarm the timer
      read(timer, &dummy, sizeof(dummy));    // ----------------
    }
  }
}
