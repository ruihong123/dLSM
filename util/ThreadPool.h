//
// Created by ruihong on 7/29/21.
//

#ifndef TimberSaw_THREADPOOL_H
#define TimberSaw_THREADPOOL_H
#include <condition_variable>
#include <deque>
#include <mutex>
#include <functional>
#include <vector>
#include <atomic>
#include <port/port_posix.h>
#include <assert.h>
namespace TimberSaw {
class DBImpl;
enum ThreadPoolType{FlushThreadPool, CompactionThreadPool, SubcompactionThreadPool};
struct BGItem {
  //  void* tag = nullptr;
  std::function<void(void* args)> function;
  void* args;
  //  std::function<void()> unschedFunction;
};
struct BGThreadMetadata {
  void* db;
  void* func_args;
};

class ThreadPool{
 public:
//  ThreadPool(std::mutex* mtx, std::condition_variable* signal);
  std::vector<port::Thread> bgthreads_;
  std::deque<BGItem> queue_;
  ThreadPoolType Type_;
  std::mutex mu_;
  std::condition_variable bgsignal_;
//  std::mutex RDMA_notify_mtx;
//  std::condition_variable RDMA_signal;
  int total_threads_limit_;
  std::atomic_uint queue_len_ = 0;
  bool exit_all_threads_ = false;
  bool wait_for_jobs_to_complete_;
  void WakeUpAllThreads() { bgsignal_.notify_all();
  }
  void BGThread() {
//    bool low_io_priority = false;
    while (true) {
      // Wait until there is an item that is ready to run
      std::unique_lock<std::mutex> lock(mu_);
      // Stop waiting if the thread needs to do work or needs to terminate.
      while (!exit_all_threads_ && queue_.empty() ) {
        bgsignal_.wait(lock);
      }

      if (exit_all_threads_) {  // mechanism to let BG threads exit safely

        if (!wait_for_jobs_to_complete_ ||
        queue_.empty()) {
          break;
        }
      }


      auto func = std::move(queue_.front().function);
      void* args = std::move(queue_.front().args);
      queue_.pop_front();

      queue_len_.store(static_cast<unsigned int>(queue_.size()),
                       std::memory_order_relaxed);

      lock.unlock();


      func(args);
    }
  }
  void StartBGThreads() {
    // Start background thread if necessary
    while ((int)bgthreads_.size() < total_threads_limit_) {

      port::Thread p_t(&ThreadPool::BGThread, this);
      bgthreads_.push_back(std::move(p_t));
    }
  }
  void Schedule(std::function<void(void* args)>&& func, void* args){

    std::lock_guard<std::mutex> lock(mu_);
    if (exit_all_threads_) {
      return;
    }
//    printf("schedule a work request!\n");
    StartBGThreads();
    // Add to priority queue
    queue_.push_back(BGItem());

    auto& item = queue_.back();
    //    item.tag = tag;
    item.function = std::move(func);
    item.args = std::move(args);

    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);

    //    if (!HasExcessiveThread()) {
    //      // Wake up at least one waiting thread.
    //      bgsignal_.notify_one();
    //    } else {
    //      // Need to wake up all threads to make sure the one woken
    //      // up is not the one to terminate.
    //      WakeUpAllThreads();
    //    }
    WakeUpAllThreads();
  }
  void JoinThreads(bool wait_for_jobs_to_complete) {

    std::unique_lock<std::mutex> lock(mu_);
    assert(!exit_all_threads_);

    wait_for_jobs_to_complete_ = wait_for_jobs_to_complete;
    exit_all_threads_ = true;
    // prevent threads from being recreated right after they're joined, in case
    // the user is concurrently submitting jobs.
    total_threads_limit_ = 0;

    lock.unlock();

    bgsignal_.notify_all();

    for (auto& th : bgthreads_) {
      th.join();
    }

    bgthreads_.clear();

    exit_all_threads_ = false;
    wait_for_jobs_to_complete_ = false;
  }
  void SetBackgroundThreads(int num){
    total_threads_limit_ = num;
  }
  //  void Schedule(std::function<void(void* args)>&& schedule, void* args);

};}


#endif  // TimberSaw_THREADPOOL_H
