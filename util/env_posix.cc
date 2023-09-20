// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.






#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"

#include "util/env_posix.h"

namespace TimberSaw {



PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false),
      mmap_limiter_(MaxMmaps()),
      fd_limiter_(MaxOpenFiles()) {
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19843, /* tcp_port */
      1,	 /* ib_port */ //physical
      1, /* gid_idx */
      4*10*1024*1024 /*initial local buffer size*/
  };
  size_t remote_block_size = RDMA_WRITE_BLOCK;
  //Initialize the rdma manager, the remote block size will be configured in the beggining.
  // remote block size will always be the same.
  rdma_mg = std::make_shared<RDMA_Manager>(config, remote_block_size);
//  rdma_mg = new RDMA_Manager(config, remote_block_size);
  // Unlike the remote block size, the local block size is adjustable, and there could be different
  // local memory pool with different size. each size of memory pool will have an ID below is "4k"

  //client will try to connect to the remote memory, now there is only one remote memory.
  rdma_mg->Client_Set_Up_Resources();



      }

void PosixEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock();
  //TOTHINK: why there is a lock limiting one thread to do the compaction at the same time
  // Can we simply create a pool of threads and then every time we signal all the
  // pool threads, the thread check whether there is available job to do.
  // There are two options:
  // 1.The scheduled job should not picked by the thread itself (eg which table to
  // flush or which tables to compact), the job should be confirmed out side the thread.
  // 2. the thread itself pick the job and that needs a mutex, or lock free method. potentially you
  // can decide the job when you are inserting the job to the queue. so that there is no
  // lock inside the thread execution.

  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
    background_thread.detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal();
  }

  background_work_queue_.emplace(background_work_function, background_work_arg);
  background_work_mutex_.Unlock();
}
void PosixEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg, ThreadPoolType type) {
  switch (type) {
    case FlushThreadPool:
      if (flushing.queue_len_.load()>256){
        //If there has already be enough compaction scheduled, then drop this one
        DEBUG_arg("queue length has been too long %d elements in the queue\n", flushing.queue_len_.load());
        return;
      }
//      DEBUG_arg("flushing thread pool task queue length %zu\n", flushing.queue_.size());
      flushing.Schedule(background_work_function, background_work_arg);
      break;
    case CompactionThreadPool:
      if (compaction.queue_len_.load()>256){
        //If there has already be enough compaction scheduled, then drop this one
        DEBUG_arg("queue length has been too long %d elements in the queue\n", compaction.queue_len_.load());
        return;
      }
//      DEBUG_arg("compaction thread pool task queue length %zu\n", compaction.queue_.size());
      compaction.Schedule(background_work_function, background_work_arg);
      break;
//    case SubcompactionThreadPool:
//      subcompaction.Schedule(background_work_function, background_work_arg);
//      break;
  }
}
unsigned int PosixEnv::Queue_Length_Quiry(ThreadPoolType type){
  switch (type) {
    case FlushThreadPool:
      DEBUG_arg("flushing thread pool task queue length %zu\n", flushing.queue_.size());
      return flushing.queue_len_.load();
      break;
    case CompactionThreadPool:
      DEBUG_arg("compaction thread pool task queue length %zu\n", compaction.queue_.size());
      return compaction.queue_len_.load();
      break;
    case SubcompactionThreadPool:
      return subcompaction.queue_len_.load();
      break;
    default:
      return 0-1;
  }
}
void PosixEnv::JoinAllThreads(bool wait_for_jobs_to_complete) {
  flushing.JoinThreads(wait_for_jobs_to_complete);
  compaction.JoinThreads(wait_for_jobs_to_complete);
  subcompaction.JoinThreads(wait_for_jobs_to_complete);
}
void PosixEnv::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_.Lock();
//    printf("create a new thread");
    // Wait until there is work to be done.
    while (background_work_queue_.empty()) {
      DEBUG("Background thread wait.\n");
      background_work_cv_.Wait();
    }

    assert(!background_work_queue_.empty());
    auto background_work_function = background_work_queue_.front().function;
    void* background_work_arg = background_work_queue_.front().arg;
    background_work_queue_.pop();

    background_work_mutex_.Unlock();
    background_work_function(background_work_arg);
  }
}


namespace {

// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)
    env_initialized_.store(true, std::memory_order::memory_order_relaxed);
#endif  // !defined(NDEBUG)
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
      env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

}  // namespace

void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_open_read_only_file_limit = limit;
}

void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_mmap_limit = limit;
}

Env* Env::Default() {
  static PosixDefaultEnv env_container;
  return env_container.env();
}

}  // namespace TimberSaw
