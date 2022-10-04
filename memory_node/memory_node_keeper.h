//
// Created by ruihong on 7/29/21.
//

#ifndef TimberSaw_HOME_NODE_KEEPER_H
#define TimberSaw_HOME_NODE_KEEPER_H


#include <queue>
//#include <fcntl.h>
#include "util/rdma.h"
#include "util/env_posix.h"
#include "util/ThreadPool.h"
#include "db/log_writer.h"
#include "db/version_set.h"

namespace TimberSaw {

struct Arg_for_persistent{
  VersionEdit_Merger* edit_merger;
  std::string client_ip;
};
class Memory_Node_Keeper {
 public:
//  friend class RDMA_Manager;
  Memory_Node_Keeper(bool use_sub_compaction, uint32_t tcp_port, int pr_s);
  ~Memory_Node_Keeper();
//  void Schedule(
//      void (*background_work_function)(void* background_work_arg),
//      void* background_work_arg, ThreadPoolType type);
  void JoinAllThreads(bool wait_for_jobs_to_complete);

  // this function is for the server.
  void Server_to_Client_Communication();
  void SetBackgroundThreads(int num,  ThreadPoolType type);
//  void MaybeScheduleCompaction(std::string& client_ip);
//  static void BGWork_Compaction(void* thread_args);
  static void RPC_Compaction_Dispatch(void* thread_args);
  static void RPC_Garbage_Collection_Dispatch(void* thread_args);
  static void Persistence_Dispatch(void* thread_args);
//  void BackgroundCompaction(void* p);
  void CleanupCompaction(CompactionState* compact);
  void PersistSSTables(void* arg);
  void PersistSSTable(std::shared_ptr<RemoteMemTableMetaData> sstable_ptr);
  void UnpinSSTables_RPC(VersionEdit_Merger* edit_merger, std::string& client_ip);
  void UnpinSSTables_RPC(std::list<uint64_t>* merged_file_number, std::string& client_ip);
  Status DoCompactionWork(CompactionState* compact, std::string& client_ip);
  void ProcessKeyValueCompaction(SubcompactionState* sub_compact);
  Status DoCompactionWorkWithSubcompaction(CompactionState* compact,
                                           std::string& client_ip);
  Status OpenCompactionOutputFile(SubcompactionState* compact);
  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(SubcompactionState* compact,
                                    Iterator* input);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status GetFileSize(const std::string& filename, uint64_t* size) {
    struct ::stat file_stat;
    if (::stat(filename.c_str(), &file_stat) != 0) {
      *size = 0;
      return PosixError(filename, errno);
    }
    *size = file_stat.st_size;
    return Status::OK();
  }
  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result)  {
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixSequentialFile(filename, fd);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result)  {
    *result = nullptr;
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    if (!mmap_limiter_.Acquire()) {
      *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
      return Status::OK();
    }

    uint64_t file_size;
    Status status = GetFileSize(filename, &file_size);
    if (status.ok()) {
      void* mmap_base =
          ::mmap(/*addr=*/nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
      if (mmap_base != MAP_FAILED) {
        *result = new PosixMmapReadableFile(filename,
                                            reinterpret_cast<char*>(mmap_base),
                                            file_size, &mmap_limiter_);
      } else {
        status = PosixError(filename, errno);
      }
    }
    ::close(fd);
    if (!status.ok()) {
      mmap_limiter_.Release();
    }
    return status;
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) {
    int fd = ::open(filename.c_str(),
                    O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result)  {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  bool FileExists(const std::string& filename)  {
    return ::access(filename.c_str(), F_OK) == 0;
  }
  static std::shared_ptr<RDMA_Manager> rdma_mg;
//  RDMA_Manager* rdma_mg;
 private:
  int pr_size;
  std::unordered_map<unsigned int, std::pair<std::mutex, std::condition_variable>> imm_notifier_pool;
  unsigned int imm_temp = 1;
  std::mutex mtx_temp;
  std::condition_variable cv_temp;
  std::shared_ptr<Options> opts;
  const InternalKeyComparator internal_comparator_;
//  const InternalFilterPolicy internal_filter_policy_;

  PosixLockTable locks_;  // Thread-safe.
  Limiter mmap_limiter_;  // Thread-safe.
  Limiter fd_limiter_;    // Thread-safe.
  // Opened lazily
  WritableFile* descriptor_file;
  log::Writer* descriptor_log;
  uint64_t manifest_file_number_ = 1;
  bool usesubcompaction;
  TableCache* const table_cache_;
  std::vector<std::thread> main_comm_threads;
  ThreadPool Compactor_pool_;
  ThreadPool Message_handler_pool_;
  ThreadPool Persistency_bg_pool_;
  std::mutex versionset_mtx;
  VersionSet* versions_;
  VersionEdit_Merger ve_merger;
  std::atomic<bool> check_point_t_ready = true;
  std::mutex merger_mtx;
//  std::mutex test_compaction_mutex;
#ifndef NDEBUG
  std::atomic<size_t> debug_counter = 0;


#endif
  Status InstallCompactionResults(CompactionState* compact,
                                  std::string& client_ip);
  Status InstallCompactionResultsToComputePreparation(CompactionState* compact);
  int server_sock_connect(const char* servername, int port);
  void server_communication_thread(std::string client_ip, int socket_fd);
  void create_mr_handler(RDMA_Request* request, std::string& client_ip,
                         uint8_t target_node_id);
  void create_qp_handler(RDMA_Request* request, std::string& client_ip,
                         uint8_t target_node_id);
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
  void install_version_edit_handler(RDMA_Request* request,
                                    std::string& client_ip,
                                    uint8_t target_node_id);
  void sst_garbage_collection(void* arg);

  void sst_compaction_handler(void* arg);

  void qp_reset_handler(RDMA_Request* request, std::string& client_ip,
                        int socket_fd, uint8_t target_node_id);
  void sync_option_handler(RDMA_Request* request, std::string& client_ip,
                           uint8_t target_node_id);
  void version_unpin_handler(RDMA_Request* request, std::string& client_ip);
  void Edit_sync_to_remote(VersionEdit* edit, std::string& client_ip,
                           std::unique_lock<std::mutex>* version_mtx,
                           uint8_t target_node_id);
};
}
#endif  // TimberSaw_HOME_NODE_KEEPER_H