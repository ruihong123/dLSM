// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_DB_DB_IMPL_H_
#define STORAGE_TimberSaw_DB_DB_IMPL_H_

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <set>
#include <string>

#include "TimberSaw/db.h"
#include "TimberSaw/env.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/RPC_Process.h"
#include "util/mutexlock.h"

#include "memtable_list.h"
#include "version_set.h"

namespace TimberSaw {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class MemTableList;
//TODO: make memtableversionlist and LSM versionset 's function integrated into
// Superversion.
struct SuperVersion {
  // Accessing members of this class is not thread-safe and requires external
  // synchronization (ie db mutex held or on write thread).
  MemTable* mem;
  MemTableListVersion* imm;
  Version* current;
  // Version number of the current SuperVersion
  uint64_t version_number;
//  std::mutex* versionset_mutex;

  // should be called outside the mutex
  SuperVersion(MemTable* new_mem, MemTableListVersion* new_imm,
               Version* new_current);
  ~SuperVersion();
  SuperVersion* Ref();
  // If Unref() returns true, Cleanup() should be called with mutex held
  // before deleting this SuperVersion.
  bool Unref();

  // call these two methods with db mutex held
  // Cleanup unrefs mem, imm and current. Also, it stores all memtables
  // that needs to be deleted in to_delete vector. Unrefing those
  // objects needs to be done in the mutex
  void Cleanup();
  void Init();

  // The value of dummy is not actually used. kSVInUse takes its address as a
  // mark in the thread local storage to indicate the SuperVersion is in use
  // by thread. This way, the value of kSVInUse is guaranteed to have no
  // conflict with SuperVersion object address and portable on different
  // platform.
  static int dummy;
  static void* const kSVInUse;
  static void* const kSVObsolete;

 private:
  std::atomic<uint32_t> refs;
  // We need to_delete because during Cleanup(), imm->Unref() returns
  // all memtables that we need to free through this vector. We then
  // delete all those memtables outside of mutex, during destruction
  autovector<MemTable*> to_delete;
};
// The structure for storing argument for thread pool.
#ifdef WITHPERSISTENCE
class DBImpl : public DB, RPC_Process {
#else
class DBImpl : public DB{
#endif
 public:
  DBImpl(const Options& options, const std::string& dbname);
  DBImpl(const Options& raw_options, const std::string& dbname,
         const std::string ub, const std::string lb);
  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;
  // Chuqing: Dbimpl外可以封装一层sharding那个文件里的
  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
//#ifdef BYTEADDRESSABLE
//  Iterator* NewSEQIterator(const ReadOptions&) override;
//#endif
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  // Chuqing: 
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
//  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);
  void CleanupSuperVersion(SuperVersion* sv);
  void ReturnAndCleanupSuperVersion(SuperVersion* sv);
  SuperVersion* GetThreadLocalSuperVersion();
  bool ReturnThreadLocalSuperVersion(SuperVersion* sv);
  void ResetThreadLocalSuperVersions();
  void InstallSuperVersion();
  void WaitforAllbgtasks(bool clear_mem) override;
  void SetTargetnodeid(uint8_t id){
    shard_target_node_id = id;
//    imm_.SetTargetnodeid(id);
  }
  // TODO: If there are two shards connected to the same memory node, what shall we
  // we do?
  void client_message_polling_and_handling_thread(std::string q_id);
  void WaitForComputeMessageHandlingThread(uint8_t target_memory_id,
                                              uint8_t shard_id_);
  std::string upper_bound;
  std::string lower_bound;
  // long double server_cpu_percent = 0.0;
//  void Wait_for_client_message_hanlding_setup();
 private:
  friend class DB;
//  struct CompactionState;
//  struct SubcompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };



  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);
//#ifdef BYTEADDRESSABLE
//  Iterator* NewInternalSEQIterator(const ReadOptions&,
//                                SequenceNumber* latest_snapshot,
//                                uint32_t* seed);
//#endif
  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable();
  void ForceCompactMemTable();
  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  Status WriteLevel0Table(FlushJob* job, VersionEdit* edit)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  Status WriteLevel0Table(MemTable* job, VersionEdit* edit, Version* base)
  EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  Status PickupTableToWrite(bool force, uint64_t seq_num, MemTable*& mem_r)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleFlushOrCompaction() EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  static void BGWork_Flush(void* thread_args);
  static void BGWork_Compaction(void* thread_args);
  void BackgroundCall();
  void BackgroundFlush(void* p);
  void BackgroundCompaction(void* p) EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  std::atomic<int> print_counter = 0;

  bool CheckWhetherPushDownorNot(Compaction* compact);
  bool CheckByteaddressableOrNot(Compaction* compact);
  long double RequestRemoteUtilization();
//  void ActivateRemoteCPURefresh();
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  void ProcessKeyValueCompaction(SubcompactionState* sub_compact);
  //TODO: We could probably use corotine to do the compaction because the compaction for
  // large key value size can have large cpu stall time for memroy copy.
  Status DoCompactionWorkWithSubcompaction(CompactionState* compact);
  Status OpenCompactionOutputFile(SubcompactionState* compact);
  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(SubcompactionState* compact,
                                    Iterator* input);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact,
                                  std::unique_lock<std::mutex>* lck_sv)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  Status TryInstallMemtableFlushResults(
      FlushJob* job, VersionSet* vset,
      std::shared_ptr<RemoteMemTableMetaData>& sstable, VersionEdit* edit);
//  SuperVersion* GetReferencedSuperVersion(DBImpl* db);

  void NearDataCompaction(Compaction* c);
//  void Communication_To_Home_Node();
  void Edit_sync_to_remote(VersionEdit* edit, uint8_t target_node_id);
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
  void sync_option_to_remote(uint8_t target_node_id);
  void remote_qp_reset(std::string& qp_type, uint8_t target_node_id);
  void install_version_edit_handler(RDMA_Request* request, std::string client_ip);
#ifdef WITHPERSISTENCE
  static void SSTable_Unpin_Dispatch(void* thread_args);
  void persistence_unpin_handler(void* arg);
#endif
  // Constant after construction
  Env* const env_;
  std::unordered_map<unsigned int, std::pair<std::mutex, std::condition_variable>> imm_notifier_pool;
//  unsigned int imm_temp = 1;
  // THose vairbale could be shared pointers from the out side.
  std::mutex* mtx_imme;
  std::atomic<uint32_t>* imm_gen;
  uint32_t* imme_data;
  uint32_t* byte_len;
  std::condition_variable* cv_imme;


  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;
  int byte_addressable_boundary= -1;
  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;
  std::atomic<bool> mem_switching;
  int thread_ready_num;
  // State below is protected by undefine_mutex
  // we could rename it as superversion mutex
  port::Mutex undefine_mutex;

//  port::Mutex write_stall_mutex_;
//  SpinMutex spin_memtable_switch_mutex;
  std::atomic<bool> shutting_down_;
  std::condition_variable write_stall_cv;
  int main_comm_thread_ready_num = 0;
  std::mutex FlushPickMTX;
  // THE Mutex will protect both memlist and the superversion pointer.
  std::mutex superversion_memlist_mtx;
  // TODO: use read write lock to control the version set mtx.
//  std::mutex versionset_mtx;
  bool locked = false;
//  bool check_and_clear_pending_recvWR = false;
//  SpinMutex LSMv_mtx;
  std::atomic<MemTable*> mem_;
//  std::atomic<MemTable*> imm_;  // Memtable being compacted
  MemTableList imm_;
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_;
  log::Writer* log_;
  std::mutex log_mtx;
  std::atomic<size_t> put_counter = 0;
  uint32_t seed_;  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;
  ThreadPool Unpin_bg_pool_;
  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
//  std::set<uint64_t> pending_outputs_;

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_;

  ManualCompaction* manual_compaction_;
  std::atomic<bool> slow_down_compaction = false;
  VersionSet* const versions_;
//  std::map<Slice, VersionSet*, cmpBySlice> const versions_pool;
  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  CompactionStats stats_[config::kNumLevels];
//  std::atomic<size_t> memtable_counter = 0;
//  std::atomic<size_t> kv_counter0 = 0;
//  std::atomic<size_t> kv_counter1 = 0;
  std::atomic<uint64_t> super_version_number_;
  SuperVersion* super_version;
//  std::unique_ptr<ThreadLocalPtr> local_sv_;
  ThreadLocalPtr* local_sv_;
  std::vector<std::thread> main_comm_threads;
  uint8_t shard_target_node_id = 0;
  uint8_t shard_id = 0;
  std::atomic<bool> level_0_compaction_in_progress = false;
  // Add for cpu utilization refreshing
  //TODO: (chuqing) if multiple servers
//  long double server_cpu_percent = 0.0;
//  std::map<uint16_t,uint16_t> remote_core_number_map;
//  std::map<uint16_t,uint16_t> compute_core_number_map;
  //TODO(chuqing): add for count time, need a better calculator
  long int accumulated_time = 0;

#ifdef PROCESSANALYSIS
  std::atomic<size_t> Total_time_elapse;
  std::atomic<size_t> flush_times;
#endif

};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_DB_DB_IMPL_H_
