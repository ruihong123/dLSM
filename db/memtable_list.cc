//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/memtable_list.h"

#include <cinttypes>
#include <limits>
#include <queue>
#include <string>
#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/version_set.h"
#include "TimberSaw/db.h"
#include "TimberSaw/env.h"
#include "TimberSaw/iterator.h"
#include "util/coding.h"
#include "table/merger.h"
#include "db/table_cache.h"
#include "table/table_builder_computeside.h"
#include "table/table_builder_bacs.h"
namespace TimberSaw {

std::mutex MemTableList::imm_mtx;

class InternalKeyComparator;
class Mutex;
class VersionSet;

void MemTableListVersion::AddMemTable(MemTable* m) {
//  std::cout <<"AddMemTable called thread ID is" <<std::this_thread::get_id() << std::endl;
  assert(std::find(memlist_.begin(), memlist_.end(), m) == memlist_.end());
  if (std::find(memlist_.begin(), memlist_.end(), m) != memlist_.end()){
    printf("Error: insert duplicated table to the memtable list");
  }
  memlist_.push_front(m);
  *parent_memtable_list_memory_usage_ += m->ApproximateMemoryUsage();
}

//void MemTableListVersion::UnrefMemTable(autovector<MemTable*>* to_delete,
//                                        MemTable* m) {
//  if (m->Unref()) {
//    to_delete->push_back(m);
//    assert(*parent_memtable_list_memory_usage_ >= m->ApproximateMemoryUsage());
//    *parent_memtable_list_memory_usage_ -= m->ApproximateMemoryUsage();
//  }
//}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage, const MemTableListVersion& old)
    : max_write_buffer_number_to_maintain_(
          old.max_write_buffer_number_to_maintain_),
      max_write_buffer_size_to_maintain_(
          old.max_write_buffer_size_to_maintain_),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {
  memlist_ = old.memlist_;
  for (auto& m : memlist_) {
    m->Ref();
  }

  memlist_history_ = old.memlist_history_;
  for (auto& m : memlist_history_) {
    m->Ref();
  }
}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage,
    int max_write_buffer_number_to_maintain,
    int64_t max_write_buffer_size_to_maintain)
    : max_write_buffer_number_to_maintain_(max_write_buffer_number_to_maintain),
      max_write_buffer_size_to_maintain_(max_write_buffer_size_to_maintain),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {}
//TODO: make refs_ a atomic pointer, all the refs_ should be atomic pointer in
// this project.  If we can make sure the reference counter can not be 0 then, we can
// avoid using lock for the reference and dereference
void MemTableListVersion::Ref() { ++refs_; }

// called by superversion::clean()
void MemTableListVersion::Unref(autovector<MemTable*>* to_delete) {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    //Unref all the memtable in the memtable list.
    for (const auto& m : memlist_) {
      m->Unref();
    }
    for (const auto& m : memlist_history_) {
      m->Unref();
    }
    delete this;
  }
}
MemTableList::~MemTableList(){
#ifdef PROCESSANALYSIS
  if (MemTable::GetNum.load() >0)
    printf("Memtable GET time statics is %zu, %zu, %zu, Memtable found Num %zu\n",
           MemTable::GetTimeElapseSum.load(), MemTable::GetNum.load(),
           MemTable::GetTimeElapseSum.load()/MemTable::GetNum.load(),
           MemTable::foundNum.load());
#endif
}
int MemTableList::NumNotFlushed() const {
  int size = static_cast<int>(current_.load()->memlist_.size());
  assert(num_flush_not_started_ <= size);
  return size;
}

int MemTableList::NumFlushed() const {
  return static_cast<int>(current_.load()->memlist_history_.size());
}

// Search all the memtables starting from the most recent one.
// Return the most recent value found, if any.
// Operands stores the list of merge operations to apply, so far.
bool MemTableListVersion::Get(const LookupKey& key, std::string* value,
                              Status* s) {
  return GetFromList(&memlist_, key, value, s);
}

//void MemTableListVersion::MultiGet(const ReadOptions& read_options,
//                                   MultiGetRange* range, ReadCallback* callback,
//                                   bool* is_blob) {
//  for (auto memtable : memlist_) {
//    memtable->MultiGet(read_options, range, callback, is_blob);
//    if (range->empty()) {
//      return;
//    }
//  }
//}

//bool MemTableListVersion::GetMergeOperands(
//    const LookupKey& key, Status* s, MergeContext* merge_context,
//    SequenceNumber* max_covering_tombstone_seq, const ReadOptions& read_opts) {
//  for (MemTable* memtable : memlist_) {
//    bool done = memtable->Get(key, /*value*/ nullptr, /*timestamp*/ nullptr, s,
//                              merge_context, max_covering_tombstone_seq,
//                              read_opts, nullptr, nullptr, false);
//    if (done) {
//      return true;
//    }
//  }
//  return false;
//}

//bool MemTableListVersion::GetFromHistory(
//    const LookupKey& key, std::string* value, std::string* timestamp, Status* s,
//    MergeContext* merge_context, SequenceNumber* max_covering_tombstone_seq,
//    SequenceNumber* seq, const ReadOptions& read_opts, bool* is_blob_index) {
//  return GetFromList(&memlist_history_, key, value, timestamp, s, merge_context,
//                     max_covering_tombstone_seq, seq, read_opts,
//                     nullptr /*read_callback*/, is_blob_index);
//}

bool MemTableListVersion::GetFromList(std::list<MemTable*>* list,
                                      const LookupKey& key, std::string* value,
                                      Status* s) {
//#ifdef GETANALYSIS
//  auto start = std::chrono::high_resolution_clock::now();
//#endif
  for (auto& memtable : *list) {
    SequenceNumber current_seq = kMaxSequenceNumber;

    bool done = memtable->Get(key, value, s);

    if (done) {
      return true;
    }
    if (!done && !s->ok() && !s->IsNotFound()) {
      return false;
    }
  }
  return false;
}

//Status MemTableListVersion::AddRangeTombstoneIterators(
//    const ReadOptions& read_opts, Arena* /*arena*/,
//    RangeDelAggregator* range_del_agg) {
//  assert(range_del_agg != nullptr);
//  // Except for snapshot read, using kMaxSequenceNumber is OK because these
//  // are immutable memtables.
//  SequenceNumber read_seq = read_opts.snapshot != nullptr
//                                ? read_opts.snapshot->GetSequenceNumber()
//                                : kMaxSequenceNumber;
//  for (auto& m : memlist_) {
//    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
//        m->NewRangeTombstoneIterator(read_opts, read_seq));
//    range_del_agg->AddTombstones(std::move(range_del_iter));
//  }
//  return Status::OK();
//}

void MemTableListVersion::AddIteratorsToVector(
    const ReadOptions& options, std::vector<Iterator*>* iterator_list,
    Arena* arena) {
  for (auto& m : memlist_) {
    iterator_list->push_back(m->NewIterator());
  }
}
MemTable* MemTableListVersion::PickMemtablesSeqBelong(size_t seq) {
  for(auto iter : memlist_){
    if (seq >= iter->GetFirstseq() && seq <= iter->Getlargest_seq_supposed()) {
      return iter;
    }
  }
  return nullptr;
}
//void MemTableListVersion::AddIterators(
//    const ReadOptions& options, MergeIteratorBuilder* merge_iter_builder) {
//  for (auto& m : memlist_) {
//    merge_iter_builder->AddIterator(
//        m->NewIterator(options, merge_iter_builder->GetArena()));
//  }
//}
// The number of sequential number
uint64_t MemTableListVersion::GetTotalNumEntries() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->Get_seq_count();
  }
  return total_num;
}
void MemTableListVersion::AddIteratorsToList(
    std::vector<Iterator*>* list) {
//  int iter_num = memlist_.size();
  for (auto iter : memlist_) {
    this->Ref();
    list->push_back(iter->NewIterator());
  }
}

//MemTable::MemTableStats MemTableListVersion::ApproximateStats(
//    const Slice& start_ikey, const Slice& end_ikey) {
//  MemTable::MemTableStats total_stats = {0, 0};
//  for (auto& m : memlist_) {
//    auto mStats = m->ApproximateStats(start_ikey, end_ikey);
//    total_stats.size += mStats.size;
//    total_stats.count += mStats.count;
//  }
//  return total_stats;
//}

//uint64_t MemTableListVersion::GetTotalNumDeletes() const {
//  uint64_t total_num = 0;
//  for (auto& m : memlist_) {
//    total_num += m->num_deletes();
//  }
//  return total_num;
//}

//SequenceNumber MemTableListVersion::GetEarliestSequenceNumber(
//    bool include_history) const {
//  if (include_history && !memlist_history_.empty()) {
//    return memlist_history_.back()->GetEarliestSequenceNumber();
//  } else if (!memlist_.empty()) {
//    return memlist_.back()->GetEarliestSequenceNumber();
//  } else {
//    return kMaxSequenceNumber;
//  }
//}

// caller is responsible for referencing m
void MemTableListVersion::Add(MemTable* m) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  AddMemTable(m);
  TrimHistory(m->ApproximateMemoryUsage());
}

// Removes m from list of memtables not flushed.  Caller should NOT Unref m.
// we can remove the argument to_delete.
void MemTableListVersion::Remove(MemTable* m) {
  // THIS SHOULD be protected by a lock.

  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  memlist_.remove(m);

//  m->MarkFlushed();
  if (max_write_buffer_size_to_maintain_ > 0 ||
      max_write_buffer_number_to_maintain_ > 0) {
    memlist_history_.push_front(m);
    // Unable to get size of mutable memtable at this point, pass 0 to
    // TrimHistory as a best effort.
    TrimHistory(0);
  } else {
    //TOFIX: this unreference will trigger memtable garbage collection, if there
    // is a reader refering the tables, there could be problem. The solution could be
    // we do not unrefer the table here, we unref it outside
    // Answer: The statement above is incorrect, because the Unref here will not trigger the
    // contention. The reader will pin another memtable list version which will prohibit
    // the memtable from being deallocated.
    m->Unref();
    //TOThink: what is the parent_memtable_list_memory_usage used for?
    *parent_memtable_list_memory_usage_ -= m->ApproximateMemoryUsage();
  }
}

// return the total memory usage assuming the oldest flushed memtable is dropped
size_t MemTableListVersion::ApproximateMemoryUsageExcludingLast() const {
  size_t total_memtable_size = 0;
  for (auto& memtable : memlist_) {
    total_memtable_size += memtable->ApproximateMemoryUsage();
  }
  for (auto& memtable : memlist_history_) {
    total_memtable_size += memtable->ApproximateMemoryUsage();
  }
  if (!memlist_history_.empty()) {
    total_memtable_size -= memlist_history_.back()->ApproximateMemoryUsage();
  }
  return total_memtable_size;
}

bool MemTableListVersion::MemtableLimitExceeded(size_t usage) {
  if (max_write_buffer_size_to_maintain_ > 0) {
    // calculate the total memory usage after dropping the oldest flushed
    // memtable, compare with max_write_buffer_size_to_maintain_ to decide
    // whether to trim history
    return ApproximateMemoryUsageExcludingLast() + usage >=
           static_cast<size_t>(max_write_buffer_size_to_maintain_);
  } else if (max_write_buffer_number_to_maintain_ > 0) {
    return memlist_.size() + memlist_history_.size() >
           static_cast<size_t>(max_write_buffer_number_to_maintain_);
  } else {
    return false;
  }
}

// Make sure we don't use up too much space in history
bool MemTableListVersion::TrimHistory(size_t usage) {
  bool ret = false;
  while (MemtableLimitExceeded(usage) && !memlist_history_.empty()) {
    MemTable* x = memlist_history_.back();
    memlist_history_.pop_back();


    *parent_memtable_list_memory_usage_ -= x->ApproximateMemoryUsage();
    ret = true;
    x->Unref();
  }
  return ret;
}

// Returns true if there is at least one memtable on which flush has
// not yet started.
bool MemTableList::IsFlushPending() const {
  int num = num_flush_not_started_.load();
  return num >= config::Immutable_FlushTrigger;
}
bool MemTableList::IsFlushDoable() const {
  int num = num_flush_not_started_.load();
  return num > 0;
}
bool MemTableList::AllFlushNotFinished() const {
  return this->current_memtable_num_.load() >= config::Immutable_FlushTrigger;
}
// Returns the memtables that need to be flushed.
//Pick up a configurable number of memtable, not too much and not too less.2~4 could be better
void MemTableList::PickMemtablesToFlush(autovector<MemTable*>* mems) {
//  AutoThreadOperationStageUpdater stage_updater(
//      ThreadStatus::STAGE_PICK_MEMTABLES_TO_FLUSH);
  auto current = current_.load();// get a snapshot
  const auto& memlist = current->memlist_;
  bool atomic_flush = false;
  int table_counter = 0;
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    MemTable* m = *it;

    if (m->CheckFlush_Requested()) {
      //The pickmemtable should never see the flush finished table.
//      assert(!m->CheckFlushFinished());
      num_flush_not_started_.fetch_sub(1);
      m->SetFlushState(MemTable::FLUSH_PROCESSING);
//      if (num_flush_not_started_ == 0) {
//        imm_flush_needed.store(false, std::memory_order_release);
//      }
//      m->flush_in_progress_ = true;  // flushing will start very soon
      mems->push_back(m);
      // at most pick 2 table and do the merge
      if(++table_counter >= TimberSaw::config::MaxImmuNumPerFlush)
        break;
    }
  }
  DEBUG_arg("table picked is %d", table_counter);
  if (!atomic_flush || num_flush_not_started_ == 0) {
    flush_requested_ = false;  // start-flush request is complete
  }
}

MemTable* MemTableList::PickMemtablesSeqBelong(size_t seq) {
    return current_.load()->PickMemtablesSeqBelong(seq);
}


Iterator* MemTableList::MakeInputIterator(FlushJob* job) {
  int iter_num = job->mem_vec.size();
  auto** list = new Iterator*[iter_num];
  for (int i = 0; i<job->mem_vec.size(); i++) {
    list[i] = job->mem_vec[i]->NewIterator();
  }

  Iterator* result = NewMergingIterator(&cmp, list, iter_num);
  delete[] list;
  return result;
}

void MemTableList::RollbackMemtableFlush(const autovector<MemTable*>& mems,
                                         uint64_t /*file_number*/) {
//  AutoThreadOperationStageUpdater stage_updater(
//      ThreadStatus::STAGE_MEMTABLE_ROLLBACK);
  assert(!mems.empty());

  // If the flush was not successful, then just reset state.
  // Maybe a succeeding attempt to flush will be successful.
  for (MemTable* m : mems) {
    assert(m->CheckFlushInProcess());

    m->SetFlushState(MemTable::FLUSH_REQUESTED);
    num_flush_not_started_.fetch_add(1);
  }
  imm_flush_needed.store(true, std::memory_order_release);
}

// Try record a successful flush in the manifest file. It might just return
// Status::OK letting a concurrent flush to do actual the recording..
//Status MemTableList::TryInstallMemtableFlushResults(
//    FlushJob* job, VersionSet* vset,
//    std::shared_ptr<RemoteMemTableMetaData>& sstable, VersionEdit* edit) {
////  AutoThreadOperationStageUpdater stage_updater(
////      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
//  autovector<MemTable*> mems = job->mem_vec;
//  assert(mems.size() >0);
//  std::unique_lock<std::mutex> lck(*imm_mtx);
////  imm_mtx->lock();
//  if (mems.size() == 1)
//    DEBUG("check 1 memtable installation\n");
//  // Flush was successful
//  // Record the status on the memtable object. Either this call or a call by a
//  // concurrent flush thread will read the status and write it to manifest.
//  for (size_t i = 0; i < mems.size(); ++i) {
//    // First mark the flushing is finished in the immutables
//    mems[i]->MarkFlushed();
//    DEBUG_arg("Memtable %p marked as flushed\n", mems[i]);
//    mems[i]->sstable = sstable;
//  }
//
//  // if some other thread is already committing, then return
//  Status s;
//
//  // Only a single thread can be executing this piece of code
//  commit_in_progress_ = true;
//
//  // Retry until all completed flushes are committed. New flushes can finish
//  // while the current thread is writing manifest where mutex is released.
////  while (s.ok()) {
//  auto& memlist = current_.load()->memlist_;
//  // The back is the oldest; if flush_completed_ is not set to it, it means
//  // that we were assigned a more recent memtable. The memtables' flushes must
//  // be recorded in manifest in order. A concurrent flush thread, who is
//  // assigned to flush the oldest memtable, will later wake up and does all
//  // the pending writes to manifest, in order.
//  if (memlist.empty() || !memlist.back()->CheckFlushFinished()) {
//    //Unlock the spinlock and do not write to the version
////      imm_mtx->unlock();
//    commit_in_progress_ = false;
//    return s;
//  }
//  // scan all memtables from the earliest, and commit those
//  // (in that order) that have finished flushing. Memtables
//  // are always committed in the order that they were created.
//  uint64_t batch_file_number = 0;
//  size_t batch_count = 0;
////    autovector<VersionEdit*> edit_list;
////    autovector<MemTable*> memtables_to_flush;
//  // enumerate from the last (earliest) element to see how many batch finished
//  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
//    MemTable* m = *it;
//    if (!m->CheckFlushFinished()) {
//      break;
//    }
//    assert(m->sstable != nullptr);
//    edit->AddFileIfNotExist(0,m->sstable);
//    batch_count++;
//
////    if (batch_count == 3){
////      DEBUG("check equals 3\n");
////      printf("Memtable list install 3 tables");
////    }
////    if (batch_count == 4)
////      DEBUG("check equals 4\n");
//  }
//
//  // TODO(myabandeh): Not sure how batch_count could be 0 here.
//
//  DEBUG("try install inner loop\n");
//  // we will be changing the version in the next code path,
//  // so we better create a new one, since versions are immutable
//  InstallNewVersion();
//  size_t batch_count_for_fetch_sub = batch_count;
//  while (batch_count-- > 0) {
//    MemTable* m = current_.load()->memlist_.back();
//
//    assert(m->sstable != nullptr);
//    autovector<MemTable*> dummy_to_delete = autovector<MemTable*>();
//    current_.load()->Remove(m);
//    UpdateCachedValuesFromMemTableListVersion();
//    ResetTrimHistoryNeeded();
//  }
//  current_memtable_num_.fetch_sub(batch_count_for_fetch_sub);
//  DEBUG_arg("Install flushing result, current immutable number is %lu\n", current_memtable_num_.load());
//
//
//
//  s = vset->LogAndApply(edit);
//  lck.unlock();
//  job->write_stall_cv_->notify_all();
//
////  }
//
//  commit_in_progress_ = false;
//  return s;
//}

// New memtables are inserted at the front of the list.
// This should be guarded by imm_mtx
void MemTableList::Add(MemTable* m) {
  assert(static_cast<int>(current_.load()->memlist_.size()) >= num_flush_not_started_);
  InstallNewVersion();
  // this method is used to move mutable memtable into an immutable list.
  // since mutable memtable is already refcounted by the DBImpl,
  // and when moving to the imutable list we don't unref it,
  // we don't have to ref the memtable here. we just take over the
  // reference from the DBImpl.
  current_.load()->Add(m);
  // Add memtable number atomically.
  current_memtable_num_.fetch_add(1);
  DEBUG_arg("Add a new file, current immtable number is %lu", current_memtable_num_.load());
  m->SetFlushState(MemTable::FLUSH_REQUESTED);
  num_flush_not_started_.fetch_add(1);
  if (num_flush_not_started_ == 1) {
    imm_flush_needed.store(true, std::memory_order_release);
  }
  UpdateCachedValuesFromMemTableListVersion();
  ResetTrimHistoryNeeded();
}

bool MemTableList::TrimHistory(autovector<MemTable*>* to_delete, size_t usage) {
  InstallNewVersion();
  bool ret = current_.load()->TrimHistory(usage);
  UpdateCachedValuesFromMemTableListVersion();
  ResetTrimHistoryNeeded();
  return ret;
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateUnflushedMemTablesMemoryUsage() {
  size_t total_size = 0;
  for (auto& memtable : current_.load()->memlist_) {
    total_size += memtable->ApproximateMemoryUsage();
  }
  return total_size;
}

size_t MemTableList::ApproximateMemoryUsage() { return current_memory_usage_; }

size_t MemTableList::ApproximateMemoryUsageExcludingLast() const {
  const size_t usage =
      current_memory_usage_excluding_last_.load(std::memory_order_relaxed);
  return usage;
}

bool MemTableList::HasHistory() const {
  const bool has_history = current_has_history_.load(std::memory_order_relaxed);
  return has_history;
}

void MemTableList::UpdateCachedValuesFromMemTableListVersion() {
  const size_t total_memtable_size =
      current_.load()->ApproximateMemoryUsageExcludingLast();
  current_memory_usage_excluding_last_.store(total_memtable_size,
                                             std::memory_order_relaxed);

  const bool has_history = current_.load()->HasHistory();
  current_has_history_.store(has_history, std::memory_order_relaxed);
}

//uint64_t MemTableList::ApproximateOldestKeyTime() const {
//  if (!current_->memlist_.empty()) {
//    return current_->memlist_.back()->ApproximateOldestKeyTime();
//  }
//  return std::numeric_limits<uint64_t>::max();
//}

void MemTableList::InstallNewVersion() {
  if (current_.load()->refs_ == 1) {
    // we're the only one using the version, just keep using it
  } else {
    // somebody else holds the current version, we need to create new one
    MemTableListVersion* version = current_;
    current_.store(new MemTableListVersion(&current_memory_usage_, *version));
    current_.load()->Ref();
    version->Unref();
  }
}


//uint64_t MemTableList::PrecomputeMinLogContainingPrepSection(
//    const autovector<MemTable*>& memtables_to_flush) {
//  uint64_t min_log = 0;
//
//  for (auto& m : current_->memlist_) {
//    // Assume the list is very short, we can live with O(m*n). We can optimize
//    // if the performance has some problem.
//    bool should_skip = false;
//    for (MemTable* m_to_flush : memtables_to_flush) {
//      if (m == m_to_flush) {
//        should_skip = true;
//        break;
//      }
//    }
//    if (should_skip) {
//      continue;
//    }
//
//    auto log = m->GetMinLogContainingPrepSection();
//
//    if (log > 0 && (min_log == 0 || log < min_log)) {
//      min_log = log;
//    }
//  }
//
//  return min_log;
//}

// Commit a successful atomic flush in the manifest file.
//Status InstallMemtableAtomicFlushResults(
//    const autovector<MemTableList*>* imm_lists,
//    const autovector<MemTable*>*& mems_list, VersionSet* vset,
//    port::Mutex* mu, const autovector<RemoteMemTableMetaData*>& file_metas,
//    autovector<MemTable*>* to_delete, FSDirectory* db_directory,
//    LogBuffer* log_buffer) {
//  AutoThreadOperationStageUpdater stage_updater(
//      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
//  mu->AssertHeld();
//
//  size_t num = mems_list.size();
//  assert(cfds.size() == num);
//  if (imm_lists != nullptr) {
//    assert(imm_lists->size() == num);
//  }
//  for (size_t k = 0; k != num; ++k) {
//#ifndef NDEBUG
//    const auto* imm =
//        (imm_lists == nullptr) ? cfds[k]->imm() : imm_lists->at(k);
//    if (!mems_list[k]->empty()) {
//      assert((*mems_list[k])[0]->GetID() == imm->GetEarliestMemTableID());
//    }
//#endif
//    assert(nullptr != file_metas[k]);
//    for (size_t i = 0; i != mems_list[k]->size(); ++i) {
//      assert(i == 0 || (*mems_list[k])[i]->GetEdits()->NumEntries() == 0);
//      (*mems_list[k])[i]->SetFlushCompleted(true);
//      (*mems_list[k])[i]->SetFileNumber(file_metas[k]->fd.GetNumber());
//    }
//  }
//
//  Status s;
//
//  autovector<autovector<VersionEdit*>> edit_lists;
//  uint32_t num_entries = 0;
//  for (const auto mems : mems_list) {
//    assert(mems != nullptr);
//    autovector<VersionEdit*> edits;
//    assert(!mems->empty());
//    edits.emplace_back((*mems)[0]->GetEdits());
//    ++num_entries;
//    edit_lists.emplace_back(edits);
//  }
//  // Mark the version edits as an atomic group if the number of version edits
//  // exceeds 1.
//  if (cfds.size() > 1) {
//    for (auto& edits : edit_lists) {
//      assert(edits.size() == 1);
//      edits[0]->MarkAtomicGroup(--num_entries);
//    }
//    assert(0 == num_entries);
//  }
//
//  // this can release and reacquire the mutex.
//  s = vset->LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu,
//                        db_directory);
//
//  for (size_t k = 0; k != cfds.size(); ++k) {
//    auto* imm = (imm_lists == nullptr) ? cfds[k]->imm() : imm_lists->at(k);
//    imm->InstallNewVersion();
//  }
//
//  if (s.ok() || s.IsColumnFamilyDropped()) {
//    for (size_t i = 0; i != cfds.size(); ++i) {
//      if (cfds[i]->IsDropped()) {
//        continue;
//      }
//      auto* imm = (imm_lists == nullptr) ? cfds[i]->imm() : imm_lists->at(i);
//      for (auto m : *mems_list[i]) {
//        assert(m->GetFileNumber() > 0);
//        uint64_t mem_id = m->GetID();
//
//        const VersionEdit* const edit = m->GetEdits();
//        assert(edit);
//
//        if (edit->GetBlobFileAdditions().empty()) {
//          ROCKS_LOG_BUFFER(log_buffer,
//                           "[%s] Level-0 commit table #%" PRIu64
//                           ": memtable #%" PRIu64 " done",
//                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
//                           mem_id);
//        } else {
//          ROCKS_LOG_BUFFER(log_buffer,
//                           "[%s] Level-0 commit table #%" PRIu64
//                           " (+%zu blob files)"
//                           ": memtable #%" PRIu64 " done",
//                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
//                           edit->GetBlobFileAdditions().size(), mem_id);
//        }
//
//        imm->current_->Remove(m, to_delete);
//        imm->UpdateCachedValuesFromMemTableListVersion();
//        imm->ResetTrimHistoryNeeded();
//      }
//    }
//  } else {
//    for (size_t i = 0; i != cfds.size(); ++i) {
//      auto* imm = (imm_lists == nullptr) ? cfds[i]->imm() : imm_lists->at(i);
//      for (auto m : *mems_list[i]) {
//        uint64_t mem_id = m->GetID();
//
//        const VersionEdit* const edit = m->GetEdits();
//        assert(edit);
//
//        if (edit->GetBlobFileAdditions().empty()) {
//          ROCKS_LOG_BUFFER(log_buffer,
//                           "[%s] Level-0 commit table #%" PRIu64
//                           ": memtable #%" PRIu64 " failed",
//                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
//                           mem_id);
//        } else {
//          ROCKS_LOG_BUFFER(log_buffer,
//                           "[%s] Level-0 commit table #%" PRIu64
//                           " (+%zu blob files)"
//                           ": memtable #%" PRIu64 " failed",
//                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
//                           edit->GetBlobFileAdditions().size(), mem_id);
//        }
//
//        m->SetFlushCompleted(false);
//        m->SetFlushInProgress(false);
//        m->GetEdits()->Clear();
//        m->SetFileNumber(0);
//        imm->num_flush_not_started_++;
//      }
//      imm->imm_flush_needed.store(true, std::memory_order_release);
//    }
//  }
//
//  return s;
//}

//void MemTableList::RemoveOldMemTables(uint64_t log_number,
//                                      autovector<MemTable*>* to_delete) {
//  assert(to_delete != nullptr);
//  InstallNewVersion();
//  auto& memlist = current_->memlist_;
//  autovector<MemTable*> old_memtables;
//  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
//    MemTable* mem = *it;
//    if (mem->GetNextLogNumber() > log_number) {
//      break;
//    }
//    old_memtables.push_back(mem);
//  }
//
//  for (auto it = old_memtables.begin(); it != old_memtables.end(); ++it) {
//    MemTable* mem = *it;
//    current_->Remove(mem, to_delete);
//    --num_flush_not_started_;
//    if (0 == num_flush_not_started_) {
//      imm_flush_needed.store(false, std::memory_order_release);
//    }
//  }
//
//  UpdateCachedValuesFromMemTableListVersion();
//  ResetTrimHistoryNeeded();
//}

void FlushJob::Waitforpendingwriter() {
  size_t counter = 0;
  for (auto iter: mem_vec) {
    // why not manually set able_to_flush when we want a non full_table_flush
    while (iter->full_table_flush && !iter->able_to_flush.load()) {
      counter++;
      if (counter == 500) {
//        printf("signal all the wait threads\n");
        usleep(10);
        // wake up all front-end threads in case of false wait thread.
        write_stall_cv_->notify_all();
        counter = 0;
      }
    }
  }

}
void FlushJob::SetAllMemStateProcessing() {
  for (auto iter: mem_vec) {
    iter->SetFlushState(MemTable::FLUSH_PROCESSING);

  }
}
FlushJob::FlushJob(std::condition_variable* write_stall_cv,
                   const InternalKeyComparator* cmp)
    : write_stall_cv_(write_stall_cv),
      user_cmp(cmp){}
Status FlushJob::BuildTable(const std::string& dbname, Env* env,
                            const Options& options, TableCache* table_cache,
                            Iterator* iter,
                            const std::shared_ptr<RemoteMemTableMetaData>& meta,
                            IO_type type, uint8_t target_node_id) {
  Status s;
//  meta->file_size = 0;
  iter->SeekToFirst();
#ifndef NDEBUG
  int Not_drop_counter = 0;
  int number_of_key = 0;
#endif
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  if (iter->Valid()) {
#ifndef BYTEADDRESSABLE
    auto* builder = new TableBuilder_ComputeSide(options, type, target_node_id);
#endif
#ifdef BYTEADDRESSABLE
    auto* builder = new TableBuilder_BACS(options, type, target_node_id);
#endif
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
//      assert(key.data()[0] == '0');
      bool drop = false;
      if (!ParseInternalKey(key, &ikey)) {
        // Do not hide error keys
        current_user_key.clear();
        has_current_user_key = false;
        printf("Corrupt key value detected\n");
        s = Status::IOError("Corrupt key value detected\n");
        break;
      } else {
        if (!has_current_user_key ||
            user_cmp->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
          // First occurrence of this user key
          current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
          has_current_user_key = true;
          // this will result in the key not drop, next if will always be false because of the last_sequence_for_key.
        }else{
          drop = true;
        }
      }
#ifndef NDEBUG
      number_of_key++;
#endif
      if (!drop){
#ifndef NDEBUG
        Not_drop_counter++;
#endif
        builder->Add(key, iter->value());
      }

    }

    if (s.ok()) {
//      assert(key.data()[0] == '0');
      meta->largest.DecodeFrom(key);
    } else{
      delete builder;
      return s;
    }
#ifndef NDEBUG
    printf("For flush, Total number of key touched is %d, KV left is %d\n", number_of_key,
           Not_drop_counter);
#endif
    // Finish and check for builder errors

    s = builder->Finish();
    builder->get_datablocks_map(meta->remote_data_mrs);
    builder->get_dataindexblocks_map(meta->remote_dataindex_mrs);
    builder->get_filter_map(meta->remote_filter_mrs);


    meta->file_size = 0;

    for(auto iter : meta->remote_data_mrs){
      meta->file_size += iter.second->length;
    }
    meta->num_entries = builder->get_numentries();
    DEBUG_arg("SSTable size is %lu \n", meta->file_size);
    assert(builder->FileSize() == meta->file_size);
    delete builder;
//TOFIX: temporarily disable the verification of index block.
//#ifndef NDEBUG
//    sleep(10);
//#endif

    // The index checking below is supposed to be deleted.
//    if (s.ok()) {
//      // Verify that the table is usable
//      Iterator* it = table_cache->NewIterator(ReadOptions(), meta);
//      s = it->status();
////#ifndef NDEBUG
////      it->SeekToFirst();
////      size_t counter = 0;
////      while(it->Valid()){
////        counter++;
////        it->Next();
////      }
////      assert(counter = Not_drop_counter);
////#endif
//      delete it;
//    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

//  if (s.ok() && !meta->remote_data_mrs.empty()) {
//    // Keep it
//  } else {
//    env->RemoveFile(fname);
//  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
