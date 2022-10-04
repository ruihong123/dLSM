// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_TimberSaw_DB_VERSION_SET_H_
#define STORAGE_TimberSaw_DB_VERSION_SET_H_

#include "db/dbformat.h"
#include "db/version_edit.h"
#include <atomic>
#include <map>
#include <set>
#include <vector>

#include "port/port.h"
#include "port/thread_annotations.h"

namespace TimberSaw {

namespace log {
class Writer;
}

class Compaction;
 class FlushJob;
class Iterator;
class MemTable;
class TableBuilder_ComputeSide;
class TableCache;
class Version;
class VersionSet;
class WritableFile;
class TableBuilder;
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state = kNotFound;// set as not found as default value.
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace
// Callback from TableCache::Get()

static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    // TOTHINK: may be the parse internal key is too slow?
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {// if found mark as kFound
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}
// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);
class Level_Info{
 public:
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> files;
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> in_progress;
  double compaction_score_;

};
//class Subversion{
// public:
//  explicit Subversion(size_t version_id, std::shared_ptr<RDMA_Manager> rdma_mg);
//
//
//  ~Subversion();
//
// private:
//  size_t version_id_;
//  std::shared_ptr<RDMA_Manager> rdma_mg_;
//};
class Version {
 public:
//  Version(const std::shared_ptr<Subversion>& sub_version);
  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    std::shared_ptr<RemoteMemTableMetaData> seek_file;
    int seek_file_level;
  };
//  std::shared_ptr<Subversion> subversion;

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);
#ifdef BYTEADDRESSABLE
  void AddSEQIterators(const ReadOptions&, std::vector<Iterator*>* iters);
#endif
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref(int mark);
  // Required to be guarded by version_set_mtx, in case that there is reference
  // outside the control of superversion.
  void Unref(int mark);

  bool GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<std::shared_ptr<RemoteMemTableMetaData>>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return levels_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;
  // An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
  class LevelFileNumIterator : public Iterator {
   public:
    LevelFileNumIterator(const InternalKeyComparator& icmp,
                         const std::vector<std::shared_ptr<RemoteMemTableMetaData>>* flist)
        : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
    }
    bool Valid() const override {
//      if ( index_ >= flist_->size())
//        printf("Pause here\n");
      return index_ < flist_->size();
    }
    void Seek(const Slice& target) override {
      index_ = FindFile(icmp_, *flist_, target);
    }
    void SeekToFirst() override { index_ = 0; }
    void SeekToLast() override {
      index_ = flist_->empty() ? 0 : flist_->size() - 1;
    }
    void Next() override {
      assert(Valid());
      index_++;
#ifndef NDEBUG
      if (index_ >= flist_->size()){
        printf("file iterator invalid\n");
      }
#endif
    }
    void Prev() override {
      assert(Valid());
      if (index_ == 0) {
        index_ = flist_->size();  // Marks as invalid
      } else {
        index_--;
      }
    }
    Slice key() const override {
      assert(Valid());
      return (*flist_)[index_]->largest.Encode();
    }
    Slice value() const override {
      assert(Valid());
      EncodeFixed64(value_buf_, (*flist_)[index_]->number);
      EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
      return {value_buf_, sizeof(value_buf_)};
    }
    std::shared_ptr<RemoteMemTableMetaData> file_value() const {
      return (*flist_)[index_];
    }
    Status status() const override { return Status::OK(); }

   private:
    const InternalKeyComparator icmp_;
    const std::vector<std::shared_ptr<RemoteMemTableMetaData>>* const flist_;
    uint32_t index_;

    // Backing store for value().  Holds the file number and size.
    mutable char value_buf_[16];
  };
  double CompactionScore(int i);
  int CompactionLevel(int i);
  void print_version_content(){
    for (int i = 0; i < config::kNumLevels; ++i) {
      printf("Version level %d contain %zu files\n", i, levels_[i].size());
    }
  }
  std::shared_ptr<RemoteMemTableMetaData> FindFileByNumber(int level, uint64_t file_number, uint8_t node_id);
 private:
  friend class Compaction;
  friend class VersionSet;



  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1){}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;
#ifdef BYTEADDRESSABLE
  Iterator* NewConcatenatingSEQIterator(const ReadOptions&, int level) const;
#endif
  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, std::shared_ptr<RemoteMemTableMetaData>));

  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  std::atomic<int> refs_;          // Number of live refs to this version

  // List of files per level
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> levels_[config::kNumLevels];
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> in_progress[config::kNumLevels];
//  double score[config::kNumLevels];
  // Next file to compact based on seek stats.
  std::shared_ptr<RemoteMemTableMetaData> file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  std::array<double, config::kNumLevels - 1> compaction_score_;
  std::array<int, config::kNumLevels - 1> compaction_level_;
#ifndef NDENUG
  std::vector<int> ref_mark_collection;
  std::vector<int> unref_mark_collection;
  std::mutex this_version_mtx;
#endif

};

class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator* cmp,
             std::mutex* mtx);
//  VersionSet(const std::string& dbname, const Options* options,
//             TableCache* table_cache, const InternalKeyComparator* cmp,
//             std::mutex* mtx);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();
#ifndef NDEBUG
  int version_remain;
  int version_all;
#endif
#ifdef PROCESSANALYSIS
  static std::atomic<uint64_t> GetTimeElapseSum;
  static std::atomic<uint64_t> GetNum;

#endif
#ifdef WITHPERSISTENCE
  void Persistency_pin(VersionEdit* edit);
  void Persistency_unpin(uint64_t* array, size_t size);
#endif
  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  //TODO: Finalize is not required for the comppute node side.
  Status LogAndApply(VersionEdit* edit);


  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1); }
  uint64_t NewFileNumberBatch(size_t size) { return next_file_number_.fetch_add(size); }
  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    std::unique_lock<std::mutex> lck(*sv_mtx);
    if (next_file_number_.load() == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_.load(); }
  uint64_t LastSequence_nonatomic() const { return last_sequence_; }
  uint64_t AssignSequnceNumbers(size_t n){
    assert(n == 1);
    return last_sequence_.fetch_add(n);
  }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_.load());
    last_sequence_.store(s);
  }
  void SetLastSequence_nonatomic(uint64_t s) {
    assert(s >= last_sequence_.load());
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }
//  static bool check_compaction_state(std::shared_ptr<RemoteMemTableMetaData> sst);
  bool PickFileToCompact(int level, Compaction* c);
  // Pick level and mem_vec for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();


  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction mem_vec for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);
  Iterator* MakeInputIteratorMemoryServer(Compaction* c);
//  Iterator* NewIterator(std::shared_ptr<RemoteMemTableMetaData> f);
  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    //TODO(ruihong): make current_ an atomic variable if we do not want to have a lock for this
    // funciton.
    Version* v = current_;
    //TODO(ruihong): we may also need a lock for changing reading the compaction score.
    return (v->compaction_score_[0] >= 1) ;
    //TODO: keep the file_to compact in our implementation in the future.
//    || (v->file_to_compact_.get() != nullptr)
  }
  bool AllCompactionNotFinished() {

    Version* v = current_;
    //since the version are apply
//    Finalize(v);
//    || (v->file_to_compact_.get() != nullptr)
    return (v->compaction_score_[0] >= 1);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;
//  void Pin_Version_For_Compute();
//  bool Unpin_Version_For_Compute(size_t version_id);
  size_t version_id = 0;
  TableCache* const table_cache_;
  // Opened lazily
  WritableFile* descriptor_file;
  log::Writer* descriptor_log;
  std::mutex* sv_mtx;
  SpinMutex version_set_list;
  Slice upper_bound;
  Slice lower_bound;
 private:
  class Builder;

  friend class Compaction;
  friend class FlushJob;
  friend class Version;
  friend class SuperVersion;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& inputs1,
                 const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
//  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  std::atomic<uint64_t> next_file_number_;
  uint64_t manifest_file_number_;
  std::atomic<uint64_t> last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted
  std::map<uint64_t, std::shared_ptr<RemoteMemTableMetaData>> persistent_pinner_;
  std::mutex pinner_mtx;

  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  //TODO: make current_ an atomic variable.
//  std::atomic<Version*> current_;        // == dummy_versions_.prev_
  Version* current_;


  //TODO: make it spinmutex?
//  std::mutex* sv_mtx;

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_index_[config::kNumLevels];
//  std::map<size_t, Version*> memory_version_pinner;

};


// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }
  void SetLevel(int level) { level_ = level; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith mem_vec file at "level()+which" ("which" must be 0 or 1).
  std::shared_ptr<RemoteMemTableMetaData> input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single mem_vec file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all mem_vec to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);
  void DecodeFrom(const Slice src, int side);
  void EncodeTo(std::string* dst);
  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);
  uint64_t FirstLevelSize();
  // Release the mem_vec version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();
  void GenSubcompactionBoundaries();

  std::vector<std::shared_ptr<RemoteMemTableMetaData>> inputs_[2];  // The two sets of mem_vec
  std::vector<Slice>* GetBoundaries();
  std::vector<uint64_t>* GetSizes();
  uint64_t GetFileSizesForLevel(int level);
  Compaction(const Options* options);

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);
  int level_;
  const Options* opt_ptr;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads mem_vec from "level_" and "level_+1"

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
  // Stores the Slices that designate the boundaries for each subcompaction
  std::vector<Slice> boundaries_;
  // Stores the approx size of keys covered in the range of each subcompaction
  std::vector<uint64_t> sizes_;
};
struct CompactionOutput {
  uint64_t number;
  uint64_t file_size;
  InternalKey smallest, largest;
  std::map<uint32_t , ibv_mr*> remote_data_mrs;
  std::map<uint32_t , ibv_mr*> remote_dataindex_mrs;
  std::map<uint32_t , ibv_mr*> remote_filter_mrs;
};
struct SubcompactionState {
  Compaction* const compaction;

  // The boundaries(UserKey) of the key-range this compaction is interested in. No two
  // subcompactions may have overlapping key-ranges.
  //TODO: double check the statement below
  // 'start' is exclusive, 'end' is inclusive, and nullptr means unbounded
  Slice *start, *end;

  // The return status of this subcompaction
  Status status;



  // State kept for output being generated
  std::vector<CompactionOutput> outputs;
  TableBuilder* builder = nullptr;

  CompactionOutput* current_output() {
    if (outputs.empty()) {
      // This subcompaction's output could be empty if compaction was aborted
      // before this subcompaction had a chance to generate any output files.
      // When subcompactions are executed sequentially this is more likely and
      // will be particulalry likely for the later subcompactions to be empty.
      // Once they are run in parallel however it should be much rarer.
      return nullptr;
    } else {
      return &outputs.back();
    }
  }
  SequenceNumber smallest_snapshot;
  uint64_t current_output_file_size = 0;

  // State during the subcompaction
  uint64_t total_bytes = 0;
  uint64_t num_output_records = 0;

  uint64_t approx_size = 0;
  // An index that used to speed up ShouldStopBefore().
  size_t grandparent_index = 0;
  // The number of bytes overlapping between the current output and
  // grandparent files used in ShouldStopBefore().
  uint64_t overlapped_bytes = 0;
  // A flag determine whether the key has been seen in ShouldStopBefore()
  bool seen_key = false;

  SubcompactionState(Compaction* c, Slice* _start, Slice* _end, uint64_t size)
  : compaction(c), start(_start), end(_end), approx_size(size) {
    assert(compaction != nullptr);
  }
};
struct CompactionState {
  // Files produced by compaction

  CompactionOutput* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
  : compaction(c),
  smallest_snapshot(0),
  //        outfile(nullptr),
  builder(nullptr),
  total_bytes(0) {}

  std::vector<SubcompactionState> sub_compact_states;
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<CompactionOutput> outputs;

  // State kept for output being generated
  //  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};
// Per level compaction stats.  stats_[level] stores the stats for
// compactions that produced data for the specified "level".
struct CompactionStats {
  CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

  void Add(const CompactionStats& c) {
    this->micros += c.micros;
    this->bytes_read += c.bytes_read;
    this->bytes_written += c.bytes_written;
  }

  int64_t micros;
  int64_t bytes_read;
  int64_t bytes_written;
};
}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_DB_VERSION_SET_H_
