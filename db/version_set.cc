// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
//#include "db/dbformat.h"
#include <algorithm>
#include <cstdio>

#include "TimberSaw/env.h"

#include "table/merger.h"
#include "table/table_builder_computeside.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace TimberSaw {
#ifdef PROCESSANALYSIS
std::atomic<uint64_t> VersionSet::GetTimeElapseSum = 0;
std::atomic<uint64_t> VersionSet::GetNum = 0;
#endif
//std::mutex VersionSet::version_set_mtx;

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = config::max_mega_bytes_for_level_base * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  // TODO: make a version set lock here, so that we do not need any lock for just a unref.
  assert(refs_.load() == 0);
  vset_->version_set_list.lock();
  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;
  vset_->version_set_list.unlock();

  DEBUG("version garbage collected.\n");
  // Drop references to files
//#ifndef NDEBUG
//  for (int level = 0; level < config::kNumLevels; level++) {
//    for (size_t i = 0; i < levels_[level].size(); i++) {
//      std::shared_ptr<RemoteMemTableMetaData> f = levels_[level][i];
////      printf("The file %zu in level %d 's use_count is %ld\n", i,level, f.use_count());
//    }
//  }
//#endif
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const std::shared_ptr<RemoteMemTableMetaData> f = files[mid];
    assert(f->largest.user_key().size() > 0);
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
//  assert(right!= files.size()); //todo: remove this assert.
  return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const std::shared_ptr<RemoteMemTableMetaData> f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const std::shared_ptr<RemoteMemTableMetaData> f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const std::shared_ptr<RemoteMemTableMetaData> f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}



static Iterator* GetFileIterator(
    void* arg, const ReadOptions& options,
    std::shared_ptr<RemoteMemTableMetaData> remote_table) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
    return cache->NewIterator(options, remote_table);
}
#ifdef BYTEADDRESSABLE
static Iterator* GetFileSEQIterator(
    void* arg, const ReadOptions& options,
    std::shared_ptr<RemoteMemTableMetaData> remote_table) {
  assert(remote_table.use_count()>=0);
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  return cache->NewSEQIterator(options, remote_table);
}
#endif
static Iterator* GetFileIterator_Memoryside(
    void* arg, const ReadOptions& options,
    std::shared_ptr<RemoteMemTableMetaData> remote_table) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  return cache->NewIterator_MemorySide(options, remote_table);
}


Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelFileIterator(
      new LevelFileNumIterator(vset_->icmp_, &levels_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}
#ifdef BYTEADDRESSABLE
Iterator* Version::NewConcatenatingSEQIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelFileIterator(
      new LevelFileNumIterator(vset_->icmp_, &levels_[level]), &GetFileSEQIterator,
      vset_->table_cache_, options);
}
#endif
//Subversion::Subversion(size_t version_id,
//                       std::shared_ptr<RDMA_Manager> rdma_mg) : version_id_(version_id), rdma_mg_(rdma_mg){}
//Subversion::~Subversion(){
//  RDMA_Request* send_pointer;
//  ibv_mr send_mr = {};
//
//  rdma_mg_->Allocate_Local_RDMA_Slot(send_mr, "message");
//
//  send_pointer = (RDMA_Request*)send_mr.addr;
//  send_pointer->command = version_unpin_;
//  send_pointer->content.unpinned_version_id = version_id_;
//  rdma_mg_->post_send<RDMA_Request>(&send_mr, std::string("main"));
//  ibv_wc wc[2] = {};
//  if (rdma_mg_->poll_completion(wc, 1, std::string("main"),true)){
//    fprintf(stderr, "failed to poll send for remote memory register\n");
//    exit(0);
//  }
//  rdma_mg_->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
//}
//Version::Version(const std::shared_ptr<Subversion>& sub_version)
//    : subversion(sub_version) {}
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < levels_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, levels_[0][i]));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!levels_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}
#ifdef BYTEADDRESSABLE
void Version::AddSEQIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < levels_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewSEQIterator(
        options, levels_[0][i]));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!levels_[level].empty()) {
      iters->push_back(NewConcatenatingSEQIterator(options, level));
    }
  }
}
#endif

static bool NewestFirst(std::shared_ptr<RemoteMemTableMetaData> a, std::shared_ptr<RemoteMemTableMetaData> b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int,
                                              std::shared_ptr<RemoteMemTableMetaData>)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> tmp;
  tmp.reserve(levels_[0].size());
  for (uint32_t i = 0; i < levels_[0].size(); i++) {
    std::shared_ptr<RemoteMemTableMetaData> f = levels_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = levels_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, levels_[level], internal_key);
    if (index < num_files) {
      std::shared_ptr<RemoteMemTableMetaData> f = levels_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
#ifdef PROCESSANALYSIS
  auto start = std::chrono::high_resolution_clock::now();
#endif
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  struct State {
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    std::shared_ptr<RemoteMemTableMetaData> last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    static bool Match(void* arg, int level, std::shared_ptr<RemoteMemTableMetaData> f) {
      State* state = reinterpret_cast<State*>(arg);

      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;
      state->last_file_read_level = level;

      state->s = state->vset->table_cache_->Get(*state->options, f,
          state->ikey, &state->saver, SaveValue);
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);
#ifdef PROCESSANALYSIS
  if (!state.found){
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    std::printf("Get from SSTables (not found) time elapse is %zu\n",  duration.count());
    VersionSet::GetTimeElapseSum.fetch_add(duration.count());
    VersionSet::GetNum.fetch_add(1);
  }

#endif
  return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  std::shared_ptr<RemoteMemTableMetaData> f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, std::shared_ptr<RemoteMemTableMetaData> f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref(int mark) {
#ifndef NDEBUG
  this_version_mtx.lock();
  if (std::find(ref_mark_collection.begin(), ref_mark_collection.end(), mark) != ref_mark_collection.end())
    printf("mark in the ref\n");
  ref_mark_collection.push_back(mark);
  this_version_mtx.unlock();
#endif


  refs_.fetch_add(1);
}

void Version::Unref(int mark) {
#ifndef NDEBUG
  this_version_mtx.lock();
  assert(this != &vset_->dummy_versions_);
  assert(refs_.load() >= 1);
  unref_mark_collection.push_back(mark);
  this_version_mtx.unlock();
#endif
  refs_.fetch_sub(1);
  if (refs_.load() == 0) {
    DEBUG("Version get garbage collected\n");
#ifndef NDEBUG
    vset_->version_remain--;
#endif

    delete this;
  }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), levels_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<std::shared_ptr<RemoteMemTableMetaData>> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
bool Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<std::shared_ptr<RemoteMemTableMetaData>>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < levels_[level].size();) {
    std::shared_ptr<RemoteMemTableMetaData> f = levels_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else if(f->UnderCompaction) {
      return false;
    }else{
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        //TOthink: this two if may never be triggered because the user_begin
        // and user_end contain the whole range of level 0.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }

    }
  }
  return true;
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files =
        levels_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}
double Version::CompactionScore(int i) { return compaction_score_[i]; }
int Version::CompactionLevel(int i) { return compaction_level_[i]; }
std::shared_ptr<RemoteMemTableMetaData> Version::FindFileByNumber(int level, uint64_t file_number, uint8_t node_id) {
  for(auto iter :levels_[level]){
    if (iter->number == file_number && iter->creator_node_id == node_id)
      return iter;
  }
  assert(false);
  return std::shared_ptr<RemoteMemTableMetaData>();
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->levels_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(std::shared_ptr<RemoteMemTableMetaData> f1,
                    std::shared_ptr<RemoteMemTableMetaData> f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<std::shared_ptr<RemoteMemTableMetaData>, BySmallestKey> FileSet;
  struct LevelState {
    std::multimap<uint64_t, uint8_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];
#ifndef NDEBUG
  int number_deleted = 0;
#endif


 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref(3);
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
//      std::vector<std::shared_ptr<RemoteMemTableMetaData>> to_unref;
//      to_unref.reserve(added->size());
//      for (FileSet::const_iterator it = added->begin(); it != added->end();
//           ++it) {
//        to_unref.push_back(*it);
//      }
      delete added;
//      for (uint32_t i = 0; i < to_unref.size(); i++) {
//        std::shared_ptr<RemoteMemTableMetaData> f = to_unref[i];
//        f->refs--;
//        if (f->refs <= 0) {
//          delete f;
//        }
//      }
    }
    base_->Unref(3);
  }

  // Apply all of the edits in *edit to the current state.
  //TODO(ruihong): change it back to no current validation.
  void Apply(VersionEdit* edit, Version* current) {
    assert(current = base_);
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_index_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = std::get<0>(deleted_file_set_kvp);
      const uint64_t number = std::get<1>(deleted_file_set_kvp);
      const uint8_t node_id = std::get<2>(deleted_file_set_kvp);
      levels_[level].deleted_files.insert({number, node_id});
//      printf("level %d, number %lu\n", level, number);
    }
//    printf("Apply: level 0 deleted file size %lu\n", levels_[0].deleted_files.size());
//    printf("Apply: level 1 deleted file size %lu\n", levels_[1].deleted_files.size());
    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      std::shared_ptr<RemoteMemTableMetaData> f = edit->new_files_[i].second;

      assert(level == f->level);
      assert(f.get()!= nullptr);
//      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;
      //Tothink: Why we have delete file here
      if (levels_[level].deleted_files.find(f->number)!= levels_[level].deleted_files.end()){
        printf("Look at here\n");
      }
      std::pair <std::multimap<uint64_t, uint8_t>::iterator, std::multimap<uint64_t ,uint8_t>::iterator>
      ret = levels_[level].deleted_files.equal_range(f->number);
      for (std::multimap<uint64_t, uint8_t>::iterator it=ret.first; it!=ret.second; ++it){
        if (it->second == f->creator_node_id)
          levels_[level].deleted_files.erase(it);
      }
      levels_[level].added_files->insert(f);
      //TODO: Why deleted file will be be remove from deletedfiles if it exist in added file

//      printf("Apply2: level 1 deleted file size %lu\n", levels_[1].deleted_files.size());
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
//    printf("SaveTo: level 1 deleted file size %lu\n", levels_[1].deleted_files.size());
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& base_files = base_->levels_[level];
//      const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& base_in_progress = base_->levels_[level];
      std::vector<std::shared_ptr<RemoteMemTableMetaData>>::const_iterator base_iter = base_files.begin();
      std::vector<std::shared_ptr<RemoteMemTableMetaData>>::const_iterator base_end = base_files.end();
      const FileSet* added_files = levels_[level].added_files;
      v->levels_[level].reserve(base_files.size() + added_files->size());
      //TOTHINK: how could this make sure the order in level 0.
      // Answer: they are not organized by order, instead the organized by key,
      // but whensearcg level 0 the reader will order the table be filenumber and then
      // iterate in the order of file number.
      // All the levels are oganized by key smallest key order
#ifndef NDEBUG
      if (!levels_[level].deleted_files.empty())
        printf("contain deleted file at level %d\n", level);
#endif
      for (const auto& added_file : *added_files) {
        //Mark the file as not under compaction.
        added_file->UnderCompaction = false;
        // Add all smaller files listed in base_
        for (std::vector<std::shared_ptr<RemoteMemTableMetaData>>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          //Ruihong: why the builder will push back the base_iter to the level?
          //Because the code tries to build the version from the scratch
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG

      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->levels_[level].size(); i++) {
          const InternalKey& prev_end = v->levels_[level][i - 1]->largest;
          const InternalKey& this_begin = v->levels_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
#ifndef NDEBUG
    int deleted_file_num_supposed = 0;
    for (int level = 0; level < config::kNumLevels; level++) {
      deleted_file_num_supposed += levels_[level].deleted_files.size();
    }
    assert(number_deleted == deleted_file_num_supposed);
#endif
  }

  void MaybeAddFile(Version* v, int level, std::shared_ptr<RemoteMemTableMetaData> f) {

    std::pair <std::multimap<uint64_t, uint8_t>::iterator, std::multimap<uint64_t ,uint8_t>::iterator>
        ret = levels_[level].deleted_files.equal_range(f->number);
    bool file_number_deleted = false;
    for (std::multimap<uint64_t, uint8_t>::iterator it=ret.first; it!=ret.second; ++it){
      if (it->second == f->creator_node_id){
        file_number_deleted = true;
#ifndef NDEBUG
        number_deleted++;
#endif
      }


    }

    if (file_number_deleted) {
      // File is deleted: do nothing
//#ifndef NDEBUG
//      printf("file NUM %lu get deleted.\n", f->number);
//#endif
    } else {
      std::vector<std::shared_ptr<RemoteMemTableMetaData>>* files = &v->levels_[level];
      std::vector<std::shared_ptr<RemoteMemTableMetaData>>* in_progresses = &v->in_progress[level];


      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
#ifndef NDEBUG
      if (level == 0 && !files->empty()){
        for(const auto& existed_f : *files){
          assert(existed_f->number!= f->number );
        }
      }
      if (!files->empty()){
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->smallest,
                                    f->smallest) != 0);//in case there is edit installed twice.
      }
#endif
//      f->refs++;
      files->push_back(f);
      if (f->UnderCompaction){
        in_progresses->push_back(f);
      }
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp, std::mutex* mtx)
    : table_cache_(table_cache),
      descriptor_file(nullptr),
      descriptor_log(nullptr),
//      upper_bound(ubound),
//      lower_bound(lbound),
      env_(options->env),
      dbname_(dbname),
      options_(options),
      icmp_(*cmp),  // Filled by Recover()
      next_file_number_(2),
      manifest_file_number_(0),
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      dummy_versions_(this),
      current_(nullptr),
      sv_mtx(mtx){
  AppendVersion(new Version(this));

}
//VersionSet::VersionSet(const std::string& dbname, const Options* options,
//                       TableCache* table_cache,
//                       const InternalKeyComparator* cmp, std::mutex* mtx)
//    : table_cache_(table_cache),
//      descriptor_file(nullptr),
//      descriptor_log(nullptr),
//      env_(options->env),
//      dbname_(dbname),
//      options_(options),
//      icmp_(*cmp),  // Filled by Recover()
//      next_file_number_(2),
//      manifest_file_number_(0),
//      last_sequence_(0),
//      log_number_(0),
//      prev_log_number_(0),
//      dummy_versions_(this),
//      current_(nullptr),
//      sv_mtx(mtx){
//  AppendVersion(new Version(this));
//
//}

VersionSet::~VersionSet() {
  current_->Unref(0);
//  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log;
  delete descriptor_file;
#ifdef PROCESSANALYSIS
  if (VersionSet::GetNum.load() >0)
    printf("LSM Version GET time statistics is %zu, %zu, %zu\n",
           VersionSet::GetTimeElapseSum.load(), VersionSet::GetNum.load(),
           VersionSet::GetTimeElapseSum.load()/VersionSet::GetNum.load());
#endif
//  current_->print_version_content();
#ifndef NDEBUG
  printf("remained versuins number is %d", version_remain);
#endif

}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_.load() == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref(1);
  }
  current_ = v;
  v->Ref(1);
#ifndef NDEBUG
  version_remain++;
  version_all++;
#endif

  // Append to linked list
  version_set_list.lock();
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
  version_set_list.unlock();
}
#ifdef WITHPERSISTENCE
void VersionSet::Persistency_pin(VersionEdit* edit) {
  std::unique_lock<std::mutex> lck(pinner_mtx);
  for(auto iter : *edit->GetNewFiles()){
    persistent_pinner_.insert({iter.second->number, iter.second});
//    printf("pin sstable %lu", iter.second->number);
  }

  assert(persistent_pinner_.size() <= 256);
}
void VersionSet::Persistency_unpin(uint64_t* array, size_t size){
  std::unique_lock<std::mutex> lck(pinner_mtx);
  for (int i = 0; i < size; ++i) {
    assert(persistent_pinner_.find(array[i]) != persistent_pinner_.end());
//    printf("Unpin sstable %lu", array[i]);
    persistent_pinner_.erase(array[i]);
  }
}
#endif
Status VersionSet::LogAndApply(VersionEdit* edit) {
//  if (edit->has_log_number_) {
//    assert(edit->log_number_ >= log_number_);
//    assert(edit->log_number_ < next_file_number_.load());
//  } else {
//    edit->SetLogNumber(log_number_);
//  }
//
//  if (!edit->has_prev_log_number_) {
//    edit->SetPrevLogNumber(prev_log_number_);
//  }
//
//  edit->SetNextFile(next_file_number_.load());
//#ifndef NDEBUG
//  for (int i = 0; i < edit->GetNewFilesNum(); ++i) {
//    printf("file number for this flush or compaction version edit installation : %lu \n", (*edit->GetNewFiles())[i].second->number);
//  }
//#endif


//  for(auto iter : *edit->GetNewFiles()){
//        if (iter.second->table_cache == nullptr)
//          iter.second->table_cache = table_cache_;
//  }

  edit->SetLastSequence(last_sequence_);
  Version* v;
  v = new Version(this);
#ifdef WITHPERSISTENCE
  Persistency_pin(edit);
#endif
  //Build an empty version.
//  if (remote_subversion_id == 0){
//    v = new Version(this);
//#ifndef NDEBUG
//    printf("sub version for the new version is %p", current_->subversion.get());
////    if (current_->subversion.get() != nullptr){
//////      printf("version id for this subversion is %lu", current_->subversion);
////    }
//#endif
//  }else{
//    //TODO: how to let the function know whether it is memory node or compute node.
//    assert(Env::Default() != nullptr);
//    std::shared_ptr<Subversion> subverison = std::make_shared<Subversion>(
//        remote_subversion_id, Env::Default()->rdma_mg);
//    v = new Version(this);
//#ifndef NDEBUG
//    printf("sub version for the new version is %p", v->subversion.get());
//    if (v->subversion.get() != nullptr){
//      printf("version id for this subversion is %lu", remote_subversion_id);
//    }
//#endif
//  }

  //TOTHINK: may be we can move the lock before the Finalize(v);
  // create a new verison and then destroy it should be atomic.
//  lck_vs->lock();

  {
    // Decide what table to keep what to discard.
    Builder builder(this, current_);
    // apply to the new version, no need to apply delete files, only add
    // alive files and new files to the new version just created
    builder.Apply(edit, current_);
    builder.SaveTo(v);
  }

  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
//  std::string new_manifest_file;
  Status s = Status::OK();
//  if (descriptor_log_ == nullptr) {
//    // No reason to unlock *mu here since we only hit this path in the
//    // first call to LogAndApply (when opening the database).
//    assert(descriptor_file_ == nullptr);
//    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
//    edit->SetNextFile(next_file_number_);
//    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
//    if (s.ok()) {
//      descriptor_log_ = new log::Writer(descriptor_file_);
//      s = WriteSnapshot(descriptor_log_);
//    }
//  }

  // Unlock during expensive MANIFEST log write
//  {
//    mu->Unlock();
//
//    // Write new record to MANIFEST log
//    if (s.ok()) {
//      std::string record;
//      edit->EncodeTo(&record);
//      s = descriptor_log_->AddRecord(record);
//      if (s.ok()) {
//        s = descriptor_file_->Sync();
//      }
//      if (!s.ok()) {
//        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
//      }
//    }
//
//    // If we just created a new descriptor file, install it by writing a
//    // new CURRENT file that points to it.
//    if (s.ok() && !new_manifest_file.empty()) {
//      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
//    }
//
//    mu->Lock();
//  }

  // Install the new version
  if (s.ok()) {
//    std::unique_lock<std::mutex> lck(*version_set_mtx);

    AppendVersion(v);
  } else {
    delete v;
    printf("installing new version failed");
    exit(0);
//    if (!new_manifest_file.empty()) {
//      delete descriptor_log_;
//      delete descriptor_file_;
//      descriptor_log_ = nullptr;
//      descriptor_file_ = nullptr;
//      env_->RemoveFile(new_manifest_file);
//    }
  }
//  lck.unlock();
  return s;
}

Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit(0);
      s = edit.DecodeFrom(record, 0, table_cache_);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit, current_);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file == nullptr);
  assert(descriptor_log == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log = new log::Writer(descriptor_file, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}
//Use mutex to synchronize between threads.
void VersionSet::MarkFileNumberUsed(uint64_t number) {
  std::unique_lock<std::mutex> lck(*sv_mtx);
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
//  int best_level = -1;
//  double best_score = -1;

  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      score = (v->levels_[level].size() - v->in_progress[level].size())/
              static_cast<double>(config::kL0_CompactionTrigger);
      assert(score>=0);
//      if (score > 2)
//        score = 2;
      v->compaction_level_[level] = level;
      v->compaction_score_[level] = score;

    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->levels_[level]) - TotalFileSize(v->in_progress[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
      assert(score>=0);
      v->compaction_level_[level] = level;
      v->compaction_score_[level] = score;
    }

//    if (score > best_score) {
//      best_level = level;
//      best_score = score;
//    }
  }
  //sort the compaction level and compaction score.
  for (int i = 0; i < config::kNumLevels - 2; i++) {
    for (int j = i + 1; j < config::kNumLevels - 1; j++) {
      if (v->compaction_score_[i] < v->compaction_score_[j]) {
        double score = v->compaction_score_[i];
        int level = v->compaction_level_[i];
        v->compaction_score_[i] = v->compaction_score_[j];
        v->compaction_level_[i] = v->compaction_level_[j];
        v->compaction_score_[j] = score;
        v->compaction_level_[j] = level;
      }
    }
  }
  if (v->levels_[0].size() == 0){
    DEBUG("level 0 file equals 0 marker\n");
  }
//  v->compaction_level_ = best_level;
//  v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit(0);
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_index_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_index_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files = current_->levels_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const std::shared_ptr<RemoteMemTableMetaData> f = files[i];
      edit.AddFile(level, f);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->levels_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 6, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->levels_[0].size()), int(current_->levels_[1].size()),
      int(current_->levels_[2].size()), int(current_->levels_[3].size()),
      int(current_->levels_[4].size()), int(current_->levels_[5].size()),
      int(current_->levels_[6].size()));
  return scratch->buffer;
}
//must have lock outside.
//void VersionSet::Pin_Version_For_Compute(){
//  //Version id 0 is ommitted.
//  version_id++;
//  DEBUG_arg("Version id is %lu", version_id);
//  memory_version_pinner.insert({version_id, current_});
//  current_->Ref(5);
//}
//// must have lock outside
//bool VersionSet::Unpin_Version_For_Compute(size_t version_id) {
//  memory_version_pinner.at(version_id)->Unref(5);
//  memory_version_pinner.erase(version_id);
//  return true;
//}
uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files = v->levels_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i], &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files = v->levels_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->levels_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->levels_[level].size(); i++) {
      const std::shared_ptr<RemoteMemTableMetaData> f = current_->levels_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in mem_vec in
// *smallest, *largest.
// REQUIRES: mem_vec is not empty
void VersionSet::GetRange(const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    std::shared_ptr<RemoteMemTableMetaData> f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs_ is not empty
void VersionSet::GetRange2(const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& inputs1,
                           const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}
//TODO: make two overwriten functions one from compute node, the other from memory node.
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]);
        }
      } else {
        // Create concatenating iterator for the files from this level
        // one iterator will responsible for multiple remote memtables.
        list[num++] = NewTwoLevelFileIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}
Iterator* VersionSet::MakeInputIteratorMemoryServer(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator_MemorySide(options, files[i]);
        }
      } else {
        // Create concatenating iterator for the files from this level
        // one iterator will responsible for multiple remote memtables.
        list[num++] = NewTwoLevelFileIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator_Memoryside, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}
//Iterator* VersionSet::NewIterator(std::shared_ptr<RemoteMemTableMetaData> f) {
//  Cache::Handle* handle = nullptr;
//  Status s = FindTable(std::move(remote_table), &handle);
//  if (!s.ok()) {
//    return NewErrorIterator(s);
//  }
//
//  Table_Memory_Side* table = Table_Memory_Side::Open(table, f);
//  Iterator* result = table->NewIterator(options);
//  result->RegisterCleanup(&UnrefEntry, table_cache, handle);
//  if (tableptr != nullptr) {
//    *tableptr = table;
//  }
//  return result;
//}
//Compaction* VersionSet::PickCompaction() {
//  Compaction* c;
//  int level;
//
//  // We prefer compactions triggered by too much data in a level over
//  // the compactions triggered by seeks.
//  const bool size_compaction = (current_->compaction_score_ >= 1);
//  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
//  if (size_compaction) {
//    level = current_->compaction_level_;
//    assert(level >= 0);
//    assert(level + 1 < config::kNumLevels);
//    c = new Compaction(options_, level);
//
//    // Pick the first file that comes after compact_index_[level]
//    for (size_t i = 0; i < current_->levels_[level].size(); i++) {
//      std::shared_ptr<RemoteMemTableMetaData> f = current_->levels_[level][i];
//      if (compact_index_[level].empty() ||
//          icmp_.Compare(f->largest.Encode(), compact_index_[level]) > 0) {
//        c->inputs_[0].push_back(f);
//        break;
//      }
//    }
//    if (c->inputs_[0].empty()) {
//      // Wrap-around to the beginning of the key space
//      c->inputs_[0].push_back(current_->levels_[level][0]);
//    }
//  } else if (seek_compaction) {
//    level = current_->file_to_compact_level_;
//    c = new Compaction(options_, level);
//    c->inputs_[0].push_back(current_->file_to_compact_);
//  } else {
//    return nullptr;
//  }
//
//  c->input_version_ = current_;
//  c->input_version_->Ref();
//
//  // Files in level 0 may overlap each other, so pick up all overlapping ones
//  if (level == 0) {
//    InternalKey smallest, largest;
//    GetRange(c->inputs_[0], &smallest, &largest);
//    // Note that the next call will discard the file we placed in
//    // c->inputs_[0] earlier and replace it with an overlapping set
//    // which will include the picked file.
//    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
//    assert(!c->inputs_[0].empty());
//  }
//
//  SetupOtherInputs(c);
//
//  return c;
//}
//bool VersionSet::check_compaction_state(
//    std::shared_ptr<RemoteMemTableMetaData> sst) {
//  return sst->UnderCompaction;
//}
// TODO: Implement the file picking up for those file who exceed their peeking limit.
bool VersionSet::PickFileToCompact(int level, Compaction* c){
  assert(c->inputs_[0].empty());
  assert(c->inputs_[1].empty());
  if (level==0){
    // if there is pending compaction, skip level 0
    if (current_->in_progress[level].size()>0){
//      assert(current_->levels_[level][0]->UnderCompaction);
      return false;
    }
    //Directly pickup all the pending table in level 0
    c->inputs_[0] = current_->levels_[level];
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    assert(!c->inputs_[0].empty());
    if(current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1])){
      //Mark all the files as undercompaction
      for (auto iter : c->inputs_[0]) {
        iter->UnderCompaction = true;
      }
      current_->in_progress[level].insert(current_->in_progress[level].end(),
                                          c->inputs_[0].begin(), c->inputs_[0].end());
      for (auto iter : c->inputs_[1]) {
        iter->UnderCompaction = true;
      }
      current_->in_progress[level+1].insert(current_->in_progress[level+1].end(),
                                           c->inputs_[1].begin(), c->inputs_[1].end());
//      return true;
    }else{
      // clear the input for this level and return.
      c->inputs_[0].clear();
      c->inputs_[1].clear();
//      return false;
    }
  }else {
    size_t current_level_size = current_->levels_[level].size();
    size_t random_index = std::rand() % current_level_size;
    InternalKey smallest, largest;
    auto user_cmp = icmp_.user_comparator();
    int counter = 0;
    while (1) {
      std::shared_ptr<RemoteMemTableMetaData> f =
          current_->levels_[level][random_index];

      if (!f->UnderCompaction) {
        // if this file is not under compaction, insert it to the input list.
        c->inputs_[0].push_back(f);
        if (random_index != current_level_size - 1){
          std::shared_ptr<RemoteMemTableMetaData> next_f = current_->levels_[level][random_index + 1];
          // need to check whether next file share the same key with this file, if yes
          // we have to add the next file. because the upper level can not have newer update
          // than lower level
          //TOTHink: Is the file sequence in vector sorted by the largest / smallest key?
          assert(user_cmp->Compare(next_f->largest.user_key(), f->largest.user_key()) > 0);
          if(user_cmp->Compare(next_f->smallest.user_key(), f->largest.user_key()) == 0){
            c->inputs_[0].push_back(next_f);
          }
        }

        GetRange(c->inputs_[0], &smallest, &largest);
        // find file for level n+1
        if(current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                       &c->inputs_[1])){
          //Mark all the files as undercompaction
          for (auto iter : c->inputs_[0]) {
            iter->UnderCompaction = true;
          }
          current_->in_progress[level].insert(current_->in_progress[level].end(),
                                              c->inputs_[0].begin(), c->inputs_[0].end());
          for (auto iter : c->inputs_[1]) {
            iter->UnderCompaction = true;
          }
          current_->in_progress[level+1].insert(current_->in_progress[level+1].end(),
                                                c->inputs_[1].begin(), c->inputs_[1].end());
          break;
        }else{
          // if level n+1 under compaction clear the files
          c->inputs_[0].clear();
          c->inputs_[1].clear();
        }
      } else {
        // Optional: if this file is under compaction then empty the input vector.
        if (!c->inputs_[0].empty()) {
          c->inputs_[0].clear();

        }
        if (!c->inputs_[0].empty()) {
          c->inputs_[0].clear();

        }
      }
      // Tothink: here we do not check the size of the inputs[0], we will avoid
      //  creating small files during the compaction.
//      if (c->FirstLevelSize() < options_->max_file_size){
//        InternalKey smallest, largest;
//        GetRange(c->inputs_[0], &smallest, &largest);
//        if(current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]))
//          break;
//        else{
//          c->inputs_[0].clear();
//        }
//      }
      random_index =
          random_index + 1 < current_level_size ? random_index + 1 : 0;
      if (++counter == current_level_size) break;
    }
  }

  return !c->inputs_[0].empty();

}
Compaction* VersionSet::PickCompaction() {

  Compaction* c;
  int level = 0;
  double level_score = 0;
  bool skipped_l0_to_base = false;
  c = new Compaction(options_, level);
  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.

  //TODO: may be we can create a verion for current_, and only use a read lock
  // when fetch the current from the list.
  std::unique_lock<std::mutex> lck(*sv_mtx);

  for (int i = 0; i < config::kNumLevels - 1; i++) {
    level = current_->CompactionLevel(i);
    level_score = current_->CompactionScore(i);
    c->SetLevel(level);
    if (level_score >= 1){

      if (skipped_l0_to_base && level == 1) {
        // If L0->L1 compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      if (PickFileToCompact(level,c)) {
        assert(c->level() == level && level < 10);
#ifndef NDEBUG
        for (auto iter : c->inputs_[0]) {
          assert(std::find(current_->levels_[level].begin(), current_->levels_[level].end(), iter)
                 !=current_-> levels_[level].end());
        }
#endif
        break;
      }else{
        if (level == 0){
          skipped_l0_to_base = true;
          //TODO: schedule a intralevel compaction like rocks db.
        }
      }
    }else{
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      break;
    }
  }
  if (!c->inputs_[0].empty()) {
    c->input_version_ = current_;
    c->input_version_->Ref(2);
    //Recalculate the scores so that next time pick from a different level.
    Finalize(current_);
//    if (c->inputs_[1].size() == 1){
//      printf("mark here, first level file number is %lu\n", c->inputs_[1][0]->number);
//    }
    return c;
  }else{
    delete c;
    return nullptr;
  }
  //TODO: enable the file seek time compaciton in the future.
//  if (current_->file_to_compact_ != nullptr) {
//    level = current_->file_to_compact_level_;
//    c->SetLevel(level);
//    assert(c->inputs_[0].empty()&& c->inputs_[1].empty());
//    c->inputs_[0].push_back(current_->file_to_compact_);
//  } else {
//    return nullptr;
//  }



//  // Files in level 0 may overlap each other, so pick up all overlapping ones
//  if (level == 0) {
//    InternalKey smallest, largest;
//    GetRange(c->inputs_[0], &smallest, &largest);
//    // Note that the next call will discard the file we placed in
//    // c->inputs_[0] earlier and replace it with an overlapping set
//    // which will include the picked file.
//    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
//    assert(!c->inputs_[0].empty());
//  }
//
//  SetupOtherInputs(c);


}
// Finds the largest key in a vector of files. Returns true if files it not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    std::shared_ptr<RemoteMemTableMetaData> f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
std::shared_ptr<RemoteMemTableMetaData> FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  std::shared_ptr<RemoteMemTableMetaData> smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    std::shared_ptr<RemoteMemTableMetaData> f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& level_files,
                       std::vector<std::shared_ptr<RemoteMemTableMetaData>>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    std::shared_ptr<RemoteMemTableMetaData> smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  AddBoundaryInputs(icmp_, current_->levels_[level], &c->inputs_[0]);
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<std::shared_ptr<RemoteMemTableMetaData>> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->levels_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<std::shared_ptr<RemoteMemTableMetaData>> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_index_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<std::shared_ptr<RemoteMemTableMetaData>> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref(0);
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}


Compaction::Compaction(const Options* options, int level)
    : level_(level),
      opt_ptr(options),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      edit_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::Compaction(const Options* options)
    : opt_ptr(options),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      edit_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}
Compaction::~Compaction() {
  //TODO: protect the unref by the versionset mtx
  if (input_version_ != nullptr) {
    input_version_->Unref(0);
    assert(false);
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  //TOTHINK: why the statement below is true.

  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number, inputs_[which][i]->creator_node_id);
    }
  }
}

void Compaction::DecodeFrom(const Slice src, int side) {
  Slice input = src;
  uint16_t level = 0;
  GetFixed16(&input, &level);
  level_ = level;
  uint32_t first_level_len = 0;
  GetFixed32(&input, &first_level_len);
  for (size_t i = 0; i < first_level_len; i++) {
    std::shared_ptr<RemoteMemTableMetaData> f = std::make_shared<RemoteMemTableMetaData>(side);
    f->DecodeFrom(input);
    inputs_[0].push_back(f);
  }
  uint32_t second_level_len = 0;
  GetFixed32(&input, &second_level_len);
  for (size_t i = 0; i < second_level_len; i++) {
    std::shared_ptr<RemoteMemTableMetaData> f = std::make_shared<RemoteMemTableMetaData>(side);
    f->DecodeFrom(input);
    inputs_[1].push_back(f);
  }
  max_output_file_size_ = MaxFileSizeForLevel(opt_ptr, level);
}
void Compaction::EncodeTo(std::string* dst){
  uint16_t level = level_;
//  dst->append(reinterpret_cast<const char*>(&level), sizeof(char));
  PutFixed16(dst,level);
  uint32_t first_level_len = inputs_[0].size();
  PutFixed32(dst, first_level_len);
  for (size_t i = 0; i < first_level_len; i++) {
    const std::shared_ptr<RemoteMemTableMetaData> f = inputs_[0][i];
//    PutVarint32(dst, kNewFile);
//    PutVarint32(dst, new_files_[i].first);  // level
    f->EncodeTo(dst);

  }

  uint32_t second_level_len = inputs_[1].size();
  PutFixed32(dst, second_level_len);
  for (size_t i = 0; i < second_level_len; i++) {
    const std::shared_ptr<RemoteMemTableMetaData> f = inputs_[1][i];
    //    PutVarint32(dst, kNewFile);
    //    PutVarint32(dst, new_files_[i].first);  // level
    f->EncodeTo(dst);

  }
}
bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<std::shared_ptr<RemoteMemTableMetaData>>& files = input_version_->levels_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      std::shared_ptr<RemoteMemTableMetaData> f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}
//Note do not use it if it is a near data compaction.
bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}
uint64_t Compaction::FirstLevelSize(){
  uint64_t sum_size = 0;
  for (auto file : inputs_[0]) {
    sum_size +=file->file_size;
  }
  return sum_size;
}
void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref(2);
    input_version_ = nullptr;
  }
}
void Compaction::GenSubcompactionBoundaries() {
  //TODO: make boundary a pair of small and largest key. Or we can add
  // internal key to the boundary. so that we can directly search the (start, end]
  // by search the key of the same userkey to the startdata, but has a higher sequence number
  std::vector<Slice>& bounds = boundaries_;
  std::vector<uint64_t>& sizes = sizes_;
  //TODO: for the first time level 0 compaction, we do not have level 1 files, so
  // we could use the data block index to find out the boundaries.

//  //insert base level
//  {
//    auto level_inputs = inputs_[0];
//    size_t num_files = level_inputs.size();
//
//    if (level() == 0) {
//      // For level 0 add the starting and ending key of each file since the
//      // files may have greatly differing key ranges (not range-partitioned)
//      for (size_t i = 0; i < num_files; i++) {
//        bounds.emplace_back(level_inputs[i]->smallest.Encode());
//        bounds.emplace_back(level_inputs[i]->largest.Encode());
//      }
//    }else{
//      bounds.emplace_back(level_inputs[0]->smallest.Encode());
//      bounds.emplace_back(level_inputs[num_files - 1]->largest.Encode());
//    }
//  }
  // insert output level
  {
    auto level_inputs = inputs_[1];
    size_t num_files = level_inputs.size();
    // For the last level include the starting keys of all files since
    // the last level is the largest and probably has the widest key
    // range. Since it's range partitioned, the ending key of one file
    // and the starting key of the next are very close (or identical).
    sizes.push_back(level_inputs[0]->file_size);
    for (size_t i = 1; i < num_files; i++) {
      // use user key as boundary, so that there will be no user key accross SSTables.
      bounds.emplace_back(ExtractUserKey(level_inputs[i]->smallest.Encode()));
      sizes.push_back(level_inputs[i]->file_size);
    }
  }
//  const VersionSet* vset = input_version_->vset_;
//  // Scan to find earliest grandparent file that contains key.
//  const auto* usr_comparator = vset->icmp_.user_comparator();
//  // sort and remove duplicated boundaries.
//  std::sort(bounds.begin(), bounds.end(),
//            [usr_comparator](const Slice& a, const Slice& b) -> bool {
//              return usr_comparator->Compare(ExtractUserKey(a),
//                                             ExtractUserKey(b)) < 0;
//            });
//  // Remove duplicated entries from bounds
//  bounds.erase(
//      std::unique(bounds.begin(), bounds.end(),
//                  [usr_comparator](const Slice& a, const Slice& b) -> bool {
//                    return usr_comparator->Compare(ExtractUserKey(a),
//                                                   ExtractUserKey(b)) == 0;
//                  }),
//      bounds.end());
}
std::vector<Slice>* Compaction::GetBoundaries(){
  return &boundaries_;
}
std::vector<uint64_t>* Compaction::GetSizes() {
  return &sizes_;
}
uint64_t Compaction::GetFileSizesForLevel(int level){
  return inputs_[level].size();
}


}  // namespace TimberSaw
