// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_DB_MEMTABLE_H_
#define STORAGE_TimberSaw_DB_MEMTABLE_H_
#define MEMTABLE_SEQ_SIZE 153846 //Make the in memory buffer close to 64MB 1 shard origin
//#define MEMTABLE_SEQ_SIZE 19231 //Make the in memory buffer close to 64MB
//#define MEMTABLE_SEQ_SIZE 38462
// 8 shard's memtable size X 8  should be a little larger that the memtable of 64MB,
// because of the merge
// #define MEMTABLE_SEQ_SIZE 610081
#include "db/dbformat.h"
#include "db/inlineskiplist.h"
#include <string>

#include "TimberSaw/db.h"

//#include "util/arena_old.h"

namespace TimberSaw {

class InternalKeyComparator;
class MemTableIterator;
class RemoteMemTableMetaData;

class MemTable {
 public:
  struct KeyComparator {
    typedef Slice DecodedType;

    virtual DecodedType decode_key(const char* key) const {
      // The format of key is frozen and can be terated as a part of the API
      // contract. Refer to MemTable::Add for details.
      return GetLengthPrefixedSlice(key);
    }
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
    int operator()(const char* prefix_len_key,
                   const DecodedType& key) const;
  };
  //Requested means in the queue but not handled by thread, scheduled means put into the
  enum FlushStateEnum { FLUSH_NOT_REQUESTED, FLUSH_REQUESTED,
    FLUSH_PROCESSING, FLUSH_FINISHED};
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  std::atomic<bool> able_to_flush = false;
  bool full_table_flush=true;
  std::shared_ptr<RemoteMemTableMetaData> sstable;
  const KeyComparator comparator;
#ifdef PROCESSANALYSIS
  static std::atomic<uint64_t> GetTimeElapseSum;
  static std::atomic<uint64_t> GetNum;
  static std::atomic<uint64_t> foundNum;
#endif
  explicit MemTable(const InternalKeyComparator& cmp);
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;
  ~MemTable();

  typedef InlineSkipList<KeyComparator> Table;
  Table* GetTable(){
    return &table_;
  }
  // Increase reference count.
  void Ref() { refs_.fetch_add(1); }
  void NotFullTableflush(){full_table_flush = false;}
  void assert_refs(int i){
    assert(refs_.load() == i);
  }
  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    refs_.fetch_sub(1);
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      // TODO: THis assertion may changed in the future
#ifndef NDEBUG
      if (full_table_flush){
        assert(seq_count.load() == MEMTABLE_SEQ_SIZE);
      }

#endif
      delete this;
    }
  }
  //Simple Delete here means that the memtable is not full but it was deleted.
  void SimpleDelete(){
    refs_--;

    delete this;
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);
  void SetLargestSeq(uint64_t seq){
    largest_seq_supposed = seq;
  }
  void SetFirstSeq(uint64_t seq){
    first_seq = seq;
  }
//  void SetLargestSeqTillNow(uint64_t seq){
//
//  }
  bool CheckFlushInProcess(){
    return flush_state_ == FLUSH_PROCESSING;
  }
  bool CheckFlush_Requested(){
    return flush_state_ == FLUSH_REQUESTED;
  }
  bool CheckFlushFinished(){
    return flush_state_.load() == FLUSH_FINISHED;
  }
  void SetFlushState(FlushStateEnum state){
    flush_state_.store(state);
  }
  void MarkFlushed(){
    assert(flush_state_ == FLUSH_PROCESSING);
    SetFlushState(FLUSH_FINISHED);
  }
  uint64_t Getlargest_seq_supposed() const{
    // in case that there is a unfull table flush, the largest seq will be different
    // from the one supposed
    return largest_seq_supposed;
  }
  uint64_t Getlargest_seq() const{
    // in case that there is a unfull table flush, the largest seq will be different
    // from the one supposed
    return largest_seq_supposed - MEMTABLE_SEQ_SIZE + seq_count;
  }
  uint64_t GetFirstseq() const{
    return first_seq;
  }
  void increase_seq_count(size_t num){
    seq_count.fetch_add(num);
    assert(num == 1);
    assert(seq_count <= MEMTABLE_SEQ_SIZE);
    //TODO; For a write batch you the write may cross the boder, we need to modify
    // the boder of the next table
    if (seq_count >= MEMTABLE_SEQ_SIZE){
      able_to_flush.store(true);
    }
  }
  size_t Get_seq_count(){
    return seq_count;
  }
 private:
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;






  std::atomic<int> refs_;
  std::atomic<size_t> seq_count = 0;

  ConcurrentArena arena_;
  Table table_;
  std::atomic<FlushStateEnum> flush_state_ = FLUSH_NOT_REQUESTED;
  int64_t first_seq;
  std::atomic<int64_t> largest_seq_till_now = 0;
  int64_t largest_seq_supposed;

};

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_DB_MEMTABLE_H_
