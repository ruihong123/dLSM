// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_TABLE_BLOCK_H_
#define STORAGE_TimberSaw_TABLE_BLOCK_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <db/dbformat.h>
#include <vector>

#include "TimberSaw/comparator.h"
#include "TimberSaw/iterator.h"

#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/rdma.h"
namespace TimberSaw {

struct BlockContents;
class Comparator;
//class IterKey;
enum BlockType {DataBlock, IndexBlock, IndexBlock_Small, FilterBlock, Block_On_Memory_Side};

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents, BlockType type);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

  class Iter;

 private:
  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  uint32_t restart_offset_;  // Offset in data_ of restart array
  bool owned_;               // Block owns data_[]
  bool RDMA_Regiested;
  BlockType type_;
  std::shared_ptr<RDMA_Manager> rdma_mg_;
};
// Helper routine: decode the next block entry starting at "p",
// storing the number of shared key bytes, non_shared key bytes,
// and the length of the value in "*shared", "*non_shared", and
// "*value_length", respectively.  Will not dereference past "limit".
//
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared, uint32_t* non_shared,
                                      uint32_t* value_length) {
  if (limit - p < 3) return nullptr;
  *shared = reinterpret_cast<const uint8_t*>(p)[0];
  *non_shared = reinterpret_cast<const uint8_t*>(p)[1];
  *value_length = reinterpret_cast<const uint8_t*>(p)[2];
  if ((*shared | *non_shared | *value_length) < 128) {
    // Fast path: all three values are encoded in one byte each
    p += 3;
  } else {
    if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
  }

  if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
    return nullptr;
  }
  return p;
}


class Block::Iter : public Iterator {
 public:
  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }
 private:
  const Comparator* const comparator_;
  const char* const data_;       // underlying block contents
  uint32_t const restarts_;      // Offset of restart array (list of fixed32), should be the end of content.
  uint32_t const num_restarts_;  // Number of uint32_t entries in restart array

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t restart_index_;  // Index of restart block in which current_ falls
//  std::string key_1;
//  std::string key_2;
  //Maitain two switched key holder in case that sometime iterator want to peek back the key and value.
  // This may not realize the peeking back correctly for the table compaction, because
  // the block iterator may be discarded when crossing the boundary of data blocks.
  IterKey key_;
  Slice value_;
//  int array_index;
//  Slice value_1;
  Status status_;
#ifndef NDEBUG
  std::string last_key;
  int64_t num_entries=0;
#endif




  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }

  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    key_.Clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    assert(offset <= restarts_);
    value_ = Slice(data_ + offset, 0);
  }

 public:
  Iter(const Comparator* comparator, const char* data, uint32_t restarts,
       uint32_t num_restarts)
      : comparator_(comparator),
        data_(data),
        restarts_(restarts),
        num_restarts_(num_restarts),
        current_(restarts_),
        restart_index_(num_restarts_){
    assert(num_restarts_ > 0);
  }
  ~Iter(){
#ifndef NDEBUG
//    printf("Block iter deallocate char p %p, iterator p %p\n", this->key_.c_str() , this);
#endif
//    DEBUG_arg("Block iter deallocate %p", this->key_.c_str());
  }
  bool Valid() const override { return current_ < restarts_; }
  Status status() const override { return status_; }
  Slice key() const override {
    assert(Valid());
    return key_.GetKey();
  }
  Slice value() const override {
    assert(Valid());
    return value_;
  }

  void Next() override {
    assert(Valid());
    ParseNextKey();
    assert(*key_.GetKey().data() != 'U' && *key_.GetKey().data() != 'V');
#ifndef NDEBUG
    if (num_entries > 0) {
      assert(comparator_->Compare(key_.GetKey(), Slice(last_key)) >= 0);
    }
    num_entries++;
    last_key = key_.GetKey().ToString();
    if (Valid())
      assert(key().size()!=0);
#endif


  }

  void Prev() override {
    assert(Valid());

    // Scan backwards to a restart point before current_
    const uint32_t original = current_;
    while (GetRestartPoint(restart_index_) >= original) {
      if (restart_index_ == 0) {
        // No more entries
        current_ = restarts_;
        restart_index_ = num_restarts_;
        return;
      }
      restart_index_--;
    }

    SeekToRestartPoint(restart_index_);
    do {
      // Loop until end of current entry hits the start of original entry
    } while (ParseNextKey() && NextEntryOffset() < original);
  }

  void Seek(const Slice& target) override {
    // Binary search in restart array to find the last restart point
    // with a key < target
    uint32_t left = 0;
    uint32_t right = num_restarts_ - 1;
    int current_key_compare = 0;

    if (Valid()) {
      // If we're already scanning, use the current position as a starting
      // point. This is beneficial if the key we're seeking to is ahead of the
      // current position.
      current_key_compare = Compare(key_.GetKey(), target);
      if (current_key_compare < 0) {
        // key_ is smaller than target
        left = restart_index_;
      } else if (current_key_compare > 0) {
        right = restart_index_;
      } else {
        // We're seeking to the key we're already at.
        return;
      }
    }

    while (left < right) {
      uint32_t mid = (left + right + 1) / 2;
      uint32_t region_offset = GetRestartPoint(mid);
      uint32_t shared, non_shared, value_length;
      const char* key_ptr =
          DecodeEntry(data_ + region_offset, data_ + restarts_, &shared,
                      &non_shared, &value_length);
      if (key_ptr == nullptr || (shared != 0)) {
        //We disable the shared string for byte addressable SSTable.
        CorruptionError();
#ifndef NDEBUG
        printf("detect corruption block, when seeking some key, num of entries is %ld, num of restart is %u\n", num_entries, num_restarts_);
#endif
        return;
      }
      // just used non_shared becasue this is a new restart point shared is 0
      Slice mid_key(key_ptr, non_shared);
      if (Compare(mid_key, target) < 0) {
        // Key at "mid" is smaller than "target".  Therefore all
        // blocks before "mid" are uninteresting.
        left = mid;
      } else {
        // Key at "mid" is >= "target".  Therefore all blocks at or
        // after "mid" are uninteresting.
        right = mid - 1;
      }
    }

    // We might be able to use our current position within the restart block.
    // This is true if we determined the key we desire is in the current block
    // and is after than the current key.
    assert(current_key_compare == 0 || Valid());
    bool skip_seek = left == restart_index_ && current_key_compare < 0;
    if (!skip_seek) {
      SeekToRestartPoint(left);
    }
    // Linear search (within restart block) for first key >= target
    while (true) {
      if (!ParseNextKey()) {
        return;
      }
      if (Compare(key_.GetKey(), target) >= 0) {
        return;
      }
    }
  }

  void SeekToFirst() override {
    SeekToRestartPoint(0);
    ParseNextKey();
    assert(key().size()!=0);
  }

  void SeekToLast() override {
    SeekToRestartPoint(num_restarts_ - 1);
    while (ParseNextKey() && NextEntryOffset() < restarts_) {
      // Keep skipping
    }
  }

 private:
  void CorruptionError() {
    assert(false);
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::Corruption("bad entry in block");
    DEBUG("bad entry in block\n");
    key_.Clear();

    value_.clear();

  }

  bool ParseNextKey() {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + restarts_;  // restarts_ is the restart array offset
    if (p >= limit) {
      // No more entries to return.  Mark as invalid.
//      printf("block is full, num of restart: %d, number of entries : %ld\n", num_restarts_, num_entries);
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return false;
    }

    // Decode next entry
    uint32_t shared, non_shared, value_length;
    p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
    if (p == nullptr || key_.Size() < shared) {
      CorruptionError();
#ifndef NDEBUG
      printf("detect corruption block, when parsing the next, num of entries is %ld, num of restart is %u\n", num_entries, num_restarts_);
#endif
      return false;
    } else {
//      int old_array_index = array_index;
//      array_index = array_index == 0 ? 1:0;
      //TODO: copy the last shared part to this one, otherwise try not to use two string buffer
//      auto start = std::chrono::high_resolution_clock::now();
//      key_[array_index].resize(shared);
//      key_[array_index].replace(0,shared, key_[old_array_index]);
//      key_[array_index].append(p, non_shared);
//      key_[array_index].SetKey(Slice(key_[old_array_index].GetKey().data(), shared));
//      key_[array_index].AppendToBack(p, non_shared);
      if (shared == 0){
        key_.SetKey(Slice(p, non_shared), false /* copy */);
        while (restart_index_ + 1 < num_restarts_ &&
               GetRestartPoint(restart_index_ + 1) < current_) {
          ++restart_index_;
        }
      }else{
        key_.TrimAppend(shared, p, non_shared);
      }

    //      key_.SetKey(Slice(p, non_shared));
    //      auto stop = std::chrono::high_resolution_clock::now();
    //      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    //      std::printf("string replace time elapse is %zu\n",  duration.count());
      value_ = Slice(p + non_shared, value_length);


      return true;
    }
  }
};
}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_TABLE_BLOCK_H_
