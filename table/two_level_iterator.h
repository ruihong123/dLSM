// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_TABLE_TWO_LEVEL_ITERATOR_H_
#define STORAGE_TimberSaw_TABLE_TWO_LEVEL_ITERATOR_H_

#include <db/version_edit.h>

#include "TimberSaw/iterator.h"
#include "db/version_set.h"
#include "TimberSaw/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
namespace TimberSaw {

struct ReadOptions;
class FileIteratorWrapper {
 public:
  FileIteratorWrapper() : iter_(nullptr), valid_(false) {}
  explicit FileIteratorWrapper(Version::LevelFileNumIterator* iter) : iter_(nullptr) { Set(iter); }
  ~FileIteratorWrapper() { delete iter_; }
  Version::LevelFileNumIterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  void Set(Version::LevelFileNumIterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == nullptr) {
      DEBUG("File iterator invalid\n");
      valid_ = false;
    } else {
      Update();
    }
  }

  // Iterator interface methods
  bool Valid() const { return valid_; }
  Slice key() const {
    assert(Valid());
    return key_;
  }
  std::shared_ptr<RemoteMemTableMetaData> value() const {
    assert(Valid());
    return iter_->file_value();
  }
  // Methods below require iter() != nullptr
  Status status() const {
    assert(iter_);
    return iter_->status();
  }
  void Next() {
    assert(iter_);
    iter_->Next();
    Update();
  }
  void Prev() {
    assert(iter_);
    iter_->Prev();
    Update();
  }
  void Seek(const Slice& k) {
    assert(iter_);
    iter_->Seek(k);
    Update();
  }
  void SeekToFirst() {
    assert(iter_);
    iter_->SeekToFirst();
    Update();
  }
  void SeekToLast() {
    assert(iter_);
    iter_->SeekToLast();
    Update();
  }

 private:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
  }

  Version::LevelFileNumIterator* iter_;
  bool valid_;
  Slice key_;
};
// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);
typedef Iterator* (*FileFunction)(void*, const ReadOptions&, std::shared_ptr<RemoteMemTableMetaData> remote_table);
class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      printf("index invalid error: %s\n", index_iter_.status().ToString().c_str());
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      printf("data invalid\n");
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_;  // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
#ifndef NDEBUG
  std::string last_key;
  int64_t num_entries=0;
#endif
//  bool valid_;
};
//TODO: there are bugs when there are mulitple concurrent iterator seeking.
// not sure why the concurrent iterator becomes invalid.
class TwoLevelFileIterator : public Iterator {
 public:
  TwoLevelFileIterator(Version::LevelFileNumIterator* index_iter, FileFunction file_function,
                       void* arg, const ReadOptions& options);

  ~TwoLevelFileIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  FileFunction file_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  FileIteratorWrapper index_iter_;
  IteratorWrapper data_iter_;  // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::shared_ptr<RemoteMemTableMetaData> this_remote_table;
//  bool valid_;
};
//Iterator* NewTwoLevelIterator(
//    Iterator* index_iter,
//    Iterator* (*block_function)(void* arg, const ReadOptions& options,
//                                const Slice& index_value),
//    void* arg, const ReadOptions& options);

Iterator* NewTwoLevelFileIterator(
    Version::LevelFileNumIterator* index_iter,
    Iterator* (*FileFunction)(void* arg, const ReadOptions& options,
                              std::shared_ptr<RemoteMemTableMetaData>),
    void* arg, const ReadOptions& options);
}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_TABLE_TWO_LEVEL_ITERATOR_H_
