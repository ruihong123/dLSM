// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_TimberSaw_TABLE_ITERATOR_WRAPPER_H_

#include "TimberSaw/iterator.h"
#include "TimberSaw/slice.h"

namespace TimberSaw {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// table_cache locality.
class IteratorWrapper {
 public:
  IteratorWrapper() : iter_(nullptr), valid_(false) {}
  explicit IteratorWrapper(Iterator* iter) : iter_(nullptr) { Set(iter); }
  ~IteratorWrapper() {
#ifndef NDEBUG
//    printf("Delete iter_ when iterator wrapper deallocation, deleted iter_ is %p \n", iter_);
#endif
    delete iter_;
  }
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  void Set(Iterator* iter) {
#ifndef NDEBUG
//    printf("Delete iter_ when replacing the iterator, deleted iter_ is %p, new iterator is %p\n", iter_, iter);
#endif
    delete iter_;

    iter_ = iter;
    if (iter_ == nullptr) {
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
  Slice value() const {
    assert(Valid());
    return iter_->value();
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
//    assert(valid_);
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
      // 0201delete
      assert(*key_.data() != 'U' && *key_.data() != 'V' );
    }

  }

  Iterator* iter_;
  bool valid_;
  Slice key_;
};


}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_TABLE_ITERATOR_WRAPPER_H_
