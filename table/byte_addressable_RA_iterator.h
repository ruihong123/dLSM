//
// Created by ruihong on 1/20/22.
//

#ifndef TIMBERSAW_BYTE_ADDRESSABLE_RA_ITERATOR_H
#define TIMBERSAW_BYTE_ADDRESSABLE_RA_ITERATOR_H

#include "TimberSaw/iterator.h"
#include "iterator_wrapper.h"
#include "TimberSaw/options.h"
#include "two_level_iterator.h"
#include "db/version_set.h"
#include "TimberSaw/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
//#include "two_level_iterator.h"
namespace TimberSaw {
typedef Slice (*KVFunction)(void*, const ReadOptions&, const Slice&);

class ByteAddressableRAIterator :public Iterator{
   public:
    ByteAddressableRAIterator(Iterator* index_iter, KVFunction block_function,
                              void* arg, const ReadOptions& options,
                              bool compute_side);

    ~ByteAddressableRAIterator() override;

    void Seek(const Slice& target) override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Next() override;
    void Prev() override;

    bool Valid() const override { return valid_; }
    Slice key() const override {
      assert(Valid());
      return key_.GetKey();
    }
    Slice value() const override {
      assert(Valid());
      return value_;
    }
    Status status() const override {
        return status_;
    }

   private:
    void SaveError(const Status& s) {
      if (status_.ok() && !s.ok()) status_ = s;
    }
//    void SkipEmptyDataBlocksForward();
//    void SkipEmptyDataBlocksBackward();
    void GetKV();
    bool compute_side_;
    char* mr_addr;
    KVFunction kv_function_;
    void* arg_;
    const ReadOptions options_;
    Status status_;
    IteratorWrapper index_iter_;
    IterKey key_;
    Slice value_;
    // If data_iter_ is non-null, then "data_block_handle_" holds the
    // "index_value" passed to block_function_ to create the data_iter_.
    std::string data_block_handle_;
#ifndef NDEBUG
    BlockHandle index_handle;
    std::string last_key;
    int64_t num_entries=0;
#endif
    bool valid_;
};
}


#endif  // TIMBERSAW_BYTE_ADDRESSABLE_RA_ITERATOR_H
