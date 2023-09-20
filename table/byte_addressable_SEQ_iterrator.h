//
// Created by ruihong on 1/21/22.
//

#ifndef TIMBERSAW_BYTE_ADDRESSABLE_SEQ_ITERRATOR_H
#define TIMBERSAW_BYTE_ADDRESSABLE_SEQ_ITERRATOR_H
#include "TimberSaw/iterator.h"
#include "iterator_wrapper.h"
#include "TimberSaw/options.h"
#include "two_level_iterator.h"
#include "db/version_set.h"
#include "TimberSaw/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
namespace TimberSaw {
typedef Slice (*KVFunction)(void*, const ReadOptions&, const Slice&);
// THE SEQ will only be used by the compute side. For the memory node, we
// still keep using the random access allocator.

//The seq iterator only support forward searching
class ByteAddressableSEQIterator :public Iterator{
 public:
  ByteAddressableSEQIterator(Iterator* index_iter, void* arg,
                             const ReadOptions& options, bool compute_side,
                             uint8_t target_node_id);

  ~ByteAddressableSEQIterator() override;

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
  void GetKVInitial();
  void GetNextKV();

  bool Fetch_next_buffer_initial(size_t offset);
  bool Fetch_next_buffer_middle();
  bool compute_side_;
//  char* mr_addr;
//  ibv_mr* mr;
  // the memory region for the prefetch buffer, the length represents the border
  // for current prefetched data.
  ibv_mr* prefetched_mr;
  uint32_t prefetch_counter = 0;
  ibv_mr remote_mr_current = {};
//  size_t this_mr_offset;
  size_t iter_offset = 0;
  size_t cur_prefetch_status = 0;
  char* iter_ptr = nullptr;
//  int8_t poll_number = 0;
//  KVFunction kv_function_;
  //Some thoughts: We don't have to pin the remote table metadata in the iterator, before we
  // create the iterator we have to creat a snapshot for all the LSM tree which can pin the table.
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
  std::string last_key;
  int64_t num_entries=0;
#endif
  bool valid_;
  uint8_t target_node_id_;
};
}

#endif  // TIMBERSAW_BYTE_ADDRESSABLE_SEQ_ITERRATOR_H
