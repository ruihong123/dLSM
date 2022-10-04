//
// Created by ruihong on 1/20/22.
//

#include "byte_addressable_RA_iterator.h"
#include "TimberSaw/env.h"
#include "table_memoryside.h"
namespace TimberSaw {
ByteAddressableRAIterator::ByteAddressableRAIterator(Iterator* index_iter,
                                                     KVFunction block_function,
                                                     void* arg,
                                                     const ReadOptions& options,
                                                     bool compute_side)
    : compute_side_(compute_side),
      mr_addr(nullptr),
      kv_function_(block_function),
      arg_(arg),
      options_(options),
      status_(Status::OK()),
      index_iter_(index_iter),
      valid_(false) {
#ifndef NDEBUG
  if (compute_side){
    assert(block_function == &Table::KVReader);
  } else{
    assert(block_function == &Table_Memory_Side::KVReader);
  }
#endif
}

ByteAddressableRAIterator::~ByteAddressableRAIterator() {
  if (mr_addr != nullptr){
    auto rdma_mg = Env::Default()->rdma_mg;
    rdma_mg->Deallocate_Local_RDMA_Slot(mr_addr, DataChunk);
  }
    //  DEBUG_arg("TWOLevelIterator destructing, this pointer is %p\n", this);
};
//Note: if the iterator can not seek the same target, it will stop at the key right before or
// right after the data, we need to make it right before
void ByteAddressableRAIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  GetKV();
  assert(valid_);
//  if ()
//  //Todo: delete the things below.
//  for (int i = 0; i < target.size(); ++i) {
//    assert(key_.GetKey().data()[i] == target.data()[i]);
//  }


}

void ByteAddressableRAIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  GetKV();
//  if (data_iter_.iter() != nullptr) {
//    data_iter_.SeekToFirst();
//    valid_ = true;
//  } else{
//    assert(false);
//  }
//  SkipEmptyDataBlocksForward();
//  assert(key().size() >0);
}

void ByteAddressableRAIterator::SeekToLast() {
  index_iter_.SeekToLast();
  GetKV();
//  if (data_iter_.iter() != nullptr){
//    data_iter_.SeekToLast();
//    valid_ = true;
//  }
//  SkipEmptyDataBlocksBackward();
}

void ByteAddressableRAIterator::Next() {
  index_iter_.Next();
  GetKV();

}

void ByteAddressableRAIterator::Prev() {
  assert(Valid());
  index_iter_.Prev();
  GetKV();
}



void ByteAddressableRAIterator::GetKV() {
  if (!index_iter_.Valid()) {
    //    SetDataIterator(nullptr);
    valid_ = false;
    DEBUG_arg("TwoLevelIterator Index block invalid, error: %s\n", status().ToString().c_str());
  } else {
    valid_ = true;
    //    DEBUG("Index block valid\n");
    Slice handle = index_iter_.value();
#ifndef NDEBUG
    Slice test_handle = handle;
    index_handle.DecodeFrom(&test_handle);
//    printf("Iterator pointer is %p, Offset is %lu, this data block size is %lu\n", this, bhandle.offset(), bhandle.size());
#endif
    if (handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      if (compute_side_){
        //TODO: just reuse the RDMA registered buffer every time, no need to
        // allocate and deallocate again. we abandon the function pointer design,
        // directly call the static function instead.
        if (mr_addr != nullptr){
          auto rdma_mg = Env::Default()->rdma_mg;
          rdma_mg->Deallocate_Local_RDMA_Slot(mr_addr, DataChunk);
        }
        Slice KV = (*kv_function_)(arg_, options_, handle);
        mr_addr = const_cast<char*>(KV.data());
        uint32_t key_size, value_size;
        GetFixed32(&KV, &key_size);
        GetFixed32(&KV, &value_size);
        assert(key_size + value_size == KV.size());


        key_.SetKey(Slice(KV.data(), key_size), false /* copy */);
        KV.remove_prefix(key_size);
        assert(KV.size() == value_size);
        value_ = KV;
        data_block_handle_.assign(handle.data(), handle.size());
      }else{
        Slice KV = (*kv_function_)(arg_, options_, handle);
        uint32_t key_size, value_size;
        GetFixed32(&KV, &key_size);
        GetFixed32(&KV, &value_size);

        assert(key_size + value_size == KV.size());

//        printf("!key is %p, KV.data is %p, the 7 bit is %s \n",
//               key_.GetKey().data(), KV.data(), KV.data()+7);

        key_.SetKey(Slice(KV.data(), key_size), false /* copy */);
        KV.remove_prefix(key_size);
        assert(KV.size() == value_size);
        value_ = KV;
        data_block_handle_.assign(handle.data(), handle.size());
      }

    }
  }
}
}