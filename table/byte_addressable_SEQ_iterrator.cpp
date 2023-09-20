//
// Created by ruihong on 1/21/22.
//
//Make sure the Prefetch Granularity is larger than the key value size other wise there
// would be error
#define PREFETCH_GRANULARITY  (1024*1024)
#include "byte_addressable_SEQ_iterrator.h"
#include "TimberSaw/env.h"
#include "port/likely.h"
namespace TimberSaw {
//Note: the memory side KVReader should be passed as block function
ByteAddressableSEQIterator::ByteAddressableSEQIterator(
    Iterator* index_iter, void* arg, const ReadOptions& options,
    bool compute_side, uint8_t target_node_id)
    : compute_side_(compute_side),
//      mr_addr(nullptr),
//      kv_function_(kv_function),
      arg_(arg),
      options_(options),
      status_(Status::OK()),
      index_iter_(index_iter),
      valid_(false),
      target_node_id_(target_node_id){
    prefetched_mr = new ibv_mr{};
    Env::Default()->rdma_mg->Allocate_Local_RDMA_Slot(*prefetched_mr, FlushBuffer);
}

ByteAddressableSEQIterator::~ByteAddressableSEQIterator() {
  auto rdma_mg = Env::Default()->rdma_mg;
//  if (poll_number != 0){
//    ibv_wc* wc = new ibv_wc[poll_number];
//    rdma_mg->poll_completion(wc, poll_number, "read_local", true);
//  }
  rdma_mg->Deallocate_Local_RDMA_Slot(prefetched_mr->addr, FlushBuffer);
  delete prefetched_mr;


  //  DEBUG_arg("TWOLevelIterator destructing, this pointer is %p\n", this);
};

void ByteAddressableSEQIterator::Seek(const Slice& target) {
//  assert(target.compare(reinterpret_cast<Table*>(arg_)->rep->remote_table.lock()->largest.Encode()) <= 0);
  index_iter_.Seek(target);
  GetKVInitial();

}

void ByteAddressableSEQIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  GetKVInitial();


}

void ByteAddressableSEQIterator::SeekToLast() {
  valid_ = false;
  status_ = Status::NotSupported("SeekToLast");
}

void ByteAddressableSEQIterator::Next() {
  GetNextKV();

}

void ByteAddressableSEQIterator::Prev() {
  assert(false);
  valid_ = false;
  status_ = Status::NotSupported("Prev not supported");
}
void ByteAddressableSEQIterator::GetKVInitial(){
  if(index_iter_.Valid()){
    Slice handle_content = index_iter_.value();
    BlockHandle handle;
    handle.DecodeFrom(&handle_content);
    iter_offset = handle.offset();

    valid_ = Fetch_next_buffer_initial(iter_offset);
//    DEBUG_arg("Move to the next chunk, iter_ptr now is %p\n", iter_ptr);
    assert(valid_);
    auto rdma_mg = Env::Default()->rdma_mg;
    // Only support forward iterator for sequential access iterator.
    uint32_t key_size, value_size;
    if (iter_offset + 8 > cur_prefetch_status){
      if(!Fetch_next_buffer_middle()){
        assert(iter_offset == cur_prefetch_status);
        Fetch_next_buffer_initial(iter_offset);
      }
//      DEBUG_arg("Move to the next subchunk, iter_ptr now is %p\n", iter_ptr);

//      ibv_wc wc[1];
//      rdma_mg->poll_completion(wc,1, "read_local", true);
//      cur_prefetch_status += PREFETCH_GRANULARITY;
//      poll_number--;
//      assert(poll_number >= 0);
    }
    Slice Size_buff = Slice(iter_ptr, 8);
    GetFixed32(&Size_buff, &key_size);
    GetFixed32(&Size_buff, &value_size);
    iter_ptr += 8;
    iter_offset += 8;
//    //Check whether the
    if (UNLIKELY(iter_offset + key_size + value_size > cur_prefetch_status)){

      if(!Fetch_next_buffer_middle()){
        assert(iter_offset == cur_prefetch_status);
        Fetch_next_buffer_initial(iter_offset);
      }
//      DEBUG_arg("Move to the next subchunk, iter_ptr now is %p\n", iter_ptr);
//      ibv_wc wc[1];
//      rdma_mg->poll_completion(wc,1, "read_local", true);
//      cur_prefetch_status += PREFETCH_GRANULARITY;
//      poll_number--;
//      assert(poll_number >= 0);
    }
    key_.SetKey(Slice(iter_ptr, key_size), false /* copy */);
    iter_ptr += key_size;
    iter_offset += key_size;
    value_ = Slice(iter_ptr, value_size);
    iter_ptr += value_size;
    iter_offset += value_size;
//    iter_offset += key_size + value_size + 2*sizeof(uint32_t);
    assert(iter_ptr - (char*)prefetched_mr->addr <= prefetched_mr->length);
  }else{
    valid_ = false;
  }
  assert(key().size()>0);
}


void ByteAddressableSEQIterator::GetNextKV() {
  assert(iter_ptr <= remote_mr_current.length + (char*)prefetched_mr->addr);
  if (iter_ptr == nullptr || iter_ptr == remote_mr_current.length + (char*)prefetched_mr->addr){

    valid_ = Fetch_next_buffer_initial(iter_offset);
//    DEBUG_arg("Move to the next chunk, iter_ptr now is %p\n", iter_ptr);
    //TODO: reset all the relevent metadata such as iter_ptr, cur_prefetch_status.
  }
  if (!valid_){
    DEBUG("Get next KV invalid\n");
    return;
  }
  //TODO: The Get KV need to wait if the data has not been fetched already, need to Poll completion
  // Use the cur_prefetch_status to represent the postion for current prefetching.
//    valid_ = true;
  auto rdma_mg = Env::Default()->rdma_mg;
  // Only support forward iterator for sequential access iterator.
  uint32_t key_size, value_size;
  if (UNLIKELY(iter_offset + 8 > cur_prefetch_status)){

    if(!Fetch_next_buffer_middle()){
      assert(iter_offset == cur_prefetch_status);
      Fetch_next_buffer_initial(iter_offset);
    }
    assert(iter_offset + 8 <= cur_prefetch_status);
//    DEBUG_arg("Move to the next subchunk, iter_ptr now is %p\n", iter_ptr);

  }
  Slice Size_buff = Slice(iter_ptr, 8);
  GetFixed32(&Size_buff, &key_size);
  GetFixed32(&Size_buff, &value_size);
  assert(key_size == 29 || key_size == 28);
  assert(value_size == 400 || value_size == 21);
  iter_ptr += 8;
  iter_offset += 8;
  //Check whether the
  if (UNLIKELY(iter_offset + key_size + value_size > cur_prefetch_status)){

    if(!Fetch_next_buffer_middle()){
      assert(iter_offset == cur_prefetch_status);
      Fetch_next_buffer_initial(iter_offset);
    }
    assert(iter_offset + 8 <= cur_prefetch_status);
//    DEBUG_arg("Move to the next subchunk, iter_ptr now is %p\n", iter_ptr);
  }
  key_.SetKey(Slice(iter_ptr, key_size), false /* copy */);
  iter_ptr += key_size;
  iter_offset += key_size;
  value_ = Slice(iter_ptr, value_size);
  iter_ptr += value_size;
  iter_offset += value_size;
//  DEBUG_arg("Iterator now is at %p \n", iter_ptr);
//  iter_offset += key_size + value_size + 2*sizeof(uint32_t);
  assert(iter_ptr - (char*)prefetched_mr->addr <= remote_mr_current.length);
}
// The offset has to be the start address of the SSTable chunk, we only fetch
// PREFETCH_GRANULARITY data every time. THis function is the first prefetch for
// one specific SSTable chunk
bool ByteAddressableSEQIterator::Fetch_next_buffer_initial(size_t offset) {
  Table* table = reinterpret_cast<Table*>(arg_);
//  ibv_mr mr = {};
  auto tablemeta = table->rep->remote_table.lock();
  auto rdma_mg = Env::Default()->rdma_mg;
//  assert(remote_mr_current.addr == nullptr);
//  assert(remote_mr_current.length == 0);
  prefetch_counter = 0;
  if(Find_prefetch_MR(&tablemeta->remote_data_mrs, offset, &remote_mr_current)){
//    size_t total_len = remote_mr_current.length;
//    prefetched_mr->length = remote_mr_current.length;
    ibv_mr remote_mr = remote_mr_current;
    ibv_mr local_mr = *prefetched_mr;

    if (remote_mr_current.length > PREFETCH_GRANULARITY){
      remote_mr.length = PREFETCH_GRANULARITY;
      local_mr.length = PREFETCH_GRANULARITY;
      rdma_mg->RDMA_Read(&remote_mr, &local_mr, PREFETCH_GRANULARITY,
                         "read_local", IBV_SEND_SIGNALED, 1, target_node_id_);
//      remote_mr_current.addr = (void*)((char*)remote_mr_current.addr + PREFETCH_GRANULARITY);
//      remote_mr_current.length -= PREFETCH_GRANULARITY;
      cur_prefetch_status = offset + PREFETCH_GRANULARITY;
    } else {
//      remote_mr.addr = (void*)((char*)remote_mr.addr );
//      local_mr.addr = (void*)((char*)local_mr.addr);
      remote_mr.length = remote_mr_current.length;
      local_mr.length = remote_mr_current.length;
      rdma_mg->RDMA_Read(&remote_mr, &local_mr, remote_mr_current.length,
                         "read_local", IBV_SEND_SIGNALED, 1, target_node_id_);
//      remote_mr_current.addr = nullptr;
//      remote_mr_current.length = 0;
      cur_prefetch_status = offset + remote_mr_current.length;

    }
    prefetch_counter = 1;
//    for (size_t i = 0; i < mr.length/PREFETCH_GRANULARITY + 1; ++i) {
//      remote_mr.addr = (void*)((char*)remote_mr.addr + i*PREFETCH_GRANULARITY);
//      local_mr.addr = (void*)((char*)local_mr.addr + i*PREFETCH_GRANULARITY);
//      if (total_len > PREFETCH_GRANULARITY){
//
//        rdma_mg->RDMA_Read(&remote_mr, &local_mr, PREFETCH_GRANULARITY, "read_local", IBV_SEND_SIGNALED, 1);
//        total_len -= PREFETCH_GRANULARITY;
//      }else if (total_len > 0){
//        remote_mr.addr = (void*)((char*)remote_mr.addr + i*PREFETCH_GRANULARITY);
//        local_mr.addr = (void*)((char*)local_mr.addr + i*PREFETCH_GRANULARITY);
//        remote_mr.length = total_len;
//        local_mr.length = total_len;
//        rdma_mg->RDMA_Read(&remote_mr, &local_mr, total_len, "read_local", IBV_SEND_SIGNALED, 0);
//        total_len = 0;
//      }
//    }
//    prefetched_mr->length = mr.length;
    iter_ptr = (char*)prefetched_mr->addr;
//    poll_number = mr.length/PREFETCH_GRANULARITY;

//    assert(total_len == 0);
    return true;
  }else{
    return false;
  }

  //  return mr;
}
bool ByteAddressableSEQIterator::Fetch_next_buffer_middle() {
  Table* table = reinterpret_cast<Table*>(arg_);
  //  ibv_mr mr = {};
  auto tablemeta = table->rep->remote_table.lock();
  auto rdma_mg = Env::Default()->rdma_mg;
//  assert(remote_mr_current.addr != nullptr);
//  assert(remote_mr_current.length != 0);
  if (remote_mr_current.length <= PREFETCH_GRANULARITY*prefetch_counter){
    // There is no middle fetch task left we need to start a new chunk.
    return false;
  }

  //    size_t total_len = remote_mr_current.length;
  ibv_mr remote_mr = remote_mr_current;
  remote_mr.addr = (char*)remote_mr.addr + PREFETCH_GRANULARITY*prefetch_counter;
  ibv_mr local_mr = *prefetched_mr;
  local_mr.addr = (char*)local_mr.addr + PREFETCH_GRANULARITY*prefetch_counter;

  //Note remote_mr_current.length will not be changed in this function, it will only be
  // changed in the initial function.
  if (remote_mr_current.length - prefetch_counter*PREFETCH_GRANULARITY > PREFETCH_GRANULARITY){
    remote_mr.length = PREFETCH_GRANULARITY;
    local_mr.length = PREFETCH_GRANULARITY;
    rdma_mg->RDMA_Read(&remote_mr, &local_mr, PREFETCH_GRANULARITY,
                       "read_local", IBV_SEND_SIGNALED, 1, target_node_id_);
//    remote_mr_current.addr = (void*)((char*)remote_mr_current.addr + PREFETCH_GRANULARITY);
//    remote_mr_current.length -= PREFETCH_GRANULARITY;
    cur_prefetch_status += PREFETCH_GRANULARITY;
  }else {
    //      remote_mr.addr = (void*)((char*)remote_mr.addr );
    //      local_mr.addr = (void*)((char*)local_mr.addr);
    remote_mr.length = remote_mr_current.length;
    local_mr.length = remote_mr_current.length;
    rdma_mg->RDMA_Read(
        &remote_mr, &local_mr,
        remote_mr_current.length - PREFETCH_GRANULARITY * prefetch_counter,
        "read_local", IBV_SEND_SIGNALED, 1, target_node_id_);
//    remote_mr_current.addr = nullptr;
//    remote_mr_current.length = 0;
    cur_prefetch_status += remote_mr_current.length - PREFETCH_GRANULARITY*prefetch_counter;

  }
  prefetch_counter++;
  //    assert(total_len == 0);
  return true;


  //  return mr;
}
}