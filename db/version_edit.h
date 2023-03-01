// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_DB_VERSION_EDIT_H_
#define STORAGE_TimberSaw_DB_VERSION_EDIT_H_
#define EDIT_MERGER_COUNT 64
#define UNPIN_GRANULARITY 10
#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "util/rdma.h"

namespace TimberSaw {

class VersionSet;
class RDMA_Manager;
class TableCache;
enum Table_Type{ invalid_table_type_, block_based, byte_addressable};
struct RemoteMemTableMetaData {
//  RemoteMemTableMetaData();
// this_machine_type 0 means compute node, 1 means memory node
// creater_node_id  odd means compute node, even means memory node
  RemoteMemTableMetaData(int machine_type, TableCache* cache, uint8_t id);
  RemoteMemTableMetaData(int side);
  //TOTHINK: the garbage collection of the Remote table is not triggered!
  ~RemoteMemTableMetaData();
  bool Remote_blocks_deallocate(std::map<uint32_t, ibv_mr*> map,
                                Chunk_type c_type) {
    std::map<uint32_t , ibv_mr*>::iterator it;
//    assert(creator_node_id%2 == 0);
    for (it = map.begin(); it != map.end(); it++){
      if(!rdma_mg->Deallocate_Remote_RDMA_Slot(
              it->second->addr, shard_target_node_id, c_type)){
        return false;
      }
      delete it->second;
    }
    return true;
  }
  // TODO: sperate the deallocation buffer.
  bool Prepare_Batch_Deallocate(){
    std::map<uint32_t , ibv_mr*>::iterator it;
    uint64_t* ptr_flush;
    uint64_t* ptr_filter;
//    assert(remote_dataindex_mrs.size() == 1);
    size_t chunk_num_flush = remote_data_mrs.size() + remote_dataindex_mrs.size();
    size_t chunk_num_filter = remote_filter_mrs.size();
    bool RPC_flush = rdma_mg->Remote_Memory_Deallocation_Fetch_Buff(
        &ptr_flush, chunk_num_flush, shard_target_node_id, FlushBuffer);
    bool RPC_filter = rdma_mg->Remote_Memory_Deallocation_Fetch_Buff(
        &ptr_filter, chunk_num_filter, shard_target_node_id, FilterChunk);
    size_t index_flush = 0;
    size_t index_filter = 0;
    for (it = remote_data_mrs.begin(); it != remote_data_mrs.end(); it++) {
      ptr_flush[index_flush] = (uint64_t)it->second->addr;
//      DEBUG_arg("deallocated data address is %p", ptr[index]);
      index_flush++;
      delete it->second;
    }
    for (it = remote_dataindex_mrs.begin(); it != remote_dataindex_mrs.end(); it++) {
      ptr_flush[index_flush] = (uint64_t)it->second->addr;
//      DEBUG_arg("deallocated data address is %p", ptr[index]);
      index_flush++;
      delete it->second;
    }
    for (it = remote_filter_mrs.begin(); it != remote_filter_mrs.end(); it++) {
      ptr_filter[index_filter] = (uint64_t)it->second->addr;
//      DEBUG_arg("deallocated data address is %p", ptr[index]);
      index_filter++;
      delete it->second;
    }
    assert(index_flush == chunk_num_flush);
    assert(index_filter == chunk_num_filter);
    if (RPC_flush){
      rdma_mg->Memory_Deallocation_RPC(shard_target_node_id, FlushBuffer);
    }
    if (RPC_filter){
      rdma_mg->Memory_Deallocation_RPC(shard_target_node_id, FilterChunk);
    }
    return true;
  }
  bool Local_blocks_deallocate(std::map<uint32_t , ibv_mr*> map){
    std::map<uint32_t , ibv_mr*>::iterator it;

    for (it = map.begin(); it != map.end(); it++){
//      if(!rdma_mg->Deallocate_Local_RDMA_Slot(it->second->addr, "FlushBuffer")){
//        return false;
//      }
      delete it->second;
    }
    return true;
  }
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice& src);
  void mr_serialization(std::string* dst, ibv_mr* mr) const;
  std::shared_ptr<RDMA_Manager> rdma_mg;
  int this_machine_type;
  uint8_t creator_node_id;// The node id who create this SSTable. This could be a compute node

  // The memory node that store the shard for this SSTable, it has to be a memory node
  uint8_t shard_target_node_id;
  //  uint64_t refs;
  uint64_t level;
  uint64_t allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;

  // The uint32_t is the offset within the file.
  std::map<uint32_t, ibv_mr*> remote_data_mrs;
  std::map<uint32_t, ibv_mr*> remote_dataindex_mrs;
  std::map<uint32_t, ibv_mr*> remote_filter_mrs;
  //std::vector<ibv_mr*> remote_data_mrs
  uint64_t file_size;    // File size in bytes
  size_t num_entries;
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
  TableCache* table_cache = nullptr;
  bool UnderCompaction = false;
  Table_Type table_type;
};

class VersionEdit {
 public:
  typedef std::set<std::tuple<int, uint64_t, uint8_t>> DeletedFileSet;
  VersionEdit(uint8_t node_id) : target_node_id_(node_id) { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetFileNumbers(uint64_t file_number_end){
    for (auto pair : new_files_) {
      pair.second->number = file_number_end++;
    }
  }
  bool IsTrival(){
    return deleted_files_.size() == 1;
  }
  void GetTrivalFile(int& level, uint64_t& file_number, uint8_t& node_id){
    level = std::get<0>(*deleted_files_.begin());
    file_number = std::get<1>(*deleted_files_.begin());
    node_id = std::get<2>(*deleted_files_.begin());
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level,
               const std::shared_ptr<RemoteMemTableMetaData>& remote_table) {
    new_files_.emplace_back(level, remote_table);
  }
  std::vector<std::pair<int, std::shared_ptr<RemoteMemTableMetaData>>>* GetNewFiles(){
    return &new_files_;
  }
  DeletedFileSet* GetDeletedFiles(){
    return &deleted_files_;
  }
  void AddFileIfNotExist(int level, const std::shared_ptr<RemoteMemTableMetaData>& remote_table) {
    for(auto iter : new_files_){
      if (iter.second == remote_table){
        return;
      }
    }
    if (remote_table->file_size >0)
      new_files_.emplace_back(level, remote_table);
 }
  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file, uint8_t node_id) {
    //TODO(ruihong): remove this.
//    assert(node_id < 2);
//#ifndef NDEBUG
//    if(level > 0){
//      assert(node_id!= 0);
//    }
//#endif
    deleted_files_.insert(std::make_tuple(level, file, node_id));
  }
  size_t GetNewFilesNum(){
    return new_files_.size();
  }
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice src, int this_machine_type, TableCache* cache);
  void EncodeToDiskFormat(std::string* dst) const;
  Status DecodeFromDiskFormat(const Slice& src, int sstable_type);
  std::string DebugString() const;
  int compactlevel(){
//    return std::get<0>(*deleted_files_.begin());
    return std::get<0>(*new_files_.begin()) - 1;
  }
  uint8_t GetNodeID(){return target_node_id_;}
 private:
  friend class VersionSet;
  // level, file number, node_id

  uint8_t target_node_id_;
  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;// level, file_number, creator_node_id
  std::vector<std::pair<int, std::shared_ptr<RemoteMemTableMetaData>>> new_files_;
};
class VersionEdit_Merger {
 public:
  typedef std::set<std::tuple<int, uint64_t, uint8_t>> DeletedFileSet;
  void Clear(){
    deleted_files_.clear();
    new_files_.clear();
    only_trival_change.clear();
#ifndef NDEBUG
//    debug_map.clear();
#endif
  }
  void Swap(VersionEdit_Merger * ve_m){
    deleted_files_.swap(ve_m->deleted_files_);
    new_files_.swap(ve_m->new_files_);
    only_trival_change.swap(ve_m->only_trival_change);
#ifndef NDEBUG
    debug_map.swap(ve_m->debug_map);
#endif
  }
  void merge_one_edit(VersionEdit* edit);
  bool IsTrival(){
    return deleted_files_.size() == 1 && new_files_.size() == 1;
  }
  std::unordered_map<uint64_t , std::shared_ptr<RemoteMemTableMetaData>>* GetNewFiles(){
    return &new_files_;
  }

  DeletedFileSet* GetDeletedFiles(){
    return &deleted_files_;
  }
  size_t GetNewFilesNum() {
    return new_files_.size();
  }
  void EncodeToDiskFormat(std::string* dst) const;
  std::list<uint64_t> merged_file_numbers;
  bool ready_to_upin_merged_file;
  std::set<uint64_t> only_trival_change;
#ifndef NDEBUG
  std::set<uint64_t> debug_map;
#endif
 private:
  DeletedFileSet deleted_files_;

  int ve_counter = 0;
  std::unordered_map<uint64_t , std::shared_ptr<RemoteMemTableMetaData>> new_files_;
};
}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_DB_VERSION_EDIT_H_
