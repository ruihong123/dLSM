//
// Created by ruihong on 6/12/22.
//

#ifndef TIMBERSAW_DB_IMPL_SHARDING_H
#define TIMBERSAW_DB_IMPL_SHARDING_H
#include "db_impl.h"
namespace TimberSaw {
//shard info: [lower bound, upper bound)
class DBImpl_Sharding : public DB {
  friend class DBImpl;
 public:
  DBImpl_Sharding(const Options& options, const std::string& dbname);

  DBImpl_Sharding(const DBImpl&) = delete;
  DBImpl_Sharding& operator=(const DBImpl&) = delete;

  ~DBImpl_Sharding() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions& options) override;
#ifdef BYTEADDRESSABLE
  Iterator* NewSEQIterator(const ReadOptions& options) override;
#endif
  void WaitforAllbgtasks(bool clear_mem) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;
  std::map<Slice, DBImpl*, cmpBySlice>* GetShards_pool(){
    return &shards_pool;
  }
 private:
  bool Get_Target_Shard(DBImpl*& db_ptr, Slice key){
    auto iter = shards_pool.upper_bound(key);
    if(iter != shards_pool.end()){
      db_ptr = iter->second;
//#ifndef NDEBUG
//      Shard_Info.f
//      assert(key.compare());
//#endif
//      db_ptr->
      return true;
      // TODO: Also remember to check the lower bound if not return false.
    }else{
      return false;
    }
  }
    std::map<Slice, DBImpl*, cmpBySlice> shards_pool;// <upper bound, dbptr>
    // In case that the shard key buffer get deleted outside the DB.
    // THe range of every shard is [lower bound, upper bound).
    std::vector<std::pair<std::string, std::string>> Shard_Info;
//    std::vector<std::thread> Sharded_main_comm_threads;
//    int main_comm_thread_ready_num = 0;
//    std::condition_variable handler_threads_cv;
  };
}
#endif  // TIMBERSAW_DB_IMPL_SHARDING_H
