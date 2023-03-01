// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "TimberSaw/c.h"

#include <string.h>

#include <cstdint>
#include <cstdlib>

#include "TimberSaw/cache.h"
#include "TimberSaw/comparator.h"
#include "TimberSaw/db.h"
#include "TimberSaw/env.h"
#include "TimberSaw/filter_policy.h"
#include "TimberSaw/iterator.h"
#include "TimberSaw/options.h"
#include "TimberSaw/status.h"
#include "TimberSaw/write_batch.h"

using TimberSaw::Cache;
using TimberSaw::Comparator;
using TimberSaw::CompressionType;
using TimberSaw::DB;
using TimberSaw::Env;
using TimberSaw::FileLock;
using TimberSaw::FilterPolicy;
using TimberSaw::Iterator;
using TimberSaw::kMajorVersion;
using TimberSaw::kMinorVersion;
using TimberSaw::Logger;
using TimberSaw::NewBloomFilterPolicy;
using TimberSaw::NewLRUCache;
using TimberSaw::Options;
using TimberSaw::RandomAccessFile;
using TimberSaw::Range;
using TimberSaw::ReadOptions;
using TimberSaw::SequentialFile;
using TimberSaw::Slice;
using TimberSaw::Snapshot;
using TimberSaw::Status;
using TimberSaw::WritableFile;
using TimberSaw::WriteBatch;
using TimberSaw::WriteOptions;

extern "C" {

struct TimberSaw_t {
  DB* rep;
};
struct TimberSaw_iterator_t {
  Iterator* rep;
};
struct TimberSaw_writebatch_t {
  WriteBatch rep;
};
struct TimberSaw_snapshot_t {
  const Snapshot* rep;
};
struct TimberSaw_readoptions_t {
  ReadOptions rep;
};
struct TimberSaw_writeoptions_t {
  WriteOptions rep;
};
struct TimberSaw_options_t {
  Options rep;
};
struct TimberSaw_cache_t {
  Cache* rep;
};
struct TimberSaw_seqfile_t {
  SequentialFile* rep;
};
struct TimberSaw_randomfile_t {
  RandomAccessFile* rep;
};
struct TimberSaw_writablefile_t {
  WritableFile* rep;
};
struct TimberSaw_logger_t {
  Logger* rep;
};
struct TimberSaw_filelock_t {
  FileLock* rep;
};

struct TimberSaw_comparator_t : public Comparator {
  ~TimberSaw_comparator_t() override { (*destructor_)(state_); }

  int Compare(const Slice& a, const Slice& b) const override {
    return (*compare_)(state_, a.data(), a.size(), b.data(), b.size());
  }

  const char* Name() const override { return (*name_)(state_); }

  // No-ops since the C binding does not support key shortening methods.
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string* key) const override {}

  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(void*, const char* a, size_t alen, const char* b,
                  size_t blen);
  const char* (*name_)(void*);
};

struct TimberSaw_filterpolicy_t : public FilterPolicy {
  ~TimberSaw_filterpolicy_t() override { (*destructor_)(state_); }

  const char* Name() const override { return (*name_)(state_); }

  void CreateFilter(const Slice* keys, int n, Slice* dst) const override {
    std::vector<const char*> key_pointers(n);
    std::vector<size_t> key_sizes(n);
    for (int i = 0; i < n; i++) {
      key_pointers[i] = keys[i].data();
      key_sizes[i] = keys[i].size();
    }
    size_t len;
    char* filter = (*create_)(state_, &key_pointers[0], &key_sizes[0], n, &len);
    dst->append(filter, len);
    std::free(filter);
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
    return (*key_match_)(state_, key.data(), key.size(), filter.data(),
                         filter.size());
  }

  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*create_)(void*, const char* const* key_array,
                   const size_t* key_length_array, int num_keys,
                   size_t* filter_length);
  uint8_t (*key_match_)(void*, const char* key, size_t length,
                        const char* filter, size_t filter_length);
};

struct TimberSaw_env_t {
  Env* rep;
  bool is_default;
};

static bool SaveError(char** errptr, const Status& s) {
  assert(errptr != nullptr);
  if (s.ok()) {
    return false;
  } else if (*errptr == nullptr) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    // TODO(sanjay): Merge with existing error?
    std::free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }
  return true;
}

static char* CopyString(const std::string& str) {
  char* result =
      reinterpret_cast<char*>(std::malloc(sizeof(char) * str.size()));
  std::memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

TimberSaw_t* TimberSaw_open(const TimberSaw_options_t* options, const char* name,
                        char** errptr) {
  DB* db;
  if (SaveError(errptr, DB::Open(options->rep, std::string(name), &db))) {
    return nullptr;
  }
  TimberSaw_t* result = new TimberSaw_t;
  result->rep = db;
  return result;
}

void TimberSaw_close(TimberSaw_t* db) {
  delete db->rep;
  delete db;
}

void TimberSaw_put(TimberSaw_t* db, const TimberSaw_writeoptions_t* options,
                 const char* key, size_t keylen, const char* val, size_t vallen,
                 char** errptr) {
  SaveError(errptr,
            db->rep->Put(options->rep, Slice(key, keylen), Slice(val, vallen)));
}

void TimberSaw_delete(TimberSaw_t* db, const TimberSaw_writeoptions_t* options,
                    const char* key, size_t keylen, char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, Slice(key, keylen)));
}

void TimberSaw_write(TimberSaw_t* db, const TimberSaw_writeoptions_t* options,
                   TimberSaw_writebatch_t* batch, char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
}

char* TimberSaw_get(TimberSaw_t* db, const TimberSaw_readoptions_t* options,
                  const char* key, size_t keylen, size_t* vallen,
                  char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = db->rep->Get(options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

TimberSaw_iterator_t* TimberSaw_create_iterator(
    TimberSaw_t* db, const TimberSaw_readoptions_t* options) {
  TimberSaw_iterator_t* result = new TimberSaw_iterator_t;
  result->rep = db->rep->NewIterator(options->rep);
  return result;
}

const TimberSaw_snapshot_t* TimberSaw_create_snapshot(TimberSaw_t* db) {
  TimberSaw_snapshot_t* result = new TimberSaw_snapshot_t;
  result->rep = db->rep->GetSnapshot();
  return result;
}

void TimberSaw_release_snapshot(TimberSaw_t* db,
                              const TimberSaw_snapshot_t* snapshot) {
  db->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

char* TimberSaw_property_value(TimberSaw_t* db, const char* propname) {
  std::string tmp;
  if (db->rep->GetProperty(Slice(propname), &tmp)) {
    // We use strdup() since we expect human readable output.
    return strdup(tmp.c_str());
  } else {
    return nullptr;
  }
}

void TimberSaw_approximate_sizes(TimberSaw_t* db, int num_ranges,
                               const char* const* range_start_key,
                               const size_t* range_start_key_len,
                               const char* const* range_limit_key,
                               const size_t* range_limit_key_len,
                               uint64_t* sizes) {
  Range* ranges = new Range[num_ranges];
  for (int i = 0; i < num_ranges; i++) {
    ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
    ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
  }
  db->rep->GetApproximateSizes(ranges, num_ranges, sizes);
  delete[] ranges;
}

void TimberSaw_compact_range(TimberSaw_t* db, const char* start_key,
                           size_t start_key_len, const char* limit_key,
                           size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      // Pass null Slice if corresponding "const char*" is null
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void TimberSaw_destroy_db(const TimberSaw_options_t* options, const char* name,
                        char** errptr) {
  SaveError(errptr, DestroyDB(name, options->rep));
}

void TimberSaw_repair_db(const TimberSaw_options_t* options, const char* name,
                       char** errptr) {
  SaveError(errptr, RepairDB(name, options->rep));
}

void TimberSaw_iter_destroy(TimberSaw_iterator_t* iter) {
  delete iter->rep;
  delete iter;
}

uint8_t TimberSaw_iter_valid(const TimberSaw_iterator_t* iter) {
  return iter->rep->Valid();
}

void TimberSaw_iter_seek_to_first(TimberSaw_iterator_t* iter) {
  iter->rep->SeekToFirst();
}

void TimberSaw_iter_seek_to_last(TimberSaw_iterator_t* iter) {
  iter->rep->SeekToLast();
}

void TimberSaw_iter_seek(TimberSaw_iterator_t* iter, const char* k, size_t klen) {
  iter->rep->Seek(Slice(k, klen));
}

void TimberSaw_iter_next(TimberSaw_iterator_t* iter) { iter->rep->Next(); }

void TimberSaw_iter_prev(TimberSaw_iterator_t* iter) { iter->rep->Prev(); }

const char* TimberSaw_iter_key(const TimberSaw_iterator_t* iter, size_t* klen) {
  Slice s = iter->rep->key();
  *klen = s.size();
  return s.data();
}

const char* TimberSaw_iter_value(const TimberSaw_iterator_t* iter, size_t* vlen) {
  Slice s = iter->rep->value();
  *vlen = s.size();
  return s.data();
}

void TimberSaw_iter_get_error(const TimberSaw_iterator_t* iter, char** errptr) {
  SaveError(errptr, iter->rep->status());
}

TimberSaw_writebatch_t* TimberSaw_writebatch_create() {
  return new TimberSaw_writebatch_t;
}

void TimberSaw_writebatch_destroy(TimberSaw_writebatch_t* b) { delete b; }

void TimberSaw_writebatch_clear(TimberSaw_writebatch_t* b) { b->rep.Clear(); }

void TimberSaw_writebatch_put(TimberSaw_writebatch_t* b, const char* key,
                            size_t klen, const char* val, size_t vlen) {
  b->rep.Put(Slice(key, klen), Slice(val, vlen));
}

void TimberSaw_writebatch_delete(TimberSaw_writebatch_t* b, const char* key,
                               size_t klen) {
  b->rep.Delete(Slice(key, klen));
}

void TimberSaw_writebatch_iterate(const TimberSaw_writebatch_t* b, void* state,
                                void (*put)(void*, const char* k, size_t klen,
                                            const char* v, size_t vlen),
                                void (*deleted)(void*, const char* k,
                                                size_t klen)) {
  class H : public WriteBatch::Handler {
   public:
    void* state_;
    void (*put_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
    void (*deleted_)(void*, const char* k, size_t klen);
    void Put(const Slice& key, const Slice& value) override {
      (*put_)(state_, key.data(), key.size(), value.data(), value.size());
    }
    void Delete(const Slice& key) override {
      (*deleted_)(state_, key.data(), key.size());
    }
  };
  H handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.deleted_ = deleted;
  b->rep.Iterate(&handler);
}

void TimberSaw_writebatch_append(TimberSaw_writebatch_t* destination,
                               const TimberSaw_writebatch_t* source) {
  destination->rep.Append(source->rep);
}

TimberSaw_options_t* TimberSaw_options_create() { return new TimberSaw_options_t; }

void TimberSaw_options_destroy(TimberSaw_options_t* options) { delete options; }

void TimberSaw_options_set_comparator(TimberSaw_options_t* opt,
                                    TimberSaw_comparator_t* cmp) {
  opt->rep.comparator = cmp;
}

void TimberSaw_options_set_filter_policy(TimberSaw_options_t* opt,
                                       TimberSaw_filterpolicy_t* policy) {
  opt->rep.filter_policy = policy;
}

void TimberSaw_options_set_create_if_missing(TimberSaw_options_t* opt, uint8_t v) {
  opt->rep.create_if_missing = v;
}

void TimberSaw_options_set_error_if_exists(TimberSaw_options_t* opt, uint8_t v) {
  opt->rep.error_if_exists = v;
}

void TimberSaw_options_set_paranoid_checks(TimberSaw_options_t* opt, uint8_t v) {
  opt->rep.paranoid_checks = v;
}

void TimberSaw_options_set_env(TimberSaw_options_t* opt, TimberSaw_env_t* env) {
  opt->rep.env = (env ? env->rep : nullptr);
}

void TimberSaw_options_set_info_log(TimberSaw_options_t* opt, TimberSaw_logger_t* l) {
  opt->rep.info_log = (l ? l->rep : nullptr);
}

void TimberSaw_options_set_write_buffer_size(TimberSaw_options_t* opt, size_t s) {
  opt->rep.write_buffer_size = s;
}

void TimberSaw_options_set_max_open_files(TimberSaw_options_t* opt, int n) {
#if TABLE_STRATEGY==2
  opt->rep.max_table_cache_size;
#else
  opt->rep.max_open_files = n;
#endif
//      opt->rep.max_open_files = n;

}

void TimberSaw_options_set_cache(TimberSaw_options_t* opt, TimberSaw_cache_t* c) {
  opt->rep.block_cache = c->rep;
}

void TimberSaw_options_set_block_size(TimberSaw_options_t* opt, size_t s) {
  opt->rep.block_size = s;
}

void TimberSaw_options_set_block_restart_interval(TimberSaw_options_t* opt, int n) {
  opt->rep.block_restart_interval = n;
}

void TimberSaw_options_set_max_file_size(TimberSaw_options_t* opt, size_t s) {
  opt->rep.max_file_size = s;
}

void TimberSaw_options_set_compression(TimberSaw_options_t* opt, int t) {
  opt->rep.compression = static_cast<CompressionType>(t);
}

TimberSaw_comparator_t* TimberSaw_comparator_create(
    void* state, void (*destructor)(void*),
    int (*compare)(void*, const char* a, size_t alen, const char* b,
                   size_t blen),
    const char* (*name)(void*)) {
  TimberSaw_comparator_t* result = new TimberSaw_comparator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->compare_ = compare;
  result->name_ = name;
  return result;
}

void TimberSaw_comparator_destroy(TimberSaw_comparator_t* cmp) { delete cmp; }

TimberSaw_filterpolicy_t* TimberSaw_filterpolicy_create(
    void* state, void (*destructor)(void*),
    char* (*create_filter)(void*, const char* const* key_array,
                           const size_t* key_length_array, int num_keys,
                           size_t* filter_length),
    uint8_t (*key_may_match)(void*, const char* key, size_t length,
                             const char* filter, size_t filter_length),
    const char* (*name)(void*)) {
  TimberSaw_filterpolicy_t* result = new TimberSaw_filterpolicy_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->create_ = create_filter;
  result->key_match_ = key_may_match;
  result->name_ = name;
  return result;
}

void TimberSaw_filterpolicy_destroy(TimberSaw_filterpolicy_t* filter) {
  delete filter;
}

TimberSaw_filterpolicy_t* TimberSaw_filterpolicy_create_bloom(int bits_per_key) {
  // Make a TimberSaw_filterpolicy_t, but override all of its methods so
  // they delegate to a NewBloomFilterPolicy() instead of user
  // supplied C functions.
  struct Wrapper : public TimberSaw_filterpolicy_t {
    static void DoNothing(void*) {}

    ~Wrapper() { delete rep_; }
    const char* Name() const { return rep_->Name(); }
    void CreateFilter(const Slice* keys, int n, std::string* dst) const {
//      return rep_->CreateFilter(keys, n, dst);
    }
    bool KeyMayMatch(const Slice& key, const Slice& filter) const {
      return rep_->KeyMayMatch(key, filter);
    }

    const FilterPolicy* rep_;
  };
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = NewBloomFilterPolicy(bits_per_key);
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

TimberSaw_readoptions_t* TimberSaw_readoptions_create() {
  return new TimberSaw_readoptions_t;
}

void TimberSaw_readoptions_destroy(TimberSaw_readoptions_t* opt) { delete opt; }

void TimberSaw_readoptions_set_verify_checksums(TimberSaw_readoptions_t* opt,
                                              uint8_t v) {
  opt->rep.verify_checksums = v;
}

void TimberSaw_readoptions_set_fill_cache(TimberSaw_readoptions_t* opt, uint8_t v) {
  opt->rep.fill_cache = v;
}

void TimberSaw_readoptions_set_snapshot(TimberSaw_readoptions_t* opt,
                                      const TimberSaw_snapshot_t* snap) {
  opt->rep.snapshot = (snap ? snap->rep : nullptr);
}

TimberSaw_writeoptions_t* TimberSaw_writeoptions_create() {
  return new TimberSaw_writeoptions_t;
}

void TimberSaw_writeoptions_destroy(TimberSaw_writeoptions_t* opt) { delete opt; }

void TimberSaw_writeoptions_set_sync(TimberSaw_writeoptions_t* opt, uint8_t v) {
  opt->rep.sync = v;
}

TimberSaw_cache_t* TimberSaw_cache_create_lru(size_t capacity) {
  TimberSaw_cache_t* c = new TimberSaw_cache_t;
  c->rep = NewLRUCache(capacity);
  return c;
}

void TimberSaw_cache_destroy(TimberSaw_cache_t* cache) {
  delete cache->rep;
  delete cache;
}

TimberSaw_env_t* TimberSaw_create_default_env() {
  TimberSaw_env_t* result = new TimberSaw_env_t;
  result->rep = Env::Default();
  result->is_default = true;
  return result;
}

void TimberSaw_env_destroy(TimberSaw_env_t* env) {
  if (!env->is_default) delete env->rep;
  delete env;
}

char* TimberSaw_env_get_test_directory(TimberSaw_env_t* env) {
  std::string result;
  if (!env->rep->GetTestDirectory(&result).ok()) {
    return nullptr;
  }

  char* buffer = static_cast<char*>(std::malloc(result.size() + 1));
  std::memcpy(buffer, result.data(), result.size());
  buffer[result.size()] = '\0';
  return buffer;
}

void TimberSaw_free(void* ptr) { std::free(ptr); }

int TimberSaw_major_version() { return kMajorVersion; }

int TimberSaw_minor_version() { return kMinorVersion; }

}  // end extern "C"
