// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "TimberSaw/comparator.h"
#include "TimberSaw/env.h"
#include "TimberSaw/iterator.h"
#include "db/version_edit.h"
#include "util/coding.h"

namespace TimberSaw {
#ifdef PROCESSANALYSIS
std::atomic<uint64_t> MemTable::GetTimeElapseSum = 0;
std::atomic<uint64_t> MemTable::GetNum = 0;
std::atomic<uint64_t> MemTable::foundNum = 0;
#endif
//
//static Slice GetLengthPrefixedSlice(const char* data) {
//  uint32_t len;
//  const char* p = data;
//  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
//  return Slice(p, len);
//}
std::atomic_int64_t Memtable_created = 0;
std::atomic_int64_t Memtable_deallocated = 0;
MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator(cmp), refs_(0), table_(comparator, &arena_) {
//#ifndef NDEBUG
//  printf("Memtable %p  get created, total created %lu\n", this, Memtable_created.fetch_add(1));
//#endif
}

MemTable::~MemTable() {
//#ifndef NDEBUG
//  printf("Memtable %p  get deallocated,total deallocated %lu\n", this, Memtable_deallocated.fetch_add(1));
  assert(refs_ == 0);
//#endif

}

size_t MemTable::ApproximateMemoryUsage() { return arena_.ApproximateMemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}
int MemTable::KeyComparator::operator()(const char* prefix_len_key,
                                        const KeyComparator::DecodedType& key)
const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  //Here used to be CompareKeySeq which will drop the value type only keep sequence
  return comparator.Compare(a, key);
}


// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

  void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = nullptr;
  // TODO this is not correct since, the key and value should write to 1
  //  sizeof(Node) larger than the buf now!
  buf = table_.AllocateKey(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.InsertConcurrently(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
#ifdef PROCESSANALYSIS
  auto start = std::chrono::high_resolution_clock::now();
#endif
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
#ifdef PROCESSANALYSIS
          foundNum.fetch_add(1);
#endif
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
#ifdef PROCESSANALYSIS
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  std::printf("Get from memtable (not found) time elapse is %zu\n",  duration.count());
  GetTimeElapseSum.fetch_add(duration.count());
  GetNum.fetch_add(1);
#endif
  return false;
}

}  // namespace TimberSaw
