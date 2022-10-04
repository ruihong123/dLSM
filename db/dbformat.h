// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_DB_DBFORMAT_H_
#define STORAGE_TimberSaw_DB_DBFORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "TimberSaw/comparator.h"
#include "TimberSaw/db.h"
#include "TimberSaw/filter_policy.h"
#include "TimberSaw/slice.h"
//#include "TimberSaw/table_builder_computeside.h"

#include "util/coding.h"
#include "util/logging.h"

namespace TimberSaw {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 6;
static const int MaxImmuNumPerFlush = 1;
// Immutable flushing will be triggered when hit this number
static const int Immutable_FlushTrigger = 1;
// Maximum number of unflushed immutable files 10 in 1-1,
// 16 totally across shards in M-M (including memtable)
// with 8 fixed shard per compute node this equals 2 or 1
static const int Immutable_StopWritesTrigger = 15; // should be 15, add memtable should be totally 16
// Level-0 compaction is started when we hit this many files.
static const int kL0_CompactionTrigger = 1;

// Soft limit on number of level-0 files.  We slow down writes at this point.
// in M-M the total number of kL0_SlowdownWritesTrigger accross shard is 24
// in 1-1 (0 shard) this value is 16
// with 8 fixed shard per compute node this equals 2
static const int kL0_SlowdownWritesTrigger = 16;

// Maximum number of level-0 files.  We stop writes at this point.
// in M-M the total number of kL0_SlowdownWritesTrigger accross shard is 40
// in 1-1 (0 shard) this value is 32
// with 8 fixed shard per compute node this equals 4
static const int kL0_StopWritesTrigger = 32;
// We  can set it as 48*64 Mega byte for the first level, then there will
// be two levels after the random file benchmark. 1-1 256.0
// M-M still 256. ahigher number of this value can result in low write performance
// decrease with the number of shards.
// with 8 fixed shard per compute node this equals 256.0, 3200.0
static const double max_mega_bytes_for_level_base = 256.0;


// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576;

}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue;

typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);

struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) {}
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

// Pack a sequence number and a ValueType into a uint64_t
inline uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  return (seq << 8) | t;
}
// Given the result of PackSequenceAndType, store the sequence number in *seq
// and the ValueType in *t.
inline void UnPackSequenceAndType(uint64_t packed, uint64_t* seq,
                                  ValueType* t) {
  *seq = packed >> 8;
  *t = static_cast<ValueType>(packed & 0xff);

  assert(*seq <= kMaxSequenceNumber);
}
// The class to store keys in an efficient way. It allows:
// 1. Users can either copy the key into it, or have it point to an unowned
//    address.
// 2. For copied key, a short inline buffer is kept to reduce memory
//    allocation for smaller keys.
// 3. It tracks user key or internal key, and allow conversion between them.
class IterKey {
 public:
  IterKey()
      : buf_(space_),
        key_(buf_),
        key_size_(0),
        buf_size_(sizeof(space_)),
        is_user_key_(true) {}
  // No copying allowed
  IterKey(const IterKey&) = delete;
  void operator=(const IterKey&) = delete;

  ~IterKey() { ResetBuffer(); }

  // The bool will be picked up by the next calls to SetKey
  void SetIsUserKey(bool is_user_key) { is_user_key_ = is_user_key; }

  // Returns the key in whichever format that was provided to KeyIter
  Slice GetKey() const { return Slice(key_, key_size_); }

  Slice GetInternalKey() const {
    assert(!IsUserKey());
    return Slice(key_, key_size_);
  }

  Slice GetUserKey() const {
    if (IsUserKey()) {
      return Slice(key_, key_size_);
    } else {
      assert(key_size_ >= 8);
      return Slice(key_, key_size_ - 8);
    }
  }

  size_t Size() const { return key_size_; }

  void Clear() { key_size_ = 0; }

  void AppendToBack(const char* data, size_t appended_size){
    size_t total_size = key_size_ + appended_size;
    if (IsKeyPinned() /* key is not in buf_ */) {
      // Copy the key from external memory to buf_ (copy shared_len bytes)
      EnlargeBufferIfNeeded(total_size);
      memcpy(buf_, key_, key_size_);
    } else if (total_size > buf_size_) {
      // Need to allocate space, delete previous space
      char* p = new char[total_size];
      memcpy(p, key_, key_size_);

      if (buf_ != space_) {
        delete[] buf_;
      }

      buf_ = p;
      buf_size_ = total_size;
    }
    memcpy(buf_ + key_size_, data, appended_size);
    key_ = buf_;
    key_size_ = total_size;
  }
  // Append "non_shared_data" to its back, from "shared_len"
  // This function is used in Block::Iter::ParseNextKey
  // shared_len: bytes in [0, shard_len-1] would be remained
  // non_shared_data: data to be append, its length must be >= non_shared_len
  void TrimAppend(const size_t shared_len, const char* non_shared_data,
                  const size_t non_shared_len) {
    assert(shared_len <= key_size_);
    size_t total_size = shared_len + non_shared_len;

    if (IsKeyPinned() /* key is not in buf_ */) {
      // Copy the key from external memory to buf_ (copy shared_len bytes)
      EnlargeBufferIfNeeded(total_size);
      memcpy(buf_, key_, shared_len);
    } else if (total_size > buf_size_) {
      // Need to allocate space, delete previous space
      char* p = new char[total_size];
      memcpy(p, key_, shared_len);

      if (buf_ != space_) {
        delete[] buf_;
      }

      buf_ = p;
      buf_size_ = total_size;
    }

    memcpy(buf_ + shared_len, non_shared_data, non_shared_len);
    key_ = buf_;
    key_size_ = total_size;
  }

  Slice SetKey(const Slice& key, bool copy = true) {
    // is_user_key_ expected to be set already via SetIsUserKey
    return SetKeyImpl(key, copy);
  }

  Slice SetUserKey(const Slice& key, bool copy = true) {
    is_user_key_ = true;
    return SetKeyImpl(key, copy);
  }

  Slice SetInternalKey(const Slice& key, bool copy = true) {
    is_user_key_ = false;
    return SetKeyImpl(key, copy);
  }

  // Copies the content of key, updates the reference to the user key in ikey
  // and returns a Slice referencing the new copy.
  Slice SetInternalKey(const Slice& key, ParsedInternalKey* ikey) {
    size_t key_n = key.size();
    assert(key_n >= 8);
    SetInternalKey(key);
    ikey->user_key = Slice(key_, key_n - 8);
    return Slice(key_, key_n);
  }

  // Copy the key into IterKey own buf_
  void OwnKey() {
    assert(IsKeyPinned() == true);

    Reserve(key_size_);
    memcpy(buf_, key_, key_size_);
    key_ = buf_;
  }

  // Update the sequence number in the internal key.  Guarantees not to
  // invalidate slices to the key (and the user key).
  void UpdateInternalKey(uint64_t seq, ValueType t) {
    assert(!IsKeyPinned());
    assert(key_size_ >= 8);
    uint64_t newval = (seq << 8) | t;
    EncodeFixed64(&buf_[key_size_ - 8], newval);
  }

  bool IsKeyPinned() const { return (key_ != buf_); }// Pinned key means the key is located outside buffer,
  // whihc means you can not modify this pinned key.

  void SetInternalKey(const Slice& key_prefix, const Slice& user_key,
                      SequenceNumber s,
                      ValueType value_type = kValueTypeForSeek,
                      const Slice* ts = nullptr) {
    size_t psize = key_prefix.size();
    size_t usize = user_key.size();
    size_t ts_sz = (ts != nullptr ? ts->size() : 0);
    EnlargeBufferIfNeeded(psize + usize + sizeof(uint64_t) + ts_sz);
    if (psize > 0) {
      memcpy(buf_, key_prefix.data(), psize);
    }
    memcpy(buf_ + psize, user_key.data(), usize);
    if (ts) {
      memcpy(buf_ + psize + usize, ts->data(), ts_sz);
    }
    EncodeFixed64(buf_ + usize + psize + ts_sz,
                  PackSequenceAndType(s, value_type));

    key_ = buf_;
    key_size_ = psize + usize + sizeof(uint64_t) + ts_sz;
    is_user_key_ = false;
  }

  void SetInternalKey(const Slice& user_key, SequenceNumber s,
                      ValueType value_type = kValueTypeForSeek,
                      const Slice* ts = nullptr) {
    SetInternalKey(Slice(), user_key, s, value_type, ts);
  }

  void Reserve(size_t size) {
    EnlargeBufferIfNeeded(size);
    key_size_ = size;
  }

  void SetInternalKey(const ParsedInternalKey& parsed_key) {
    SetInternalKey(Slice(), parsed_key);
  }

  void SetInternalKey(const Slice& key_prefix,
                      const ParsedInternalKey& parsed_key_suffix) {
    SetInternalKey(key_prefix, parsed_key_suffix.user_key,
                   parsed_key_suffix.sequence, parsed_key_suffix.type);
  }

  void EncodeLengthPrefixedKey(const Slice& key) {
    auto size = key.size();
    EnlargeBufferIfNeeded(size + static_cast<size_t>(VarintLength(size)));
    char* ptr = EncodeVarint32(buf_, static_cast<uint32_t>(size));
    memcpy(ptr, key.data(), size);
    key_ = buf_;
    is_user_key_ = true;
  }

  bool IsUserKey() const { return is_user_key_; }

 private:
  char* buf_;
  const char* key_;
  size_t key_size_;
  size_t buf_size_;
  char space_[32];  // Avoid allocation for short keys
  bool is_user_key_;

  Slice SetKeyImpl(const Slice& key, bool copy) {
    size_t size = key.size();
    if (copy) {
      // Copy key to buf_
      EnlargeBufferIfNeeded(size);
      memcpy(buf_, key.data(), size);
      key_ = buf_;
    } else {
      // Update key_ to point to external memory
      key_ = key.data();
    }
    key_size_ = size;
    return Slice(key_, key_size_);
  }

  void ResetBuffer() {
    if (buf_ != space_) {
      delete[] buf_;
      buf_ = space_;
    }
    buf_size_ = sizeof(space_);
    key_size_ = 0;
  }

  // Enlarge the buffer size if needed based on key_size.
  // By default, static allocated buffer is used. Once there is a key
  // larger than the static allocated buffer, another buffer is dynamically
  // allocated, until a larger key buffer is requested. In that case, we
  // reallocate buffer and delete the old one.
  void EnlargeBufferIfNeeded(size_t key_size) {
    // If size is smaller than buffer size, continue using current buffer,
    // or the static allocated one, as default
    if (key_size > buf_size_) {
      EnlargeBuffer(key_size);
    }
  }

  void EnlargeBuffer(size_t key_size);
};
// Returns the user key portion of an internal key.
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;

 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
  const char* Name() const override;
  int Compare(const Slice& a, const Slice& b) const override;
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override;
  void FindShortSuccessor(std::string* key) const override;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_;

 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
  const char* Name() const override;
  void CreateFilter(const Slice* keys, int n, Slice* dst) const override;
  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
class InternalKey {
 private:
  std::string rep_;

 public:
  InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }
  InternalKey(ParsedInternalKey& key) {
    AppendInternalKey(&rep_, key);
  }
  bool DecodeFrom(const Slice& s) {
    rep_.assign(s.data(), s.size());
    return !rep_.empty();
  }
  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const { return ExtractUserKey(rep_); }

  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};

inline int InternalKeyComparator::Compare(const InternalKey& a,
                                          const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  uint8_t c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  return (c <= static_cast<uint8_t>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  LookupKey(const LookupKey&) = delete;
  LookupKey& operator=(const LookupKey&) = delete;

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // Return the user key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_;
  const char* kstart_;
  const char* end_;
  char space_[200];  // Avoid allocation for short keys
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}
//void FindNextInternalKeySuccessor(Slice* key) {
//  ParsedInternalKey ikey;
//  ParseInternalKey(*key, &ikey);
//  ikey.sequence +=1;
//
//}
}  // namespace TimberSaw


#endif  // STORAGE_TimberSaw_DB_DBFORMAT_H_
