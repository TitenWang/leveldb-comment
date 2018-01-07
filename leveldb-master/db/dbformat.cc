// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

// 利用ParsedInternalKey对象key编码一个internal key，然后存放到result中。
void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

// ParsedInternalKey类方法DebugString()以固定格式格式化ParsedInternalKey类实例信息，并
// 作为返回值。
std::string ParsedInternalKey::DebugString() const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d",
           (unsigned long long) sequence,
           int(type));
  std::string result = "'";
  result += EscapeString(user_key.ToString());
  result += buf;
  return result;
}

// DebugString()以固定格式格式化internal key的各个组成部分。
std::string InternalKey::DebugString() const {
  std::string result;
  ParsedInternalKey parsed;

  // 调用ParseInternalKey()从rep_解析出internal key的组成部分，并封装成一个
  // ParsedInternalKey类型的对象。如果解析结果为true，则解析成功，然后调用
  // ParsedInternalKey类方法DebugString()将其以固定格式进行格式化成字符串。
  if (ParseInternalKey(rep_, &parsed)) {
    result = parsed.DebugString();
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

// Name()函数用于返回比较器的名字
const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

// Compare()函数是InternalKeyComparator类实现两个key大小比较的函数，针对internal key的
// 比较规则如下：
// 1. 根据上层调用者提供的比较函数比较user key的大小，如果结果不等于，则直接返回比较结果。
// 2. 如果第一步中的比较结果等于0，那么需要sequence number和type，对于这两者的比较是按照
//    降序的方式比较，即a.seq > b.seq，则a < b。
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

// FindShortestSeparator()方法用于将internal key动态字符串start进行一些变化，规则为：
// 1. 首先调用user_comparator_的FindShortestSeparator方法，在user_start小于user_limit的情况下，
//    计算出大于等于user_start，而小于user_limit的最短动态字符串。
// 2. 如果第一步计算出来的tmp的长度小于user_start，而且tmp大于user_start，那么就将tmp当作
//    新的user_key，并在这之后添加上sequence number和type，形成新的internal key。替换start中原有
//    的内容
void InternalKeyComparator::FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

// 方法FindShortSuccessor()用于将internal key字符串key中的user key进行一些变化，规则为：
// Changes user_key to a short string >= user_key。其实现步骤为：
// 1. 首先调用user_comparator_的FindShortSuccessor()方法，计算出大于tmp的最短动态字符串，
//    并赋值给tmp。
// 2. 如果经过第一步计算出来的tmp比user_key短，而且user_key比tmp小，那么就将tmp部分当作
//    新的user_key，然后在该user_key后面追加上sequence number和type，共同组成一个新的
//    internal key。
void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char* InternalFilterPolicy::Name() const {
  return user_policy_->Name();
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

// LookupKey类的构造函数LookupKey()利用user_key和sequence number构造一个
// LookupKey类的实例。LookupKey类实例的内存布局如下：
//   klength	varint32			 <-- start_
//	 userkey  char[klength] 		 <-- kstart_
//	 tag	  uint64
//
//其中的klength是5个字节，因为32位整数采用varint32编码时最多需要使用5个字节，
// 然后tag则包括sequence number和type，共8个字节。
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();

  // 13个字节的内存空间用于存放internal key的varint32编码格式的长度、
  // sequence number以及type信息
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;

  // 如果key比较小，那么直接用space_的空间，可以避免内存申请；如果key比较大，那么
  // 就采用动态申请。
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb
