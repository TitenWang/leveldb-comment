// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  // Name()返回当前比较器实现的名字。
  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  // Compare()函数比较两个slice的大小，从实现可以看出其是利用了Slice对象
  // 的比较方法。
  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  // If *start < limit, changes *start to a short string in [start,limit)。
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    // 1. 找到start和limit的共同前缀，记录下两者不同部分的起始索引
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    // 经过上面的处理，diff_index就是start和limit不同部分的起始索引。
	// 如果diff_index大于等于min_length，说明start和limit中有一个是另外
	// 一个的前缀，在这种情况下无论谁是谁的前缀，我们都不做任何操作，
	// 因为如果start是limit的前缀，那么start天然满足[start, limit);
	// 如果limit是start的前缀，那么前提条件都不满足，所以也不做任何操作。
    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      // 如果diff_index小于min_length，说明start和limit还存在一部分不相同的。
      // 这种情况下，我们只判断不同部分的第一个字节，如果start中与limit
      // 不同部分的第一个字节内容不等于255，也比limit中对应字节至少小2，那么就
      // 将这个start中的这个字节加1，并修改start的长度，使得修改后的start动态字符串
      // 比原start动态字符串大，这样的start就满足了[start, limit)；如果start比
      // limit大的话，那前提条件不满足就不用做任何处理了。
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  // Changes *key to a short string >= *key，该函数的作用就是将key设置为一个
  // 比原来key大，但是比原来key短的动态字符串。其实现方法就是：
  //    遍历动态字符窜key，找到其中第一个值不为0xff的字节，然后将该字节的值
  // 在原来基础上加1，这样就使得新的动态字符串key就大于旧的动态字符串key了。
  // 然后修改新的动态字符串的大小。
  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

// 创建并返回一个bytewise比较器。
const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule);
  return bytewise;
}

}  // namespace leveldb
