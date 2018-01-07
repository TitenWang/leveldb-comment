// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_SLICE_H_
#define STORAGE_LEVELDB_INCLUDE_SLICE_H_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>
#include "leveldb/export.h"

namespace leveldb {

// Slice类封装了一个数据片段及其相关的操作。数据片段主要是通过内部的data_和size_成员
// 实现的，data_字段是一个char*类型，一般指向一个字符数组，size_则是这个字符数组的长度。
class LEVELDB_EXPORT Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) { }

  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) { }

  // Create a slice that refers to the contents of "s"
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) { }

  // Create a slice that refers to s[0,strlen(s)-1]
  Slice(const char* s) : data_(s), size_(strlen(s)) { }

  // Return a pointer to the beginning of the referenced data
  const char* data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  // 重载Slice类实例的下标操作符。
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  // clear()用于清空Slice类实例的内部数据
  void clear() { data_ = ""; size_ = 0; }

  // Drop the first "n" bytes from this slice.
  // remove_prefix()函数用于移除Slice实例自身的前n个字节数据。
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  // ToString()用于将Slice实例信息封装成一个string类型的对象
  std::string ToString() const { return std::string(data_, size_); }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  // starts_with()函数用于判断Slice实例x是不是以Slice实例this作为前缀。
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_, x.data_, x.size_) == 0));
  }

 private:
  //data_成员通常指向一个字符数组，而size_则是字符数组的长度。
  const char* data_;
  size_t size_;

  // Intentionally copyable
};

// 重载Slice类实例的相等判断操作。
inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

// 重载Slice类实例的不想等判断逻辑，其利用了已经实现的相等判断操作。
inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

// Slice::compare()函数用于判断Slice实例b和this之间的大小关系
// 其中如果this < b，则返回-1；this == b，返回 0，this > b，返回1；
inline int Slice::compare(const Slice& b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  // 先利用memcmp()库函数进行初步判断，如果这一步可以判断出大于和小于关系，则可以直接返回
  // 两者之间的逻辑关系；但是如果这一步判断出两者相等，则需要进一步判断两者的大小关系，
  // 这个时候长度大的就大。
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_INCLUDE_SLICE_H_
