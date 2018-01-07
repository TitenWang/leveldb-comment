// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  // 过滤策略
  const FilterPolicy* policy_;

  // keys_动态字符串中存放了加入到filter中的key值。keys_中的key值个数达到一个阈值之后
  // 就会将这一个key集合用来生成一个过滤器，生成过滤器之后会将keys_内容清除，为创建下一个
  // key集合对应的过滤器做准备。
  std::string keys_;              // Flattened key contents

  // start_动态数组中存放的是每个key在keys_动态字符串中的索引起始值。当keys_中的key集合
  // 对应的过滤器创建完毕之后就会将start_清空，为创建下一个key集合对应的过滤器做准备。
  std::vector<size_t> start_;     // Starting index in keys_ of each key

  // 存放每一个key集合生成的过滤器。多个key集合对应的过滤器会以追加的方式
  // 存放到result_中
  std::string result_;            // Filter data computed so far

  // 在创建Filter的时候，用来存放从keys_中取出来的所有key值。如何结合start_
  // 从keys_中取出每一个key值可以参考GenerateFilter()的实现。当tmp_keys中
  // 的key集合对应的过滤器创建完毕之后就会将tmp_keys清空，为创建下一个key集合对应的
  // 过滤器做好准备。
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument

  // filter_offsets_动态数组存放的是每一个过滤器在result_动态字符串中的索引起始值。
  std::vector<uint32_t> filter_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;  //
  // data_指向了存放着多个过滤器的内存起始地址
  const char* data_;    // Pointer to filter data (at block-start)

  // offset_指向了存放着过滤器在data_中的起始索引值的内存。
  const char* offset_;  // Pointer to beginning of offset array (at block-end)

  // num_记录的是data_中存放的过滤器个数。
  size_t num_;          // Number of entries in offset array

  // kFilterBaseLg的值
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
