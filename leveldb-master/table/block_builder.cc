// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);

  // 对于一个block来说，第一个key-value记录的key肯定是完整存放的，所以第一个重启点
  // 就是block开始处，即偏移为0。
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

// CurrentSizeEstimate()方法用来计算当前正在构建的Block的内存占用情况。
// 也正是根据这个方法的返回值判断是否要结束这个Block的构建。这个地方除了buffer_
// 的大小之外，还需要加上重启点数组的内存占用以及最后一个用来存放重启点数组
// 个数的4个字节。
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

// 当一个Block的内存占用达到阈值的时候就会调用Finish()方法在最后面一个key-value记录
// 后面追加重启点数组以及重启点数组的个数。
Slice BlockBuilder::Finish() {
  // Append restart array
  // restarts_动态数组中存放的就是每一个重启点对应的key-value记录相对于block开始处的
  // 偏移，这里往buffer_中的key-value记录后面追加重启点数组，接着往后面继续追加数组的
  // 长度信息。
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

// Add()方法是用来往当前正在构建的Block中添加一个key-value记录Record。
// 一个Record记录包含了k-v值，并且其中存储的key可能不是完整的key值，
// 而只是key的一部分值，那么为了适应这种设计，Record记录本身的内存布局是怎么样的呢？
// -------------------------------------------------------------
// |key共享长度|key非共享长度|value长度|key非共享内容|value内容|
// -------------------------------------------------------------
// 以上面的Record 1和Record 2为例，对于Record 2来说，key共享长度为"my "的长度，为3：
// key非共享长度为"girlfriend"的长度，为10，value长度的就是k-v的v的长度，key非共享内容
// 就是"girlfriend"，value内容就是k-v的v的内容。

void BlockBuilder::Add(const Slice& key, const Slice& value) {

  // 获取上一次加入到当前正在构建的block中的key-value记录的key值。
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;

  // 如果counter_小于options_->block_restart_interval，说明当前加入的这个key-value
  // 记录不会被当作一个新的重启点，所以这个key-value记录的key信息在block中只会
  // 存放和之前key不同的那部分；如果counter_等于options_->block_restart_interval，
  // 说明需要重新开启一个重启点了，这个重启点对应的首条记录就是本次加入的key-value记录，
  // 这个key会在block中被完整的存放，相应的restarts_数组中也会追加buffer_当前的长度
  // 作为这个新的重启点对应的偏移，因为本次加入的key-value记录就会在buffer_后面追加到
  // block中。
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string

	// 计算上一个key和本次加入的的key之间共同部分的长度
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }

  // 计算上一次加入到block中的key和本次加入的key之间不同部分的长度。
  // 如果是要开启一个新的重启点的话，那么shared等于0，non_shared就会等于
  // 本次加入的key的长度。
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  // 下面按照key-value记录的内存布局开始存放相应的数据
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  // 到这里为止，本次要被加入的key-value记录已经追加到存放block具体信息的动态字符串
  // buffer_中了，这个时候更新last_key_的值，因为本次加入到的key-value相对于下一次
  // 被加入的key-value来说就是上一次加入的key-value记录了，所以将last_key_设置为本次
  // 加入的key值。
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);

  // 同时累加下属于当前重启点的记录个数。"属于当前重启点的记录个数"的意思就是说
  // 从当前重启点对应的那个记录开始，到下一个重启点对应的记录加入到block之前的
  // 记录个数，其中重启点对应的那条记录是重启点的对应的多条记录中的首条记录。
  counter_++;
}

}  // namespace leveldb
