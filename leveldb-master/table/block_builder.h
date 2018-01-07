// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <vector>

#include <stdint.h>
#include "leveldb/slice.h"

namespace leveldb {

struct Options;

// BlockBuilder类是用来创建一个Block对象的，其创建的过程其实也就是
// 形成Block内存布局的过程。Block的内存布局如下：
// 一个Block的内存布局：
// ------------------
// |    Record 1    |
// ------------------
// |    Record 2    |
// ------------------
// |      ....      |
// ------------------
// |    Record n    |
// ------------------
// |    Restart 1   |
// ------------------
// |    Restart 2   |
// ------------------
// |      ....      |
// ------------------
// |    Restart m   |
// ------------------
// |  num_restarts  |
// ------------------
// 从上面的内存布局可以看到，一个Block内部可以分成两个部分，前面的Record数组
// 存放的是一些K-V记录，其顺序是根据Key值由小到大排列的。后面则是一个称为
// "重启点"的数组，存储是距离block开始处的偏移值，存放了Record数组中一些记录位置。
// "重启点"是干什么的呢？我们知道Block内部的k-v记录是从小到大排列的，这样的话，
// 相邻的两条或者多条记录的key值很有可能存在重叠部分，比如上图中的Record 1的
// key值为"my friend"，Record 2的key值为"my girlfriend"，那么两者就存在重叠部分
// "my "，为了减少key的存储量，Record 2中的key可能只存储和Record 1的key的不同部分，
// 即"girlfriend"，两者的相同部分则可以从Record 1中获取。基于上述的设计，"重启点"的
// 意思是：从这条记录开始，不再采取只存储不同的key部分，而是记录这条记录完整的
// key值，不管该记录的key值是否前一条记录有相同部分，例如Record 3就是一个重启点，
// 且他的key值为"your friend" 或者"my monther"，都将记录完整的key值，而不只是
// 和前一个记录的不同部分。而num_restarts则是"重启点"的个数。

// 前面我们说过，一个Record记录包含了k-v值，并且其中存储的key可能不是完整的key值，
// 而只是key的一部分值，那么为了适应这种设计，Record记录本身的内存布局是怎么样的呢？
// -------------------------------------------------------------
// |key共享长度|key非共享长度|value长度|key非共享内容|value内容|
// -------------------------------------------------------------
// 以上面的Record 1和Record 2为例，对于Record 2来说，key共享长度为"my "的长度，为3：
// key非共享长度为"girlfriend"的长度，为10，value长度的就是k-v的v的长度，key非共享内容
// 就是"girlfriend"，value内容就是k-v的v的内容。

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // Add()方法是用来往当前正在构建的Block中添加一个key-value记录。
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  // Finish()方法用来往Block中key-value记录后面追加重启点数组以及重启点数组长度信息的。
  // 当一个正在构建的Block的内存占用达到阈值时就不会再往这个Block中继续添加key-value记录了，
  // 这个时候就会在Block的末尾追加重启点数组以及重启点数组长度信息来结束这个Block的构建。
  // 这个block的内存布局是一致的。
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  // CurrentSizeEstimate()方法用来计算当前正在构建的Block的内存占用情况。
  // 也正是根据这个方法的返回值判断是否要结束这个Block的构建。
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options*        options_;

  // buffer_是用来存放block具体内容的缓冲区。
  std::string           buffer_;      // Destination buffer

  // restarts_动态数组中存放的就是每个重启点相对于block开始处的偏移，也就是
  // 重启点数组中元素的值。
  std::vector<uint32_t> restarts_;    // Restart points

  // counter_存放的是一个重启点对应的记录后面跟的key-value记录个数，
  // 如果这个个数达到了options_->block_restart_interval，那么就要重新
  // 创建一个重启点了，即要开始存放完整的key信息，而不仅仅只存放和前面key的
  // 不同部分了。
  int                   counter_;     // Number of entries emitted since restart

  // finished_是Finish()方法被调用过的标志。
  bool                  finished_;    // Has Finish() been called?

  // 上一次通过调用Add()方法加入到当前block中的key值。
  std::string           last_key_;

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
