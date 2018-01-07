// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
// 每2K的数据生成一个过滤器。
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

// FilterBlockBuilder类构造函数，初始化过滤策略实例成员policy_。
FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

// leveldb中会为每2K的数据创建一个过滤器，filter_offsets_动态数组存放的是每一个
// 过滤器在result_动态字符串中的索引起始值。那么filter_offsets_动态数组的元素个数
// 其实就是目前已经创建的过滤器的个数。从上层调用者传入的参数可以看出block_offset
// 是目前sstable文件的大小。那么block_offset/kFilterBase计算的就是根据文件内容总的
// 大小应该要有的过滤器个数，如果目前已经创建的过滤器个数小于应有的过滤器个数，那么
// 就要为已经加入到keys_中的key值创建过滤器直到达到了目标的过滤器数量。
// leveldb会在每个data block创建完毕(即写入到sstable文件后)会调用StartBlock()方法
// 检测是否需要创建新的过滤器，如果文件内容新增大小没有达到2K，那么就不会创建过滤器。
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

// AddKey()方法用来往FilterBlockBuilder类实例中添加一个key信息，这个key也是
// 要加入到sstable文件的data block中。在往filter中添加一个key时，首先是将keys_
// 当前的长度信息存放到start_中，而将key内容直接追加到动态字符串中，那么就可以
// 根据start_中每个元素的值从keys_动态字符串中获取到对应的key值。因为start_中每个
// 元素的值就是对应的key值在keys_动态字符串中的索引起始值(起始下标)。
// 假设有"parent","brother"两个key调用了AddKey()，那么
// keys_的内容就是：
// ---------------
// |parentbrother|
// ---------------
// 而start_的内容就是：
// -----
// |0|6|
// -----
// 这样的话，类实例就可以通过start_数组中的元素值从动态字符串中获取到key值，每个key的
// 索引起始值可以从start_中直接获取，而长度则可以利用start_中前后两个元素的差值计算
// 得到。
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;

  // 将keys_动态字符串的长度存放到start_动态数组中
  // start_动态数组中存放的是每个key在keys_动态字符串中的索引起始值
  start_.push_back(keys_.size());

  // 将key值内容追加到keys_动态字符串中。
  keys_.append(k.data(), k.size());
}

// Finish()用来为filter block的创建工作做一些收尾。
Slice FilterBlockBuilder::Finish() {

  // start_动态数组不为空，说明上层调用者有调用AddKey()往filter block中加入key值
  // 那么就需要为这些加入到filter block中的key值建立过滤器。
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  // result_中存放了所有key集合生成的过滤器，每一个key集合过滤器在result_
  // 按先后顺序追加存放，不会覆盖。array_offset中存放的是所有key集合生成的过滤器
  // 总的大小。
  const uint32_t array_offset = result_.size();

  // filter_offsets_动态数组存放的是每一个过滤器在result_动态字符串中的索引起始值。
  // 在所有key集合对应的过滤器都已经写入result_之后，再将每个过滤器在result_中的索引
  // 起始值也以追加方式写入到result_中。
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 将每个过滤器的索引起始值追加写入到result_中之后，再将所有过滤器的总大小写入到
  // result_中，那么result_中的信息就足够用来从获取到每一个key集合对应的过滤器了。
  // result_的内存布局是所有过滤器+每个过滤器在result_中的索引起始值+存放每个过滤器
  // 索引起始值的内存起始索引。
  PutFixed32(&result_, array_offset);

  // 最后将kFilterBaseLg追加到result_末尾。并返回保存着result_信息的Slice对象。
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {

  // num_keys为目前加入到FilterBlockBuilder类实例中的key个数。
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  // 这个地方再往start_中追加一次keys_的长度，是为了计算最后一个加入到keys_中的key长度。
  // 因为在这里的实现中start_动态数组中只记录了每个key在keys_中的索引起始值，没有记录
  // 长度，所以还需要依靠前后两个索引值的差值计算前一个key的长度。所以这里需要在没有添加
  // 元素的情况下再追加一次keys_动态字符串的长度。
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  // 将keys_中记录的所有key值分别取出来，然后添加到tmp_keys这个动态数组中，而数组中
  // 的每一个元素就是从keys_中取出来的key值。
  for (size_t i = 0; i < num_keys; i++) {

  	// 取得每个key在keys_中的索引起始值，然后利用前后两个索引值的差值得到前一个key的长度
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i+1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // 在filter_offsets_动态数组中存放本次将要生成的过滤器在result_中的索引起始值。
  // 因为每一个key集合对应的过滤器都是以追加的方式存放到result_中的，所以本次
  // 即将生成的过滤器将从result_.size()位置开始存放。
  filter_offsets_.push_back(result_.size());

  // 调用上层调用者传入的CreateFilter()方法来为key集合创建一个过滤器。过滤器的默认
  // 实现为布隆过滤器，其实现可以参考bloom.cc。这一个key集合对应的过滤器会追加
  // 到动态字符串result_中，因为result_会用来存放每一个key集合生成的过滤器，多个
  // key集合生成的过滤器均为追加到result_中。
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  // 将tmp_keys、keys_和start_清零，为下一次key值集合建立过滤器做好准备
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

// FilterBlockReader类的构造函数，参数中的policy就是所使用的过滤器实现，
// 比如布隆过滤器，contents就是存放了多个过滤器内容的字符串信息，相当于
// FilterBlockBuilder类中的Finish()方法返回值。
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy),
      data_(NULL),
      offset_(NULL),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  // 在FilterBlockBuilder类的Finish方法中可以看到存放着过滤器信息的内存其至少
  // 需要5个字节的空间，因为在构建filter的收尾阶段是在内存的末尾追加存放着每个
  // 过滤器的索引起始值的内存起始位置以及kFilterBaseLg。
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n-1];

  // 解析出开始存放每个过滤器的索引起始值的位置，从这里位置往前就是多个过滤器
  // 实际内容，往后就是前面多个过滤器的索引起始值。
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

// 判断key是否在block_offset对应的过滤器中。
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
  	// index*4是为了找到存放了index下标对应的过滤器在data_中的索引起始值的内存起始处。
  	// 一个索引起始值占4个字节。这个可以从FilterBlockBuilder类的filter_offsets_成员
  	// 类型看出。
    uint32_t start = DecodeFixed32(offset_ + index*4);
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    // 应用过滤器判断key是否在其中。
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}
