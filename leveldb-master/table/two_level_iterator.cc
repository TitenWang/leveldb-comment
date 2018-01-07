// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

// BlockFunction回调函数用于创建一个迭代器
typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  // Valid()返回内层迭代器是否有效
  virtual bool Valid() const {
    return data_iter_.Valid();
  }

  // key()返回内层迭代器的key信息
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }

  // value()返回内层跌第七的value信息。
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }

  // status()返回迭代器的状态，首先检查外城迭代器的状态，如果ok，则继续检查内层迭代器
  // 的状态，如果也是ok的那么就返回整体的状态。
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;  // block_function用于根据外层迭代器的值创建一个内层迭代器
  void* arg_;  // arg通常存放了两层迭代器都会访问的对象
  const ReadOptions options_;
  Status status_;  // 二级迭代器自身的状态

  IteratorWrapper index_iter_;  // 外层迭代器，通常是索引迭代器
  IteratorWrapper data_iter_; // May be NULL // 内层迭代器，通常数据内容迭代器
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  // data_block_handle_字符串存放着index_iter_迭代器当前指向的记录的value部分的内容，
  // 通常也就是内层迭代器将要遍历的对象的信息，比如位置和大小信息。
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL) {
}

TwoLevelIterator::~TwoLevelIterator() {
}

// Seek()方法用来将内层迭代器data_iter_指向key值大于等于target的最小key值
// 对应的data block中的记录。然后就可以通过key()、value()等函数获取到具体记录信息了。
void TwoLevelIterator::Seek(const Slice& target) {

  // 通过调用索引迭代器index_iter_的Seek()可以找到存放了target所属的那个data block的
  // 位置和大小信息的记录。然后调用InitDataBlock()函数可以获取到target所属的那个
  // data block的迭代器，然后调用data block的迭代器data_iter_的Seek()接口找到data block中
  // key值大于等于target的最小key值对应的记录，进而可以通过key()、value()等接口获取到最终的
  // key-value信息。至于为什么可以通过二级迭代器找到对应的记录，可以参照sstable的构造
  // 过程table_builer.cc文件的138行的注解。
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

// SeekToFirst()方法用来将内层迭代器data_iter_指向第一个data block的第一条key-value记录。
void TwoLevelIterator::SeekToFirst() {

  // 调用索引迭代器index_iter_的SeekToFirst()可以让index_iter_指向index block中的第一条记录。
  // 然后调用InitDataBlock()函数可以获取到index block中第一条记录存放的位置和大小信息对应的
  // data block的迭代器。接着调用data block的迭代器data_iter_的SeekToFirst()找到该data block中
  // 的第一条key-value记录。
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

// SeekToLast()方法用来将内核迭代器data_iter_指向最后一个data block的最后一条key-value记录。
void TwoLevelIterator::SeekToLast() {

  // 调用索引迭代器index_iter_的SeekToLast()可以让index_iter_指向index block中的最后一条记录。
  // 然后调用InitDataBlock()函数可以获取到index block中最后一条记录存放的位置和大小信息对应的
  // data block的迭代器。接着调用data block的迭代器data_iter_的SeekToLast()找到该data block中
  // 的最后一条key-value记录。
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

// Next()方法用于将内层迭代器指向当前记录的下一条记录。这个下一条记录有两种情况，第一种情况
// 就是这个下一条记录和当前迭代器所指向的记录属于同一个data block，这个时候只要将内层迭代器
// 往后移动一条记录即可。第二种情况就是当前内层迭代器指向的记录刚好是某个data block的最后一条
// 记录，那么这个时候就需要将内层迭代器指向下一个data block的第一条记录，这样情况的下一条记录
// 是逻辑上的下一条记录，对应的第一种情况可以说是物理上的下一条记录。
void TwoLevelIterator::Next() {
  assert(Valid());

  // 将内层迭代器后移一个位置。如果在执行下面这条语句之前，data_iter_已经指向了data block中
  // 的最后一条记录，那么调用Next()会使得data_iter_变成一个无效的迭代器。
  data_iter_.Next();

  // 调用SkipEmptyDataBlocksForward()方法用于判断内层迭代器data_iter_是否有效或者是否为空
  // 如果无效的话，说明data_iter_已经指向了data block的数据部分的末尾，这个时候需要让data_iter_
  // 指向下一个data block的第一条记录，这样的跳跃需要借助索引迭代器，让索引迭代器后移，从而
  // 可以从索引迭代器中获取到下一个data block对应的位置和大小信息，接着进一步可以获取到下一个
  // data block及其对应的迭代器，然后通过更新后的内层迭代器获取到下一个data block的key-value
  // 记录。
  SkipEmptyDataBlocksForward();
}


// Prev()方法用于将内层迭代器指向当前记录的上一条记录。这个上一条记录有两种情况，第一种情况
// 就是这个上一条记录和当前迭代器所指向的记录属于同一个data block，这个时候只要将内层迭代器
// 往前移动一条记录即可。第二种情况就是当前内层迭代器所指向的记录刚好是某个data block的第一条
// 记录，那么这个时候需要将内层迭代器指向前一个data block的第一条记录，这种情况的上一条记录
// 是逻辑上的上一条记录，对应的第一种情况可以说是物理上的上一条记录。
void TwoLevelIterator::Prev() {
  assert(Valid());

  // 将内层迭代器前移一个位置，如果在指向下面这条语句之前，data_iter_已经指向了data block中
  // 的第一条记录，那么调用Prev()会使得data_iter_变成一个无效的迭代器。这部分的实现可以参考
  // block.cc中Prev()的注解。
  data_iter_.Prev();

  // 调用SkipEmptyDataBlocksBackward()方法用于判断内层迭代器data_iter_是否有效或者是否为空
  // 如果无效的话，说明data_iter_已经指向了data block的数据部分的末尾（为什么data block的
  // 迭代器无效时总会指向data block数据部分的末尾呢？可以参考block.cc的Prev()注解），这个时候
  // 需要让data_iter_指向上一个data block的最后一条记录，这样的跳跃需要借助索引迭代器，让索引
  // 迭代器前移，从而可以从索引迭代器中获取到上一个data block对应的位置和大小信息，接着进一步
  // 可以获取到上一个data block及其对应的迭代器，然后通过调用内层迭代器的SeekToLast()让其指向
  // 上一个data block的最后一条key-value记录。
  SkipEmptyDataBlocksBackward();
}


// 调用SkipEmptyDataBlocksForward()方法用于判断内层迭代器data_iter_是否有效或者是否为空
// 如果无效的话，说明data_iter_已经指向了data block的数据部分的末尾，这个时候需要让data_iter_
// 指向下一个data block的第一条记录，这样的跳跃需要借助索引迭代器，让索引迭代器后移，从而
// 可以从索引迭代器中获取到下一个data block对应的位置和大小信息，接着进一步可以获取到下一个
// data block及其对应的迭代器，然后通过调用内层迭代器的SeekToFirst接口获取到下一个data block的
// key-value第一条记录。
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {

    // 如果索引迭代器index_iter_不可用了，即Valid()为false，说明索引迭代器已经遍历完了
    // index block中的所有索引记录了，那么对应的也就不能生成内层迭代器了，因为内层迭代器
    // 所迭代的对象已经没了，该对象的位置和大小信息正是由索引迭代器所指向记录的value包含的，
    // 既然索引记录都没了，那么对应的对象也就没了，所以这里将data_iter_设置为NULL。直接返回。
    // 不管迭代器是因为指向第一条记录再往前移动导致的无效，还是指向最后一条记录再往后移动导致
    // 的无效，其最终都会使得迭代器指向block的数据部分的末尾，重启点数组的开始位置。这部分可以
    // 参考block.cc中Prev()的实现注解。将sstable的二级迭代器代入有助于理解。
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }

	// Move to next block
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  }
}

// 调用SkipEmptyDataBlocksBackward()方法用于判断内层迭代器data_iter_是否有效或者是否为空
// 如果无效的话，说明data_iter_已经指向了data block的数据部分的末尾（为什么data block的
// 迭代器无效时总会指向block的数据部分的末尾呢？可以参考block.cc的Prev()注解），这个时候
// 需要让data_iter_指向上一个data block的最后一条记录，这样的跳跃需要借助索引迭代器，让索引
// 迭代器前移，从而可以从索引迭代器中获取到上一个data block对应的位置和大小信息，接着进一步
// 可以获取到上一个data block及其对应的迭代器，然后通过调用内层迭代器的SeekToLast()让其指向
// 上一个data block的最后一条key-value记录。
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {


    // 如果索引迭代器index_iter_不可用了，即Valid()为false，说明索引迭代器已经遍历完了
    // index block中的所有索引记录了，那么对应的也就不能生成内层迭代器了，因为内层迭代器
    // 所迭代的对象已经没了，该对象的位置和大小信息正是由索引迭代器所指向记录的value包含的，
    // 既然索引记录都没了，那么对应的对象也就没了，所以这里将data_iter_设置为NULL。直接返回。
    // 不管迭代器是因为指向第一条记录再往前移动导致的无效，还是指向最后一条记录再往后移动导致
    // 的无效，其最终都会使得迭代器指向block的数据部分的末尾，重启点数组的开始位置。这部分可以
    // 参考block.cc中Prev()的实现注解。将sstable的二级迭代器代入有助于理解。
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }

	// Move to next block
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  }
}

// 设置data block的迭代器。
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

// InitDataBlock()用索引迭代器index_iter_当前所指向的记录值来获取对应的内层迭代器。
// 其中index_iter_当前所指向的记录的值包含了用来生成内层迭代器的data block的位置和大小
// 信息，block_function_回调函数利用外层记录的值就可以生成一个内层迭代器，并设置到
// TwoLevelIterator类实例的data_iter_私有成员中。
void TwoLevelIterator::InitDataBlock() {

  // 如果索引迭代器index_iter_不可用了，即Valid()为false，说明索引迭代器已经遍历完了
  // index block中的所有索引记录了，那么对应的也就不能生成内层迭代器了，因为内层迭代器
  // 所迭代的对象已经没了，该对象的位置和大小信息正是由索引迭代器所指向记录的value包含的，
  // 既然索引记录都没了，那么对应的对象也就没了，所以这里将data_iter_设置为NULL。
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);
  } else {

    // 如果索引迭代器有效，说明其指向了可用的记录，那么从记录中获取值，得到对应的data
    // block的位置和大小信息。如果这个data block对应的迭代器已经有了，说明之前已经生成过
    // 迭代器，那么就什么都不做；如果这个data block对应的迭代器还没有生成，那么就调用
    // block_function_回调函数生成一个data block的迭代器，并设置data block的位置和大小信息
    // 设置到data_block_handle_中。
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

// NewTwoLevelIterator()函数用于创建一个二级迭代器，其中index_iter是外层迭代器，
// 该迭代器的value保存着用于创建内层迭代器的对象信息。block_function回调函数则
// 是根据外层迭代器的value创建一个内层迭代器。arg参数通常存放着两层迭代器需要
// 遍历的对象。
Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
