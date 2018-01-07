// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <assert.h>

namespace leveldb {

// leveldb的Arena类用于向操作系统一次性申请的内存大小，即4K字节。
static const int kBlockSize = 4096;

// 构造函数将memory_usage_的内部指针初始化为0，指向地址为0的内存处，表明此时没有内存使用
Arena::Arena() : memory_usage_(0) {
  alloc_ptr_ = NULL;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
}

// Arena类的析构函数，将blocks_维护的向操作系统申请的内存统一进行释放。这样的话，在
// 内存使用过程中就可以不用考虑内存释放问题，因为内存会在Arena类实例析构的时候统一
// 进行释放操作。
Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

// Arena::AllocateFallback()函数根据bytes大小给上层调用者返回可用内存，如果bytes大于
// kBlockSize / 4的话，则申请一块bytes大小内存直接返回给上层调用者；如果bytes小于等于
// kBlockSize的话，则先申请一块kBlockSize大小内存，然后从这块新的内存中分配bytes大小
// 内存给上层调用者。同时更新本次申请之后内部可用内存首地址alloc_ptr_和可用大小
// alloc_bytes_remaining_的值。
char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  // 这个地方可能会导致部分内存的浪费。举个例子，假设之前的内存分配使得alloc_bytes_remaining_
  // 为256B，但是本次上层调用者需要申请的内存大小为512B，那么alloc_ptr_指向的内存不够本次
  // 分配，那么这个时候leveldb就会重新申请一块大小为kBlockSize的内存，并让alloc_ptr_指向这块
  // 新内存，那alloc_ptr_原来指向的那块大小为256B的内存就没有被使用了，由于alloc_ptr_被重新
  // 赋值，所以原有那块256B内存就找不到了。
  // 个人觉得如果需要将内存分配控制得更精细一些的话，可以在内部用一个数组来维护内部可用内存
  // 以上面那种情况为例，如果alloc_ptr_指向的内存不够用了，那么就用数组元素保存其地址，后续
  // 当上层调用者需要的内存小于256B时，则可以使用其进行分配。
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  // 更新alloc_ptr_和alloc_bytes_remaining_的值，用于后续内存分配。
  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

// 函数Arena::AllocateAligned()用于给上层调用者返回一块字节对齐的内存。该函数相对于
// 函数Arena::Allocate()来说的主要区别再于会返回一个内存首地址是字节对齐的内存。
char* Arena::AllocateAligned(size_t bytes) {

  // 首先先计算出当前操作系统的字节对齐方式，即计算出操作系统内存地址是几字节对齐的。
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2

  // 这一步是计算alloc_ptr_指向的内存首地址相对于操作系统字节对齐的偏移量。
  // 以64位机器为例，上面计算可以得出align为8，假设经过之前的一些内存分配，
  // alloc_ptr_指向的内存地址的低四位为1011，那么下面计算出来的current_mod为0011，
  // 也就是说如果上层调用者需要的内存地址是字节对齐的话，那么alloc_ptr_就需要向
  // 后移动(align - current_mod)字节大小使其指向的内存地址是字节对齐的。也就是
  // 说要让alloc_ptr_指向的内存的地址是8的倍数。
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);

  // needed是总共需要的内存大小，包括上层调用者需要的大小以及因为地址对齐而浪费的大小。
  size_t needed = bytes + slop;
  char* result;

  // 下面的实现步骤就和Arena::Allocate()函数的一样了。
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}

// Arena::AllocateNewBlock()函数用于向操作系统申请一块大小为block_bytes的内存，
// 并将这块内存追加到blocks_这个vector中，后续归还给操作系统的时候需要使用到。
// 另外，leveldb会将本次申请的内存大小记录起来，追加到memory_usage_成员中，用于
// 统计总的内存使用量。
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);

  // memory_usage_成员统计总的内存使用量是如何实现的呢?可以看到，memory_usage_
  // 成员是一个原子变量，其类型为void *，其用于统计总的内存使用量的原理是这样的：
  // 在Arena类初始化的时候，memory_usage_成员内部指针在构造函数内被初始化为0，
  // 也就是指向了地址为0的内存处，相当于目前没有内存申请，即使用量为0。当上层调用者
  // 申请一块内存时（以大于可BlockSize/4为例），最终会调用到Arena::AllocateNewBlock()函数
  // 那么leveldb先将类实例memory_usage_的内部指针强转为uintptr_t类型，然后在这个
  // 值基础上加上本次申请的大小，然后再强转会void *，那么memory_usage_的内部指针
  // 就指向了地址为block_bytes的内存，相当于目前使用了block_bytes的内存。
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}

}  // namespace leveldb
