// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "port/port.h"

namespace leveldb {

// 内存管理类
class Arena {
 public:
  Arena();
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  // 统计内存使用量
  size_t MemoryUsage() const {
    return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  // alloc_ptr_成员指向的是当前可提供给上层调用者申请的内存的首地址。在leveldb中
  // 为了避免频繁向操作系统申请内存，首次上层调用者申请内存时，leveldb会申请一个大小
  // 为kBlockSize的内存，将操作系统返回的地址赋值给alloc_ptr_。以上层申请大小为size的
  // 内存为例，当size < kBlockSize时，leveldb会从alloc_ptr_指向的内存块分配一块上层指定
  // 大小的内存，然后alloc_ptr_则会指向alloc_ptr_ + size的地址，alloc_bytes_remaining_
  // 则为kBlockSize - size。直到alloc_ptr_指向的可用内存不足以满足上层调用者需求时，
  // leveldb才会又申请一块大小为BlockSize的内存用于后续内存分配。如果size大于kBlockSize
  // 则leveldb会直接向操作系统申请对应大小的内存，并返回给上层调用者。
  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  // 在Arena类中，使用一个vector来管理申请的内存块，当Arena类析构时，统一将之前申请的所有
  // 内存返还给操作系统。
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  // 该私有变量用于统计内存使用量
  // memory_usage_成员统计总的内存使用量是如何实现的呢?可以看到，memory_usage_成员内部
  // 指针是一个原子变量，其内部指针类型为void *，其用于统计总的内存使用量的原理是这样的：
  // 在Arena类初始化的时候，memory_usage_成员内部指针在构造函数内被初始化为0，
  // 也就是指向了地址为0的内存处，相当于目前没有内存申请，即使用量为0。当上层调用者
  // 申请一块内存时（以大于可BlockSize/4为例），最终会调用到Arena::AllocateNewBlock()函数
  // 那么leveldb先将类实例memory_usage_的内部指针强转为uintptr_t类型，然后在这个
  // 值基础上加上本次申请的大小，然后再强转会void *，那么memory_usage_的内部指针
  // 就指向了地址为block_bytes的内存，相当于目前使用了block_bytes的内存。当然这个内存使用
  // 量统计是相对于操作系统而言的。
  port::AtomicPointer memory_usage_;

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

// Arena::Allocate()函数用于提供给上层调用者申请一块大小为bytes的内存，
// 该函数不保证给上层调用者的内存首地址是字节对齐的。如果上层调用者对
// 内存的字节对齐有要求，则可以使用Arena::AllocateAligned()函数。
inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);

  // 如果上层调用者申请的内存小于当前leveldb内部可用内存时，则直接使用
  // 内部可用内存给上层调用者分配内存，并同时更新本次申请之后内部可用内存
  // 首地址和剩余大小。否则的话，调用AllocateFallback()函数根据bytes大小
  // 或申请一块bytes大小内存直接返回给上层调用者，或申请一块kBlockSize大小
  // 内存，然后从这块新的内存中分配bytes大小内存给上层调用者。
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
