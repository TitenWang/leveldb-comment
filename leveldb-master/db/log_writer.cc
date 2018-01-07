// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() {
}

// 该函数用于向日志模块中添加一条操作日志，操作日志实际内容由调用者封装成一个数据片段Slice。
Status Writer::AddRecord(const Slice& slice) {

  // 获取数据片段的值和大小。
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
  	// 当前block中还可以用来存放日志记录(未被使用)的空间。
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);

	// 如果当前block中的可用空间已经不足以容纳下一条记录的头部内容，则直接切换到
	// 一个新的block，那么这部分剩余空间中也需要写入零值数据，作为一条零长记录。
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
		// 向dest_中写入内容
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }

	  // 因为要切换到一个新的block，所以这里将剩余可用的空间相对于block首地址的偏移清零。
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    // 上面的if语句判断程序走到这里一定有剩余空间用来存放实际的记录内容。
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

	// 计算当前block除去头部长度之后剩余可用的空间大小，
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;

	// 如果当前block中剩余可用空间多余记录剩余未写入的内容长度，那么可以直接将
	// 记录内容写入当前block中；如果当前block中剩余空间内容不足以写入记录剩余
	// 未写入内容，则尽可能多地写入记录内容，余下的写入到后面的block中；如果
	// 剩余未写入内容长度刚好等于当前block可用空间的话，那就直接写入。
	// fragment_length存放的是本次写入的记录长度。
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
	// left == fragment_length说明这是要写的是记录最后一部分内容了
    const bool end = (left == fragment_length);

    if (begin && end) {
	  // kFulltType说明本条记录在一个block中就完全写完了。
      type = kFullType;
    } else if (begin) {
      // kFirstType说明本条记录的第一部分写入到了当前block中。
      type = kFirstType;
    } else if (end) {
	  // kLastType说明本条记录的最后一部分写入到了当前block中。
      type = kLastType;
    } else {
	  // kMiddleType说明本条记录的中间部分写入到了当前block中。
      type = kMiddleType;
    }

	// 将本次待写的记录内容写入到文件中。
    s = EmitPhysicalRecord(type, ptr, fragment_length);

	// 更新指向记录中剩余未写入的内容的指针及剩余未写入文件的记录长度
    ptr += fragment_length;
    left -= fragment_length;

	// 对于一条记录来说，写了一次如果没写完，则后续的写就不再是begin了，
	// 也就是不可能为kFullType或者kFirstType了。
    begin = false;
  } while (s.ok() && left > 0);

  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  // 构造本次写入的头部信息，头部信息长度为7个字节，其中四个字节的校验和，
  // 两个字节的内容长度，一个字节的记录类型信息
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 写入头部信息，写成功后紧接着写入记录内容信息，写成功后刷新到文件中。
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }

  // 更新当前block中剩余可用空间起始地址相对于block起始地址的偏移。
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
