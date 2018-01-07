// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

// SkipToInitialBlock()函数用于更新log文件的读取偏移点
bool Reader::SkipToInitialBlock() {
  // offset_in_block是初始逻辑记录在其所在的物理block中的偏移，而
  // initial_offset_s是初始逻辑记录在整个文件中的偏移。
  const size_t offset_in_block = initial_offset_ % kBlockSize;

  // block_start_location是初始逻辑记录所在的物理block在整个文件中的偏移。
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // 物理block的最后面可能会因为空间不足以容纳一个header大小，而填充一些
  // 零。所以如果初始逻辑记录在block内的偏移大于kBlockSize - 6，则需要从
  // 下一块block读取逻辑记录。
  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }

  // 更新下一个即将被读取的物理block的偏移。
  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  // 将log文件的读取位置移动到初始逻辑记录所在的block处。
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

// ReadRecord()用于返回log文件中的一个完整的逻辑记录。一个完整的逻辑记录在log
// 文件中可能只有一个逻辑记录段，即FullType类型，也有可能包括多个逻辑记录段，
// 而多个逻辑记录段可能分布在同一个block中，也有可能在多个block中。ReadRecord()
// 就需要根据ReadPhysicalRecord()返回的逻辑记录段类型做一些循环控制以读取足够的
// 逻辑记录段内容组成一条完整的逻辑记录。ReadRecord()函数的出参record用于在
// 所读取的逻辑记录没有分段时使用，该Slice封装的是内部缓冲区backing_store_中
// 属于本次逻辑记录的内容；scratch则是当所读取的逻辑记录在log中是分段存储的时候
// 用来存放具体的逻辑记录内容的。
// ReadPhysicalRecord()用于返回block中的一个逻辑记录段的类型、大小和内容其实地址。
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  // initial_offset_保存的是上层调用者期望的初始逻辑记录在log文件中的偏移，而
  // last_record_offset_记录的则是上一次调用ReadRecord()函数返回的逻辑记录在log
  // 文件中的偏移，如果last_record_offset_小于initial_offset_的话，那么需要将
  // log文件的读取点进行调整，不从上次调用ReadRecord()返回的记录所在block的下一个
  // block开始读取，二是从initial_offset_指定的初始逻辑记录所在的block开始读取。
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();

  // in_fragmented_record标识本条逻辑记录是否有分段
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  // prospective_record_offset记录的是逻辑记录的起始字节在log文件中的偏移。
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
  	// 调用ReadPhysicalRecord()返回一个逻辑记录段及该逻辑记录段的类型
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    // 在函数ReadPhysicalRecord()会更新buffer_，使其指向缓冲区中未被处理的内容，
    // end_of_buffer_offset_保存的是下次将要读取的block在文件中的偏移，fragment
    // 指向的是已经读取的逻辑记录段，那么physical_record_offset保存的就是本逻辑
    // 记录段开始部分在log文件中的偏移。
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
	  	// 如果逻辑记录段的类型是FullType，说明逻辑记录没有分段。而如果标志位
	  	// in_fragmented_record是true，说明之前的逻辑记录并没有读取完毕，而本次
	  	// 读取的逻辑记录段又属于一个新的逻辑记录，这样说明log文件中存放的逻辑记录
	  	// 是有问题的。
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }

		// prospective_record_offset记录的是逻辑记录的起始字节在log文件中的偏移。
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
		// 记录ReadRecord()返回的逻辑记录起始字节在log文件中的偏移。
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
	  	// 如果逻辑记录段的类型是FirstType，说明逻辑记录有分段。而如果标志位
	  	// in_fragmented_record是true，说明之前的逻辑记录并没有读取完毕，而本次
	  	// 读取的逻辑记录段又属于一个新的逻辑记录，这样说明log文件中存放的逻辑记录
	  	// 是有问题的。因为对于有分段的逻辑记录，在从log文件中读取到LastType类型
	  	// 的逻辑记录段以后就返回了。
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
		
		// 记录逻辑记录起始字节在log文件中的偏移，将内容存放到scratch中，并将
		// in_fragmented_record标志位置位。
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
	  	// MiddleType类型的逻辑记录段，如果检查无误的话，直接将内容追加到scratch中
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
	  	// LastType类型的逻辑记录段，如果逻辑上没有问题的话，就将内容追加到scratch
	  	// 中，更新逻辑记录起始字节在log文件中的偏移，并返回存放了逻辑记录内容的scratch
	  	// 对应的Slice对象。
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != NULL &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

// ReadPhysicalRecord()函数用于解析并返回block中的一个逻辑记录段内容，
// 这个逻辑记录段可能是FullType，FirstType，MiddleType或者LastType中的
// 一种。这个类型可以从该函数的返回值中获取。
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
  	// 如果buffer中剩余未处理内容的大小小于逻辑记录头部大小，说明需要读取新的
  	// block了。因为一条逻辑记录段在log文件中存储时肯定是以头部开始的，而且一条
  	// 逻辑记录段的头部和内容部分肯定会在同一个block中。buffer_.size() < kHeaderSize
  	// 说明buffer中剩余未处理内容不足以容纳一个逻辑记录段的头部，所以肯定不会有完整的
  	// 逻辑记录段。
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();

	    // 从log文件中读取大小为kBlockSize的内容，其中backing_store_是具体的
	    // 存放内容的内部缓冲区，而buffer_则是对backing_store_进行封装得到
	    // 的一个Slice对象，之所以要用Slice对象来封装backing_store_是为了
	    // 更好地对缓冲区进行操作。
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);

		// 更新end_of_buffer_offset_，即下一个将要被读取的block在文件中的偏移。
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
		  // 程序如果进入了这个分支，说明本次从文件中读取的内容不够一个kBlockSize
		  // 说明已经读到了log文件的结尾了，所以这里将eof_标志位置位，表示文件读取
		  // 完毕。
		  // 可以看到文件读取完毕之后会执行下面的continue，然后继续对buffer_对应缓冲区
		  // 中的内容做处理，如果有完整逻辑记录段，则做进一步处理，如果buffer_中实际内容
		  // 不足以容纳一个逻辑记录段的头部大小，那么返回kEof。
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

	// 程序执行到这里说明缓冲区中肯定存在至少一条完整的逻辑记录段，则对该逻辑记录段
	// 做进一步处理，其中包括按照编码格式解析头部，算出逻辑记录段的长度、类型和校验和
	// 校验和验证完毕后按照长度对backint_store_这个实际缓冲区返回一个对应的Slice对象。
	// 并将buffer_这个slice中包含的已经解析完的逻辑记录段部分移除，这里的移除不会删除
	// backing_store_中的内容，而只会移动Slice对象的起始地址，使其指向缓冲区中剩余
	// 未被处理的内容。
	
    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

	// 更新buffer中的起始地址部分，使buffer_这个Slice对象指向backing_store_中
	// 剩余未被处理的内容。
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    // end_of_buffer_offset_保存的是下次将要读取的block在文件中的偏移，
    // 而buffer_.size()计算的则是backing_store_中剩余未被处理的内容大小，
    // length是本次解析的逻辑记录段内容大小，kHeaderSize则是头部大小，
    // end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length计算的
    // 就是本次读取的逻辑记录段在整个log文件中的偏移。
    // 如果本次读取的逻辑记录段的偏移小于初始逻辑记录的偏移，那么这个逻辑
    // 记录段就不符合要求，返回一个kBadRecord。
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

	// 返回封装了本条逻辑记录段信息的Slice对象。
    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
