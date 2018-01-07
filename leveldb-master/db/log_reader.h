// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <stdint.h>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-NULL, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  //
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  bool ReadRecord(Slice* record, std::string* scratch);

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  uint64_t LastRecordOffset();

 private:

  // log文件封装对象
  SequentialFile* const file_;

  // reporter_用于错误上报
  Reporter* const reporter_;

  // checksum_用于表示是否需要对校验和做校验处理
  bool const checksum_;
  
  // backing_store_缓冲区用于存放从log文件中读取到的block
  char* const backing_store_;

  // buffer_用于封装backing_store_缓冲区中剩余未被处理的内容，将未被处理的内容的
  // 起始地址和大小用一个Slice来保存。
  Slice buffer_;

  // 表示log文件是否已经读取完毕了。
  bool eof_;   // Last Read() indicated EOF by returning < kBlockSize

  // Offset of the last record returned by ReadRecord.
  // last_record_offset_存放的是上一次调用ReadRecord()函数返回的逻辑记录的
  // 起始字节在整个log文件中偏移。
  uint64_t last_record_offset_;

 
  // Offset of the first location past the end of buffer_.
  // end_of_buffer_offset_存放的是当前buffer结束位置存放的内容的在log文件中
  // 紧接着的下一个字节在整个文件中的偏移。往buffer中存放内容的地方只有一个，
  // 那就是从文件中读取一个block的时候，每次往buffer中存放一个block大小的内容
  // 时，就会更新一下end_of_buffer_offset_，所以end_of_buffer_offset_更准确的说
  // 是下一个将要被读取的block在整个文件中的偏移。
  uint64_t end_of_buffer_offset_;

  // Offset at which to start looking for the first record to return
  // log文件中初始逻辑记录距离log文件头部的偏移。
  uint64_t const initial_offset_;

  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
  // particular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode
  bool resyncing_;

  // Extend record types with the following special values
  enum {
    kEof = kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    kBadRecord = kMaxRecordType + 2
  };

  // Skips all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  bool SkipToInitialBlock();

  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(Slice* result);

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  // No copying allowed
  Reader(const Reader&);
  void operator=(const Reader&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
