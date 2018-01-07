// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#ifndef STORAGE_LEVELDB_UTIL_CODING_H_
#define STORAGE_LEVELDB_UTIL_CODING_H_

#include <stdint.h>
#include <string.h>
#include <string>
#include "leveldb/slice.h"
#include "port/port.h"

namespace leveldb {

// Standard Put... routines append to a string
extern void PutFixed32(std::string* dst, uint32_t value);
extern void PutFixed64(std::string* dst, uint64_t value);
extern void PutVarint32(std::string* dst, uint32_t value);
extern void PutVarint64(std::string* dst, uint64_t value);
extern void PutLengthPrefixedSlice(std::string* dst, const Slice& value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
extern bool GetVarint32(Slice* input, uint32_t* value);
extern bool GetVarint64(Slice* input, uint64_t* value);
extern bool GetLengthPrefixedSlice(Slice* input, Slice* result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// NULL on error.  These routines only look at bytes in the range
// [p..limit-1]
extern const char* GetVarint32Ptr(const char* p,const char* limit, uint32_t* v);
extern const char* GetVarint64Ptr(const char* p,const char* limit, uint64_t* v);

// Returns the length of the varint32 or varint64 encoding of "v"
extern int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
extern void EncodeFixed32(char* dst, uint32_t value);
extern void EncodeFixed64(char* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char* EncodeVarint32(char* dst, uint32_t value);
extern char* EncodeVarint64(char* dst, uint64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

// DecodeFixed32()用于从ptr字符数组中解码32位的无符号整型数字
// 在buf中，无论是小端机器还是大端机器，都要求buf中的内容按照小端机器的形式进行存放，
// 所以在编码的时候要求在大端机器上面不能直接将value值拷贝到buf中，而需要将每个字节
// 分别进行拷贝，即将value中的低字节值拷贝到buf中的低字节，value中的高字节值放到buf中的
// 高字节。
// 所以在解码的时候需要按照编码规则的逆序从buf中解码出对应的数字。
inline uint32_t DecodeFixed32(const char* ptr) {

  // 在小端序机器上面，则可以直接拷贝；
  // 在大端序机器上面，则需要调整字节直接的顺序，然后组成和完整的数字。
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

// DecodeFixed64()用于从ptr字符数组中读取64位的无符号整型数字
inline uint64_t DecodeFixed64(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char* GetVarint32PtrFallback(const char* p,
                                          const char* limit,
                                          uint32_t* value);

// GetVarint32Ptr()函数用于从p指向的内存中解码出一个32位无符号整型，
// 然后将返回值设置成缓冲区中未被解码的第一个字节的地址。其中p是用于
// 存放待解码数据的缓冲区，limit指向的是缓冲区中存放了目标值编码信息
// 最后一个字节的下一个字节，即表示要从缓冲区中解码目标值，最多到limit
// 的上一个字节即可。
inline const char* GetVarint32Ptr(const char* p,
                                  const char* limit,
                                  uint32_t* value) {
  if (p < limit) {

  	// 判断缓冲区中第一个字节是否小于128，如果小于128，说明只要解码第一个字节即可
  	// 得到目标值。否则需要解码高字节内容以得到目标值。
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CODING_H_
