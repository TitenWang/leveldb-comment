// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "port/port.h"
#include "leveldb/status.h"

namespace leveldb {

// 申请一块内存，然后将state中的内容拷贝到新申请的内存中，然后将新申请内存作为返回值。
const char* Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  memcpy(result, state, size + 5);
  return result;
}

// Status::Status()是类Status的构造函数，利用code和msg构造出一个状态信息，状态信息的
// 编码格式为：
//    state_[0..3] == length of message
//    state_[4]    == code
//    state_[5..]  == message

Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != kOk);
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);

  // 多申请5个字节的内存，用于存放信息内容和状态码。
  char* result = new char[size + 5];

  // 按照上述格式构造状态信息。
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    memcpy(result + 7 + len1, msg2.data(), len2);
  }

  // 状态信息构造好之后存放到state_这个私有成员中。
  state_ = result;
}

std::string Status::ToString() const {

  // 获取状态码对应的字符串
  if (state_ == NULL) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      default:
        snprintf(tmp, sizeof(tmp), "Unknown code(%d): ",
                 static_cast<int>(code()));
        type = tmp;
        break;
    }

	// 将状态码对应的字符串用来初始化结果字符串。
    std::string result(type);
    uint32_t length;

	// state_中存放的第一部分是状态信息的长度部分。
    memcpy(&length, state_, sizeof(length));

	// 将状态信息内容追加到结果字符串中。到最后结果字符串中的字符串编码格式为：
	// -------------------------------
	// | code string | state content |
	// -------------------------------
    result.append(state_ + 5, length);
    return result;
  }
}

}  // namespace leveldb
