// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/logging.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace leveldb {

// AppendNumberTo()函数用于将number转换成一个字符串，并将结果追加到str中。
void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
  str->append(buf);
}

// 将Slice对象中的内容编码到str中，其中对于Slice对象value中的可打印字符，
// 那么直接将其追加到str中，而对于其中的不可打印字符，则以十六进制
// 形式将不可见字符转换成对应的整形数字，取最低字节内容，拼接在"\\x"之后，然后追加到str中。
void AppendEscapedStringTo(std::string* str, const Slice& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

// NumberToString()函数会将number转换成一个string对象
std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

// EscapeString()函数将Slice对象内容转换成字符串。
std::string EscapeString(const Slice& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

// ConsumeDecimalNumber()函数用于将Slice对象in中的数字部分转换为对应的数字，但受以下条件制约：
// 1. 如果Slice对象in不是以数字字符开始，那么将直接返回，将0赋值给*val，并返回false，表示*val值无效。
// 2. 遇到Slice对象in中第一个非数字字符后，就不再处理Slice对象in的后续内容，而直接将前面计算
//    得到的数字作为最终结果直接赋值给*val，返回true，表示值有效。
// 3. 如果Slice对象in中已经处理完的数字字符解析结果超过了64位无符号整形最大值，那么将认为值无效，
//    返回false，*val将会是0。
bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') {
      ++digits;
      // |delta| intentionally unit64_t to avoid Android crash (see log).
      const uint64_t delta = (c - '0');
	  // 如果
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64/10 ||
          (v == kMaxUint64/10 && delta > kMaxUint64%10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1);
    } else {
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

}  // namespace leveldb
