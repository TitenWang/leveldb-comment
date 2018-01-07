// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

// EncodeFixed32()函数用于将32位无符号value编码到buf中。在buf中，无论是小端机器还是大端机器，
// 都要求buf中的内容按照小端机器的形式进行存放，所以在编码的时候要求在大端机器上面
// 不能直接将value值拷贝到buf中，而需要将每个字节分别进行拷贝，即将value中的低字节值
// 拷贝到buf中的低字节，value中的高字节值放到buf中的高字节。
void EncodeFixed32(char* buf, uint32_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

// EncodeFixed64()用于将value的值编码到buf中。编码的规则同编码32位无符号整型一样。如下：
// 在buf中，无论是小端机器还是大端机器，都要求buf中的内容按照小端机器的形式进行存放，
// 所以在编码的时候要求在大端机器上面不能直接将value值拷贝到buf中，而需要将每个字节
// 分别进行拷贝，即将value中的低字节值拷贝到buf中的低字节，value中的高字节值放到buf中的高字节。
void EncodeFixed64(char* buf, uint64_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

// PutFixed32()函数用于把value值编码然后追加到dst这个动态字符串中。
void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

// PutFixed64()函数用于把value值编码然后追加到dst这个动态字符串中。
void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

// 以varint编码方式将32位无符号整型v编码到dst字符数组中，varint的编码规则为：
// 对于待编码的v而言，在进行varint编码的时候，会对其二进制值以7位为组进行分组，
// 目标缓冲区中一个字节中的8位bit中，低7位用于存放具体的内容，最高位是一个标志位，
// 该标志位为1表示v中的高位分组内容还被编码在了缓冲区的高字节中，为0表示该字节中的
// 内容是v的最高分组，到此为止，v的7位分组已经被编码完了。
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;

  // 如果v的值小等于2^7，那么用一个字节就可以编码完；
  // 如果v的值大于2 ^ 7，小于等于2 ^ 14，那么用两个字节就可以编码完；
  // 如果v的值大于2 ^ 14，小于等于2 ^ 21，那么用三个字节就可以编码完；
  // 如果v的值大于2 ^ 21，小于等于2 ^ 28，那么用四个字节就可以编码完；
  // 如果v的值大于2 ^ 28，小于等于2 ^ 32，那么就需要五个字节来编码。
  // 对于varint这样的编码方式，当待编码的值在区间(2 ^ 28, 2 ^ 32)之间的
  // 时候会比uint32_t的普通编码方式多一个字节。
  if (v < (1<<7)) {
    *(ptr++) = v;
  } else if (v < (1<<14)) {

  	// 低位分组对应的字节最高位被置位，表示后面还有内容。
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

// PutVarint32()函数用于把32位无符号value值以varint的编码方式进行编码，
// 然后追加到dst这个动态字符串中。
void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

// EncodeVarint64()用于将64位无符号整型以varint编码方式编码到dst缓冲区中。
// 具体编码方式同32位无符号整型一样。这个实现方式值得学习~。
char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

// PutVarint64()函数用于把64位无符号value值以varint的编码方式进行编码，
// 然后追加到dst这个动态字符串中。
void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

// 用于将Slice类实例的内容编码到dst动态字符串中，具体编码方式为：
// 1.对于Slice类实例的内容大小size用varint的编码方式编码后放置到dst中；
// 2.对于Slice类实例的内容则直接追加到dst中。
// 对于Slice类的实例来说，经过编码之后在缓冲区中形成了如果的编码格式：
// ----------------------
// | varint size | data |
// ----------------------
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

// VarintLength()函数用于计算如果用varint方式编码一个64位无符号整型的话，所需要内存大小。
int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

// GetVarint32PtrFallback()函数用于从p指向的内存中解码出一个32位无符号整型，
// 然后将返回值设置成缓冲区中未被解码的第一个字节的地址。其中p是用于
// 存放待解码数据的缓冲区，limit指向的是缓冲区中存放了目标值编码信息
// 最后一个字节的下一个字节，即表示要从缓冲区中解码目标值，最多到limit
// 的上一个字节即可。
const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;

    // byte & 128 不等于0，说明该字节的最高位不为0，该位不为0，说明缓冲区
    // 中的高字节还存放了目标值的数据，需要继续解码缓冲区高字节才能得到
    // 完整的目标值；否则说明缓冲区中该字节就是用于编码目标值的最后一个字节。

    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

// GetVarint32()用于从Slice类实例中解码出一个32位无符号整型，然后将Slice类实例中
// 剩余未被解码的部分生成一个新的Slice实例作为出参，如果Slice类实例中内容被解码完毕，
// 则以NULL作为出参。解码结果是否正确以返回的true或者false来判断。
bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

// GetVarint64Ptr()函数用于从p指向的缓冲区中解码出一个64位无符号整型，缓冲区区间为[p, limit)。
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

// GetVarint64()用于从Slice类实例中解码出一个64位无符号整型，然后将Slice类实例中
// 剩余未被解码的部分生成一个新的Slice实例作为出参，如果Slice类实例中内容被解码完毕，
// 则以NULL作为出参。解码结果是否正确以返回的true或者false来判断。
bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

// GetLengthPrefixedSlice()函数用于从p指向的缓冲区中解码出一个Slice类实例出来，其中
// 缓冲区区间为[p, limit)，而解码规则为函数PutLengthPrefixedSlice()中采用的编码方式的逆序。
// 对于Slice类的实例来说，经过编码之后在缓冲区中形成了如果的编码格式：
// ----------------------
// | varint size | data |
// ----------------------
// 所以在解码的时候，先是从缓冲区中以varint的逆序解码出Slice实例的长度信息，然后便直接是
// Slice类实例的内容部分了。
const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) {
  uint32_t len;

  // 先解码出长度字段，并返回数据部分的首地址。然后用长度和首地址即可构造出Slice实例。
  p = GetVarint32Ptr(p, limit, &len);
  if (p == NULL) return NULL;
  if (p + len > limit) return NULL;
  *result = Slice(p, len);
  return p + len;
}

// GetLengthPrefixedSlice()函数用于从一个存放了编码内容的Slice实例中解码出原有信息，
// 然后用原有信息构造出一个新的Slice实例来。解码结果是否正确以返回的true或者false来判断。
bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;

  // 解码规则：
  // 1. 先调用GetVarint32()函数，从存放了编码内容的Slice实例中解码出原信息的长度，
  //    然后更新Slice实例input中未被解码部分组成的新Slice实例赋值给input。此时input
  //    实例内部缓冲区的起始部分就是目标值内容的开始
  // 2. 然后利用第一步中解码得到的长度和数据起始地址构造出一个结果Slice作为出参，并
  //    将Slice实例input的内部缓冲区首地址更新到指向未被解码的数据内容的第一个字节。
  if (GetVarint32(input, &len) &&
      input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb
