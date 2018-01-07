// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

// GetLengthPrefixedSlice()函数用于从data中获取具体的内容，因为data中的
// 格式为： |var_length|content|
// 所以该函数先要从以varint编码的长度中解码出实际内容长度，然后返回指向
// 实际内容的Slice对象。
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

// ApproximateMemoryUsage()返回MemTable类实例的内存使用量。
size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

// 下面这个重载用于实现MemTable内部键值比较器KeyComparator的比较操作。我们知道在往
// MemTable类实例中插入一个元素的时候，元素的内存布局为：
//    klength  varint32
//    userkey  char[klength]
//    tag      uint64
//    vlength  varint32
//    value    char[vlength]
// 这也是为什么从MemTable类实例中Get一个元素的时候要先封装一个memtable_key[klength+internal_key]
// 所以KeyComparator类重载实现的比较函数要能比较memtable_key的大小，其实现方式为：
// 先从klength+internal_key组成的字符串中解析出internal key。然后再利用
// InternalKeyComparator类实现的internal key比较函数来比较internal key大小。
// InternalKeyComparator类实现的比较函数也会调用上层调用者传入的比较user key的函数。
int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
// 用于编码一个Slice对象，编码完成后形成的内存布局格式为：
//     |var_length|content|
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

// MemTableIterator类用于实现MemTable类实例的迭代器，对外的效果就是可以迭代
// 返回MemTable中的元素信息。在类MemTable的定义中我们可以看到MemTable类内部
// 用来实现元素按序存放的功能是通过skiplist来实现的，所以MemTableIterator类
// 内部实现中也是对skiplist迭代器的封装，这从其私有数据成员iter_就能看出来。
class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  // 因为MemTableIterator()内部封装的是skiplist的迭代器，所以下面实现的迭代器
  // 接口也是通过封装skiplist的迭代器。
  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }

  // 在MemTable类实例的Add()方法中，被插入的key、value等信息按照一定格式的编码成一个entry，
  // 而entry被插入到MemTable内部的skiplist中时，entry被当做一个节点的key，这个key可以通过
  // skiplist的迭代器获得。
  // 编码格式如下：
  //    klength  varint32
  //    userkey  char[klength]
  //    tag      uint64
  //    vlength  varint32
  //    value    char[vlength]
  // 所以如果要获取key信息的话，需要先解析出klength，然后获取key对应的Slice对象。
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }

  // 根据上面的编码格式，获取value信息，首先需要找到vlength+value部分的起始地址，
  // 然后再进一步解析出vlength大小以及value的起始地址，再利用这两部分信息构造出
  // 一个包含了value信息的Slice对象作为函数返回值。
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  // 定义skiplist的迭代器实例
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

// NewIterator()用于创建一个MemTable类实例的迭代器，其实就是创建了一个
// MemTableIterator类实例。
Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

// Add()函数实现了MemTable类实例添加元素的操作，其输入参数包括序列号、类型
// 以及键值信息，其中类型包括kTypeDeletion和kTypeValue，为什么需要一个类型信息
// 呢？因为leveldb中的删除操作是延迟操作，删除操作也当做是一种插入操作，类型
// 信息就是用于区分是删除操作还是普通的插入操作。
// skiplist中的一个项的编码格式为:
//    klength  varint32
//    userkey  char[klength]
//    tag      uint64
//    vlength  varint32
//    value    char[vlength]

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  
  // internal key需要多8个字节用于存储sequence number和type。
  size_t internal_key_size = key_size + 8;

  // encoded_len计算的是一个entry所需要的内存使用量。
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);

  // 首先往buf中写入以varint32格式编码的internal key大小信息。
  char* p = EncodeVarint32(buf, internal_key_size);
  
  // 然后是写入user-key、sequence number和type。
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;

  // 接着是写入以varint32格式编码的value的大小信息。
  p = EncodeVarint32(p, val_size);

  // 最后是写入value信息。
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == encoded_len);

  // entry编码完成之后就插入到skiplist中。
  table_.Insert(buf);
}

// Get()函数用于从MemTable类实例中获取一个key的信息，其输入参数是一个类型为
// LookupKey的实例，给类型封装了internal key和memtable key。其中的memtable key
// 就是Add()函数在编码entry时用到的，其格式为：
//  key_size     : varint32 of internal_key.size()
//  key bytes    : char[internal_key.size()]
// 而其中的internal key格式为：
//  user-key     : user key content
//  tag          : sequence number | type
// 因为元素插入的时候使用的是memtab_key+value，所以MemTable类内部比较器KeyComparator
// 比较的是memtable_key的大小，这样才能正确比较元素大小。同理，在Get的时候我们给内部
// 迭代器传入的也需要时memtable_key，因为内部迭代器(泛型)会使用KeyComparator来比较
// 元素大小，插入时也是一样的。
// 我们往skiplist插入的时候插入的就是key+value信息，只是我们在get的时候只用了key信息。
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {

  // 从LookupKey实例中获取memtable key。
  Slice memkey = key.memtable_key();

  // 创建一个skiplist的迭代器实例
  Table::Iterator iter(&table_);

  // 尝试用memtable_key查找元素，如果找到的话，那么返回的迭代器是可用的。
  // 如果迭代器可用，那么久可以从迭代器中获取对应的key+value信息，因为
  // 因为我们往skiplist插入的时候插入的就是key+value信息，只是我们在get的
  // 时候只用了key信息。接着就可以根据编码格式解码出value信息了。
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    // iter.key()就是Add的时候添加进去的entry结构。
    const char* entry = iter.key();
    uint32_t key_length;

	// GetVarint32Ptr()函数从中取出key的长度，并返回指向key内容的指针。
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);

	// 然后调用上层调用者指定的Compare()函数比较entry中的user key和
	// Get()参数中的key是否一致，如果一样的话，说明找到了key对应的元素。
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      
      // Correct user key
      // 解析type，如果是kTypeValue类型，则取出value信息，通过出参返回给调用者；
      // 如果类型是kTypeDeletion，那么说明这个元素是无效的，之前已经被标记为
      // 删除状态，所以返回未找到的信息。
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
