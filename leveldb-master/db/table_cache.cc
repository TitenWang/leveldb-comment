// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

// struct TableAndFile结构体封装了sstable文件以及用来操作sstable文件的内存对象。
struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}

// FindTable()用于返回file_number对应struct TableAndFile类实例。
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  // 将file_number编码到buf中，作为key值到cache_中去查找对应的记录，如果找到了
  // 记录，那么直接返回；如果没有找到记录，那么需要先创建一个存放在记录中对象，
  // 然后将对象插入到缓存中，并返回对应的记录。记录中的对象其实就是struct TableAndFile
  // 类实例。
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {

    // 根据既定的规则创建数据库文件名字，然后从文件中读取内容，并返回一个用来
    // 管理这些内容的Table类实例。
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      // 创建一个struct TableAndFile对象，并插入到缓存中，返回一个handle对象。
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

// NewIterator()方法用于创建管理着file_number对应的文件内容的Table实例的迭代器。
// 返回值是Table类实例的迭代器，而tableptr则是对应的Table类实例。
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  // 首先利用file_number找到保存着struct TableAndFile类实例的handle对象，
  // 然后从中取出TableAndFile类实例，接着创建一个table的迭代器并往这个迭代器中
  // 注册一个清理回调函数。
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  // 取出Table类实例，并创建一个table迭代器，这个迭代器是一个二层迭代器。外层迭代器
  // 是table中的index block的迭代器，用于遍历多个data block，而内层迭代器则是data
  // block的迭代器，用于遍历data block内的多个key-value记录。
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

// 首先利用file_number找到保存着struct TableAndFile类实例的handle对象，
// 然后从中取出TableAndFile类实例，接着找到其中的Table类实例，然后调用
// 其InternalGet()方法。
// InternalGet()方法用于获取待查询key值k所对应的在index block中的记录，如果找到了
// 这个记录的话，那么利用这个记录的value所保存的位置和大小信息对应的data block，
// 然后再创建一个data block的迭代器，并让迭代器指向key值大于等于k的最小key值
// 对应的记录，最后调用saver回调函数来处理在data block中找到的这个记录。
Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;

  // 首先利用file_number找到保存着struct TableAndFile类实例的handle对象，
  // 然后从中取出TableAndFile类实例，接着找到其中的Table类实例，然后调用
  // 其InternalGet()方法。
  // InternalGet()方法用于获取待查询key值k所对应的在index block中的记录，如果找到了
  // 这个记录的话，那么利用这个记录的value所保存的位置和大小信息对应的data block，
  // 然后再创建一个data block的迭代器，并让迭代器指向key值大于等于k的最小key值
  // 对应的记录，最后调用saver回调函数来处理在data block中找到的这个记录。
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);

	// 这里为什么要释放一次引用计数呢？
    cache_->Release(handle);
  }
  return s;
}

// Evict()方法用于擦除缓存中file_number对应的记录。
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
