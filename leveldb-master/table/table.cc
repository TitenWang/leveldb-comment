// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;  // sstable文件对象
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  // metaindex_handle保存着metaindex block在文件中的位置和大小信息。
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer

  // index_block指向了存放了从sstable文件读出来的index block的内存。
  Block* index_block;
};

// Open()函数用于从sstable 文件中读取大小为size的内容，并用一个Table实例来管理
// 这些信息，说白了就是将sstable文件中的内容从磁盘读入到内存中。
Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = NULL;

  // 如果size小于footer对象大小，说明size大小肯定比一个完整的sstable文件小，
  // 所以返回失败。
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  // 首先从文件中读取footer对象，因为这个对象存放了metaindex block和index block
  // 在sstable文件中的位置和大小信息，而index block中又存放了所有data block在
  // sstable文件中的位置和大小信息，所以这里读取footer对象内容最终是为找到block
  // 而服务的。
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  // 从文件读取到的流中反序列化出footer对象。
  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block

  // 程序执行到这里说明已经成功从文件中读取到了footer对象，并从中获取到了index block
  // 的位置和大小信息了。那么就可以根据位置和大小信息从sstable文件中将index block的
  // 内容读取出来，存放到index_block_contents中
  BlockContents index_block_contents;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);
  }

  // 如果s.ok()为true的话，说明已经成功读取到了footer和index block的信息，
  // 那么就可以开始构建一个Table类实例了。
  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    // 根据从文件中读出来的index block对应的流创建一个index block实例。
    // 后面可以根据这个实例，创建一个index block的迭代器，用于迭代获取
    // data block对象。
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// BlockReader()方法用于创建一个index_value存放的位置和大小信息对应的data block的迭代器。
// 这个迭代器可以用于遍历data block内的key-value记录。index_value参数存放着目标data block
// 的位置和大小信息。
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  // 从index_value中反序列化出data block对应的位置(偏移)和大小信息。
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {

	  // 如果block缓存开启的话，则尝试先从缓存中读取index_value对应的block信息
	  // 缓存中存放了block信息，则对应的key值则由cache_id和block在对应sstable
	  // 中的偏移信息组成。如果缓存中没有读取到对应的block信息，则从文件中读取
	  // block信息，然后将block信息存入到缓存中，下次如果索引这个block的话，就
	  // 可以直接从缓存中读取了。
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {
      // 程序执行到这里说明没有开启block的缓存，那么直接从sstable中读取指定的
      // block，handle中存放了目标data block在文件中的位置和大小信息，而读取的
      // 内容则存放在了contents中，contents中保存着存放了block内容的内存指针以及
      // 内存块的大小。
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  // 经过上面的处理流程，如果block不为NULL的话，说明已经成功读取到了block对象，
  // 那么就可以根据这个block对象创建一个迭代器，并注册这个迭代器对应的清理资源
  // 回调函数。
  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
	  // 如果没有开启缓存的话，那么当迭代器释放的时候，调用DeleteBlock函数直接释放block内存。
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      // 如果开启了缓存的话，那么当迭代器释放的时候，调用ReleaseBlock函数，先将缓存中的block
      // 对应的记录移除，然后再释放block内存。
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

// 创建一个Table类实例的迭代器，其实Table类实例的迭代器是一个二级迭代器，用于找到
// 待查询key对应的value信息。为什么需要二级迭代器呢？我们知道一个table中可能有多个
// data block，一个data block中又有多个key-value记录，所以我们要找到一个key对应的
// value信息，首先需要找到key所属的data block，找到所属data block之后还需要在这个
// data block中找到这个key对应的记录。而data block的位置和大小信息是存放在index
// block中的，所以外层迭代器是index block的迭代器，用于遍历data block，内层迭代器
// 是一个data block迭代器，用于遍历data block内的key-value记录。
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

// InternalGet()方法用于获取待查询key值k所对应的在index block中的记录，如果找到了
// 这个记录的话，那么利用这个记录的value所保存的位置和大小信息对应的data block，
// 然后再创建一个data block的迭代器，并让迭代器指向key值大于等于k的最小key值
// 对应的记录，最后调用saver回调函数来处理在data block中找到的这个记录。
Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;

  // 创建一个index block的迭代器，然后找到待查询key值k对应的记录。
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      // 创建一个data block的迭代器，并让迭代器指向key值大于等于待查询key值k的最小key
      // 值对应的记录，如果迭代器有效的话，就调用saver来处理这个记录。
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*saver)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}


// ApproximateOffsetOf()方法返回key所属的data block在sstable文件中的偏移。
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {

  // 创建一个index block的迭代器，后面会用传入的待查询key去索引index block，
  // 迭代器负责去索引。
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);

  // 调用index block迭代器的Seek()接口，这个接口会使迭代器指向index block中
  // key值大于等于待查询key值的最小key值对应的记录。如果迭代器有效的话，那么
  // 就可以从迭代器的value中获取到记录信息，这个记录信息就保存了key值所属data
  // block在sstable文件中的位置和大小信息。
  index_iter->Seek(key);
  uint64_t result;

  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
