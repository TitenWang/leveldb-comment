// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  WritableFile* file;  // sstable文件对应的WritableFile*对象
  uint64_t offset;  // file当前的大小，其实也是当前正在构建的block在文件中的偏移
  Status status;  // status用来就每一个步骤的结果
  BlockBuilder data_block;  // 用来构建table中的data block之用
  BlockBuilder index_block;  // 用来构建table中的index block之用
  std::string last_key;  // last_key保存着上一次调用Add()加入到table中的key值
  int64_t num_entries;  // 当前已经加入到table中的key-value个数。
  // closed是Finish()或者Abandon()被调用过的标志
  bool closed;          // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;  // 用来构建table中的filter block之用。

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.

  // pending_index_entry标志位用来标记需要在index block中记录一下刚刚构建完毕
  // 的data block位置和大小信息，因为data block已经构建完了，也知道这个block
  // 在文件中的偏移和大小了，所以就可以在index block中记录下这个data block的
  // 起始位置和大小信息了，而pending_handle中正是记录这个已经完成构建的data block的
  // 起始位置和大小信息。这个大小信息是不包含trailer部分的，只包含data block中的
  // 数据内容部分(key-value记录组+restart数组+restart数组长度)。
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  // 当一个data block数据内容构建完之后而在写type+crc信息之前，用来存放data block
  // 中数据内容被压缩之后的内容，如果type是kSnappyCompression的话。
  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

// Add()方法用于往sstable中添加一个key-value记录。
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 如果r->pending_index_entry为true的话，说明本次要加入到sstable中的key-value
  // 记录会被存放到一个新的data block中，也就是说之前构建的那个data block由于
  // 大小达到了阈值不能再往里面添加key-value信息了，并且这个data block中也已经
  // 写入了trailer，构建完毕了，所以在开启一个新的data block之前，需要先将这个
  // 已经构建完毕的data block在文件中的偏移以及大小信息写入到对应的index block中。
  // 后续从sstable中读取data block的信息时才能有足够的信息完整内容读取。
  // 这个大小信息是不包含trailer部分的，只包含data block中的数据内容部分(key-value
  // 记录组+restart数组+restart数组长度)。
  if (r->pending_index_entry) {

    // 在WriteBlock()方法中构建完一个data block并写入到文件之后就会将r->data_block
    // 清空，为下一个data block的构建做好准备。
    assert(r->data_block.empty());

    // FindShortestSeparator会将r->last_key设置为一个在[r->last_key, key)范围内的
    // 最短字符串，这里要将r->last_key限制比下一个block中第一个key小就是为了让
    // 上层调用者传入一个待查询的key值时，能定位到其所属的那个data block。
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;

	// 将存放着已经构建完毕的data block的位置和大小信息的pending_handle中的
	// 内容编码到字符串中。然后将刚刚计算得到的last_key和存放着编码后的data block的
	// 位置和大小信息的Slice对象作为一个key-value记录加入到index block中。
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;

	// ------------------------------------重要----------------------------------
	// 之所以在往index block中插入一个记录的时候，需要让它的key值比其value中保存的
	// 位置和大小信息对应的那个data block中最大的key值大或者相等，是为了当上层调用
	// 者传入一个待查询的key之后肯定能找到key所属的那个data block，因为上层调用者
	// 传入的这个待查询的key肯定小于等于所属data block中的最大key值，而如果index block中
	// 对应这个data block的记录的key值大于或者等于其value中保存的位置和大小信息对应
	// data block中的最大key值，那么当上层调用者传入一个待查询key之后，index block的迭代器
	// 通过Seek()接口会查询到大于或者等于传入的key值的最小key值对应的记录，这个记录就保存
	// 着待查询key所属的data block的位置和大小信息，然后进一步找到data block，再调用data block
	// 迭代器的Seek()接口就能找到key对应的value信息。
  }

  // 如果r->filter_block_不为NULL的话，那么就将key值添加到filter block中
  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  // 将本次需要添加到sstable中的key-value记录添加到data block中，这个data block
  // 可能是一个全新的block(这个key-value记录是其中的第一条记录)，也有可能是一个
  // 还没构建完毕，但已经有内容的data block。需要累加r->num_entries，表示往sstable
  // 中添加了一条key-value信息，并将r->last_key设置为本次加入的key值，因为相对于
  // 下一次调用Add()添加的key-value记录来说本次加入的key值就是上一次加入的key值了。
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  // 将本次添加的key-value记录写入到data block之后，需要判断当前正在构建的这个data
  // block的内存占用是否已经达到了阈值，如果达到了的话，那么就要将data block中的内容
  // 写入到sstable文件中，然后将r->pending_index_entry标志置位true，表示一个data block
  // 构建完毕，需要往index block中写入这个已经构建完毕的data block的位置和大小信息。
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

// Flush()方法在一个block的内存占用达到了选项设置的阈值之后就会被调用，在这个函数中
// 会调用WriteBlock()来完善block的构建并写入文件，为下一个block的构建做好准备。
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);

  // 如果ok()返回true的话，说明Block（数据部分、type和crc）已经成功写入到文件中，
  // 我们知道在index block中会记录每一个写入到文件中的block在文件中的偏移和大小
  // 信息，因为一个Block已经成功写入到了文件，所以在在开始构建下一个block之前
  // 需要现在index block中记录下这个已经构建完毕并写入到文件的block的信息，具体添加
  // 是在Add()函数中判断了r->pending_index_entry为true时会写入，所以在这里我们需要
  // 将这个标志位置位，然后当调用Add()往Table中添加一个key-value时候发现该标志位
  // 为true，说明key-value应该写入到一个新的block中，在写新的block之前，需要先将
  // 已经构建好的block的信息写入到index block中。
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }

  // 从FilterBlockBuilder类实现来看leveldb会为每2K的数据创建一个过滤器，需要注意的是这里的
  // 2K大小是指sstable文件每生成2K数据，所以2K不仅仅包括了key-value记录信息，还有trailer信息。
  // 在TableBuilder的实现中，也就是这里，每次构建完一个data block的时候，就会以当前sstable
  // 文件的大小来调用r->filter_block的StartBlock()方法来触发过滤器的创建。但是调用StartBlock()
  // 方法不一定会引发过滤器的创建，因为在FilterBlockBuilder类实现中还会判断目前sstable文件的
  // 大小是否达到了再创建一个过滤器的阈值，如果在上一个过滤器创建之后sstable文件内容又增加了
  // 2K，那么就会创建一个新的过滤器。
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);
  }
}

// 在调用WriteBlock()方法时上层调用者已经将block中的key-value记录数组已经构建
// 好了，所以接下来在这个函数中需要在block中的key-value数组后面追加重启点数组
// 以及重启点数组的个数，从而生成一个完整的block数据部分，然后根据选项选择是否
// 对数据部分进行压缩，最后调用WriteRawBlock（）写入文件。handle参数是一个出参，
// 最后WriteBlock()返回的时候会保存着这个block在sstable文件的偏移以及大小信息。
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;

  // 当一个Block的内存占用达到阈值的时候就会调用Finish()方法在最后面一个key-value记录
  // 后面追加重启点数组以及重启点数组的个数。从而生成一个完整的block数据部分，返回的
  // Slice对象就保存了这个block的内存和大小信息。
  Slice raw = block->Finish();

  Slice block_contents;

  // 下面主要是根据options中的选项判断是否需要对block的内容进行压缩，如果不需要的话，
  // 那么就直接将原始内容写入到文件中，否则进行snappy压缩，然后在写入文件。
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }

  // 在leveldb中，每一个block除了有具体的存放的数据内容之外，在往sstable中写入
  // 内容的时候，还会在block的数据内容后面写入数据存放类型（是否压缩）以及对
  // 对block数据内容生成的校验和。然后将这个block中数据内容部分在文件中的偏移
  // 以及数据内容的大小通过handle传递给上层调用者。
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();

  // 将BlockBuilder类实例中构建好的block数据块写入文件之后，这个BlockBuilder类实例
  // 就要用来构建后面的block对象了，所以这里将其内容清空。
  block->Reset();
}

// 在leveldb中，每一个block除了有具体的存放的数据内容之外，在往sstable中写入
// 内容的时候，还会在block的数据内容后面写入数据存放类型（是否压缩）以及对
// 对block数据内容生成的校验和。然后将这个block中数据内容部分在文件中的偏移
// 以及数据内容的大小通过handle传递给上层调用者。
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;

  // 将这个即将构建完毕的block的数据内容部分在文件中的偏移以及大小设置到handle，
  // 然后传递给上层调用者。这里为什么说r->offset是这个block的在文件中的偏移呢？
  // 因为r->offset在Table类实例的构建过程中，每次往其中写入一个完整的Block（包括
  // 数据内容部分，type和crc）之后，才会去更新r->offset（调用Finish写入footer时候
  // 更新那次是sstable已经构建完毕了，不算）。那么说明r->offset保存着下一个将要构建
  // 的Block的起始位置在文件中的偏移。
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());

  // 将文件内容追加到文件中。成功写入内容之后再往后面追加trailer部分，即type+crc。
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));

	// trailer部分写入到文件之后，这个Block就算完整的构建并写入完毕了。所以要更新文件
	// 的偏移，这样就可以记录下下一个block在文件中的偏移了。
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

// status()返回rep_->status，即构建table的状态信息。
Status TableBuilder::status() const {
  return rep_->status;
}

// Finish()在一个sstable文件的数据部分(即所有的data block，meta block)构建完毕之后
// 会被调用，其实保存着所有data block在sstable文件中的位置和对应的大小信息的index
// block也已经构建好了，这个时候主要是往sstable中写入filter block、metaindex_block
// index_block和footer等内容，用以构建完整的sstable文件。
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 往sstable中写入filter block。filter block中的内容以不压缩的方式存储，然后将
  // 写入到sstable文件的filter block在文件中的位置(即偏移)和大小信息通过filter_block_handle
  // 返回到上层。
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // 目前sstable中的meta block会用来存放filter信息。
  if (ok()) {
  	// 创建一个meta index block
    BlockBuilder meta_index_block(&r->options);
	// 如果r->filter_block不为零，说明创建过filter block。
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      // 利用过滤器的名字组装key值
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());

	  // 将filter block在sstable文件中的位置和大小信息编码到字符串中handle_encoding
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);

	  // 然后往meta index block中添加一条存放了filter block位置和大小信息的记录，
	  // 记录的key就是利用过滤器名字组装得到的，而value就是存放着filter block的位置
	  // 和大小信息的字符串
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    // 将meta_index_block内容写入到sstable文件中，并返回meta_index_block在sstable文件
    // 中的位置和大小信息。
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
  	// 这个地方的r->pending_index_entry为true，说明最后一个data block完整构建了，
  	// 所以需要在index block中写入这个block的大小和位置信息。然后再将index block
  	// 写入到文件中。
    if (r->pending_index_entry) {

	  // FindShortestSeparator会将r->last_key设置为一个大于等于r->last_key原有值的
      // 最短字符串
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;

	  // ------------------------------------重要----------------------------------
	  // 之所以在往index block中插入一个记录的时候，需要让它的key值比其value中保存的
	  // 位置和大小信息对应的那个data block中最大的key值大或者相等，是为了当上层调用
	  // 者传入一个待查询的key之后肯定能找到key所属的那个data block，因为上层调用者
	  // 传入的这个待查询的key肯定小于等于所属data block中的最大key值，而如果index block中
	  // 对应这个data block的记录的key值大于或者等于其value中保存的位置和大小信息对应
	  // data block中的最大key值，那么当上层调用者传入一个待查询key之后，index block的迭代器
	  // 通过Seek()接口会查询到大于或者等于传入的key值的最小key值对应的记录，这个记录就保存
	  // 着待查询key所属的data block的位置和大小信息，然后进一步找到data block，再调用data block
	  // 迭代器的Seek()接口就能找到key对应的value信息。
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // footer部分存放了metaindex_block和index_block在文件中的位置和大小信息，
  // footer的作用主要是在遍历sstable文件寻找key对应的value信息时，用于找到
  // index_block和metaindex_block，然后在index_block中再找到key所在的data block，
  // 进一步在data block中找到key-value记录。
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
	  // 更新r->offset，最终这个r->offset就表示了sstable文件的大小。
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

// 将r->closed标志位置位，表示table构建已经完毕或者终止了。
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

// 当前已经加入到table中的key-value个数。
uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

// 返回当前文件的大小，rep_->offset其实也是下一个正在构建的block在文件中的偏移。
uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
