// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

// struct FileMetaData结构体存放着一个sstable文件的元信息
struct FileMetaData {
  int refs;  // sstable文件的引用计数

  // allowed_seeks存放的是该sstable文件在进行compact之前最大允许访问的次数
  int allowed_seeks;          // Seeks allowed until compaction

  // sstable文件对应的file number
  uint64_t number;

  // sstable文件的大小
  uint64_t file_size;         // File size in bytes

  // sstable文件中存放的最小的key
  InternalKey smallest;       // Smallest internal key served by table

  // sstable文件中存放的最大的key
  InternalKey largest;        // Largest internal key served by table

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

// VersionEdit类保存的是一个版本变动的信息，在某个基准版本上面，应用一个或者多个
// VersionEdit就可以得到新的版本。VersionEdit中存放了基于上一个版本增加的文件信息，
// 删除的文件信息。
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }

  //设置下一个file number
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file

  // AddFile()方法用于往VersionEdit类实例中添加版本变动需要增加的一个sstable文件，
  // 其中level是sstable文件所在的层数，file是sstable文件的file number，file_size是
  // sstable文件的大小，smallest和largest分别是sstable文件中存放的最小和最大的key信息。
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  // DeleteFile()方法用于往VersionEdit类实例中添加一个版本变动需要被删除的sstable文件，
  // level是sstable文件所在的层级数，而file则是sstable文件的file number。
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  // 比较器名字
  std::string comparator_;

  // log文件对应的FileNumber
  uint64_t log_number_;

  // 上一个log文件对应的FileNumber
  uint64_t prev_log_number_;

  // 下一个可用的FileNumber
  uint64_t next_file_number_;

  // 用过的最后一个SequenceNumber
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  // 为了尽量均匀compact每个层级，所以会将这一次compact的end-key作为下一次
  // compact的start-key，compact_pointers_就保存了每一个level下一次compact的
  // start-key，compact_pointers_中的每个元素类型为std::pair<int, InternalKey>，
  // pair中的first就是level信息，而second就是对应的key信息。
  std::vector< std::pair<int, InternalKey> > compact_pointers_;

  // deleted_files_是一个集合set，存放了版本变动相对于基线版本来说要删除的sstable文件信息，
  // 其中std::pair<int, uint64_t>的first部分是sstable所在的层级数level，而second部分则是
  // sstable文件的对应的file number。这里为什么要使用set呢？set里面的元素具有唯一性，这里
  // 使用set可以避免重复删除同一个文件。
  DeletedFileSet deleted_files_;

  // 版本变动相对于基线版本来说要增加的sstable文件信息，其中std::pair<int, FileMetaData>
  // 的first部分是sstable所在的层级数level，而second部分则是sstable文件的
  // 元信息。
  std::vector< std::pair<int, FileMetaData> > new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
