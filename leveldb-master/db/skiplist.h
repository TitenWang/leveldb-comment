// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

// SkipList类是跳表的定义
template<typename Key, class Comparator>
class SkipList {
 private:

  // 跳表中的一个节点，这里只是前置声明，具体定义在后面。
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  // Iterator类定义了skip list的迭代器
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    // Valid()方法用于判断迭代器是否有效，即是否指向了一个可用的skiplist的节点。
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    // key()方法用于返回当前位置的迭代器中的key信息。
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    // Next()方法使迭代器指向当前节点的下一个节点。
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    // Prev()方法使迭代器指向当前节点的前一个节点。
    void Prev();

    // Advance to the first entry with a key >= target
    // Seek()方法使迭代器指向skiplist中第一个key大于target的节点。
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    // SeekToFirst()方法使迭代器指向skiplist中的第一个节点。
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    // SeekToLast()方法使迭代器指向skiplist中的最后一个节点。
    void SeekToLast();

   private:
    const SkipList* list_;  // 迭代器的迭代对象，即一个跳表。
    Node* node_;  // 存放迭代器指向的节点的指针。
    // Intentionally copyable
  };

 private:
  // 定义跳表的最大高度
  enum { kMaxHeight = 12 };

  // Immutable after construction
  // 跳表的内部元素比较器，用于对元素进行排序。
  Comparator const compare_;

  // 跳表内部的元素内存管理器，用于在跳表中插入元素时申请内存之用。
  Arena* const arena_;    // Arena used for allocations of nodes

  // 跳表头，用于管理跳表的各个level形成的单向链表。
  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  // 跳表当前的最大高度。
  port::AtomicPointer max_height_;   // Height of the entire list

  // GetMaxHeight()方法用于获取跳表当前的高度。
  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  // 用于生成跳表高度的随机数对象。
  Random rnd_;

  // NewNode()方法用于创建一个新的跳表节点。
  Node* NewNode(const Key& key, int height);

  // 产生一个随机高度。
  int RandomHeight();

  // Equal()方法用于比较两个key是否相等。
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  // KeyIsAfterNode()用于判断key是否比节点n的key更大，如果是，则返回true；否则返回false。
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  // FindGreaterOrEqual()方法用于找到在0层链表比key值大或者等于key值的节点，并返回key在每一个层级的前置节点。
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  // FindLessThan()方法用于找到在0层链表比key小的最大key值节点。0层是唯一包括了所有元素的层级。
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  // FindLast()方法用于找到0层链表的最后一个节点，也是整个skiplist的最后一个节点。
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
// 定义skiplist中的一个节点，也是skiplist中那个前置声明struct Node的定义部分。
template<typename Key, class Comparator>
struct SkipList<Key,Comparator>::Node {
  explicit Node(const Key& k) : key(k) { }

  // 节点中的key。其实这部分内容可能也包含了value，在memtable中就是这个样子的key+value。
  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  // Next()方法用于返回当前节点在第n层的下一个节点，n取值返回[0, height - 1]。
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }

  // SetNext()方法用于设置当前节点在第n层的下一个节点。
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  // next_是一个变长数组，长度为跳表能达到的最大高度。一般在申请一个Node的时候，也会
  // 为这部分动态数组申请内存。
  port::AtomicPointer next_[1];
};


// NewNode()方法用于申请并初始化一个节点。
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
  return new (mem) Node(key);
}

// skiplist内部迭代器的构造函数。
template<typename Key, class Comparator>
inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = NULL;
}


// Valid()方法用于判断迭代器是否有效。
template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != NULL;
}

// key()用于返回迭代器的key信息。
template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

// Next()使迭代器指向当前节点在0层链表的下一个节点。
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

// Next()使迭代器指向当前节点在0层链表的上一个节点。
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

// Seek()方法使迭代器指向0层链表中key值不小于target的第一个节点。
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL);
}

// SeekToFirst()方法是迭代器指向0层链表中的第一个节点。
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

// SeekToLast()方法是迭代器指向0层链表中的最后一个节点。
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

// KeyIsAfterNode()函数用于判断key是否大于Node节点n的key，如果大于，则说明key对应的节点
// 应该在节点n之后，这里的比较过程根据泛型参数Comparator指定。对于MemTable来说，其具体化
// skiplist的时候，使用的Comparator是struct KeyComparator，而struct KeyComparator又是对
// InternalKeyComparator类的封装，所以MemTable中最终使用的比较函数则是InternalKeyComparator
// 的实现，这个实现可以参考dbformat.cc。
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

// FindGreaterOrEqual()方法用于返回0层链表中key值大于或者等于参数key的第一个节点，并通过出参
// 返回key在skiplist中各个层级的前置节点。
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

// FindLessThan()方法用于返回0层链表中key值小于参数key的最大key值节点
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == NULL || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// FindLast()方法用于返回0层链表中的最后一个节点，也是整个skiplist的最后一个节点。
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// skiplist的构造函数。
template<typename Key, class Comparator>
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(reinterpret_cast<void*>(1)),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, NULL);
  }
}

// Insert()方法实现了元素key插入到skiplist的功能。其步骤如下：
// 1. 首先利用key值在skiplist中找出所有层级链表中的前置节点。
// 2. 生成一个随机高度，用于表示本次插入元素在skiplist将要有的层级数。如果层级数大于当前
//    skiplist的最大高度，那么将高于当前高度的key的前置节点设置为头结点head_，因为key值在
//     高于当前最高节点部分是没有前置节点的，然后更新skiplist的最大高度为生成的随机高度
// 3. 根据key值所需的高度申请并初始化一个节点，然后设置当前节点在skiplist中的每一层链表
//    的前置和后继节点，这样就完成了将一个节点插入到skiplist中。
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (NULL), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since NULL sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

// Contains()方法用于判断key值信息是否存在于skiplist中。
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const {

  // 先是找到在skiplist的0层链表中key大于等于参数key的节点 ，然后判断
  // 这个节点的key和参数key是否相等，如果相等，那么久说明在skiplist中，
  // 否则肯定不在。
  Node* x = FindGreaterOrEqual(key, NULL);
  if (x != NULL && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
