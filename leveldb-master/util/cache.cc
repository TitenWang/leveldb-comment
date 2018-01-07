// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
// 结构体struct LRUHandle的实例是缓存中的基本元素，维护了以下几类信息：
// 1. key和value信息，其中key的存储采用的是变长数据，key_data配合key_lenght使用。
//    value则是一个void *类型的指针，意味着可以存放任意类型数据，反正存放的是指向
//    具体数据的指针。
// 2. 在hash表中存放的最小粒度单元也是struct LRUHandle的对象，所以struct LRUHandle中
//    维护了hash值。除此之外，为了解决hash冲突，struct LRUHandle还维护了一个LRUHandle*
//    类型的指针next_hash，用于串联起具有相同hash值的struct LRUHandle对象。
// 3. 在LRUCache中，是使用双向链表来实现lru思想的，而struct LRUHandle类型的对象也是
//    LRUCache中的最小粒度单元，因此struct LRUHandle中还需要维护next和prev指针。
struct LRUHandle {
  void* value; // value对应的地址
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash; // 用于串联起hash表槽中具有相同hash值的struct LRUHandle类型的对象
  LRUHandle* next;  // 和prev一起构造一个双向链表节点，用于表达lru思想
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?  // charge是当前元素的内存占用
  size_t key_length;  // key的长度，结合key_data成员表示变长的key信息
  bool in_cache;      // Whether entry is in the cache.
  uint32_t refs;      // References, including cache reference, if present.
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key

  // key()用于将key信息封装成一个Slice类型的对象
  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// hash表的实现。其实现的思想如下：
// 1. 内部维护着一个LRUHandle**类型的成员list_，这个成员是一个存放了LRUHandle*类型元素
//    的一维数组。其实list_成员就是hash表的槽位数组(slot)，槽位数组的长度由另外一个私有
//    成员length_存储。
// 2. 在执行Insert操作的时候，先利用key算出来的hash值求出key将要落在的槽位，然后遍历该
//    槽位中由LRUHandle *对象串联起来的单项链表，如果找到了一个hash值和key值都相同的
//    LRUHandle *对象，则返回该对象的地址，然后会用本次待插入的元素替换该元素，
//    完成插入操作；否则定位到该链表最后一个元素，返回该元素的next_hash成员的地址，此时
//    本次插入的对象会存放在这个成员中。
//  需要注意的一点是落到了同一个槽位中的元素，其hash值也不一定相同，因为槽位数组的长度
//  是有限的，举个例子假如槽位的长度为16，元素a的key算出来的hash值为15，元素b的key算出来的
//  hash值为31，那么这两个hash值对16求余之后会得到相同的值，也就会落到同一个槽位中，但是
//  他们的hash值和key值均不相同。
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {

    // FindPointer()遍历key所在槽位的单项链表，如果找到了一个hash值和key值都相同的元素，
    // 那么就返回该元素的地址，后面会将本次待插入元素会替换这个元素；否则会返回链表最后
    // 一个元素的next_hash成员的地址，因为本次将要插入的元素要存放在这个成员中。
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;

	// 如果在单向链表中没有找到hash值和key值都相同的元素，则old为NULL；如果找到了，那么先
	// 取出该元素的后面一个元素的地址，然后保存到本次待插入元素的next_hash成员中，然后将用
	// 本次待插入元素替换单项链表的原有的那个元素(该元素和本次待插入元素有相同的key值和hash值)
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;

	// 如果old == NULL，说明在槽位单向链表中没有和本次待插入元素的key和value值都相同的元素，
	// 那么本次插入会使得该槽位增加元素；如果在槽位中找到了一个具有相同key值信息的元素，那么
	// 从上面的处理方式我们可以看到采用的是直接用新元素去替换已有的元素，并没有在槽位链表中
	// 增加一个节点。
    if (old == NULL) {
      ++elems_;

	  // 如果元素个数超过了槽位数组的长度，那么就对hash表的槽位进行扩容，然后对hash表中的
	  // 元素进行重分布。
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }

    return old;
  }

  // Remove()函数用于删除hash表中key对应的节点信息，其步骤如下：
  // 1. 首先是调用FindPointer()函数遍历key所在槽位的单项链表，如果找到了一个hash值和key值
  // 都相同的元素，那么就返回该元素的地址；否则会返回单项链表中最后一个元素的next_hash成员
  // 的地址值，该地址存放的是一个NULL值。
  // 2. 如果hash表中存在待删除元素，那么就用该元素在单向链表中的下一个元素存放到存放了待删除
  // 元素的地址处。
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;

	// 如果result不为NULL，说明hash表中存在要删除的元素，那么将该元素在单向链表中的下一个
	// 一个元素存放到存放了待删除元素的地址处，即用下一个元素替换了待删除元素，作用就是
	// 将待删除元素从链表中抹掉了。
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;  // 元素个数减一。
    }

    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;  // hash表的slot数组长度。
  uint32_t elems_; // hash表中总的元素个数。
  LRUHandle** list_;  // hash表的slot数组。

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {

    // 首先利用key的hash值定位到对应的槽位。然后遍历槽位单项链表
    // 尝试寻找具有相同key和hash值的元素，如果找到了，那么就返回
    // 该元素的地址。否则会返回链表最后一个元素的next_hash成员的地址。
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  // Resize()函数用于对hash表中的槽位进行扩容，扩容后的大小是不小于当前hash表中元素个数的
  // 4的最小整数倍。将槽位扩容之后，会对hash表中的元素进行重分布，重分布的结果就是原来在同一个
  // 槽位中的元素，经过重分布之后可能就会分散到不同的槽位中。例如原来hash表的槽位是16个，
  // 当某次插入元素之后hash表中的的元素个数为17个，这个时候就会对hash表进行扩容重分布，
  // 假设元素a的hash值为15，而元素b的hash值为31，这两个元素原来在同一个槽位中，但是扩容
  // 之后槽位数为32个，那么元素a就会落在编号为14的槽位中，而元素b则会落在编号为31的槽位中，
  // 这样就减少了hash冲突的情况。
  void Resize() {
    uint32_t new_length = 4;

	// 扩容后的大小是不小于当前hash表中元素个数的4的最小整数倍
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;

	// 依次hash表的老槽位数组，将每个槽位中的元素都进行重分布。
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash; // 先获取老槽位链表中的下一个元素
        uint32_t hash = h->hash;

		// 获取该元素所在新槽位中的单项链表头部地址，如果该槽位中没有存放元素，
		// 那么ptr中存放的是一个NULL值，否则ptr中存放的就是新槽位中的单项链表
		// 头部元素的地址。那么对于槽位中没有存放元素的情况，h则会直接放进去
		// 作为链表中的第一个元素；如果槽位链表中已经有元素了，那么h会插入到
		// 单向链表的头部，而原有的头部元素则会变为第二个元素。
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;

        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();

  // 返回LRUCache总的内存使用量
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e);

  // Initialized before use.
  // 缓存中的内存占用上限，即容量，当内存占用超过这个值之后就会开始清理缓存中旧的内容。
  // 这个成员还有另外一个作用，就是如果这个成员值大于0 ，说明开启了缓存；如果等于0 ，
  // 说明缓存关闭。
  size_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;

  // 当前缓存中总的内存占用，在Insert的时候可以获取一个元素的内存占用信息
  // 元素的内存占用并不是LRUHandle对象的大小，而是从Insert()的时候上层调用者
  // 负责传入。
  size_t usage_;

  // leveldb中使用了二级的LRU算法，即通过两个双向链表来表达LRU。
  // 一级的LRU由下面的lru_成员来实现，二级LRU由下面的in_use_来实现
  // 当某个元素被插入到Cache中的时候，会先进入二级LRU，即in_use_
  // 链表中，当最老的元素被新元素从in_use_挤出来后，会进入一级LRU，
  // 即lru_链表。当lru_中最老的元素被挤出来之后，就会从缓存中删掉。
  // 二级LRU管理最近访问的元素，一级LRU管理次近访问的元素。两者之间
  // 通过引用计数来区分，当某个元素的引用计数为0时，就会从缓存中删掉。

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_;

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_;

  // 虽然元素可以用两个链表来管理，包括插入、查询和删除等。但是当上层调用者
  // 要查询的时候，如果从链表中找的话，时间复杂度会比从hash表中查找要高一些，
  // 所以LRUCache中例外用了hash表来存放元素，加快查询速度。
  HandleTable table_;
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked lists.

  // 初始化两个LRU双向循环链表
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  // 析构的时候，将lru_链表中的元素一一删除，
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

// 给元素增加一个引用计数，如果原来的引用计数为1，且in_cache标志为true，说明该元素原来
// 在一级LRU链表中，那么本次添加引用计数之后就会将其从一级LRU链表中删除，然后加入到
// 二级LRU链表中。
void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

// 给元素减少一个引用计数，如果减少引用计数之后，元素的引用计数为0，说明这个元素就没有
// 必要继续存放在缓存中了，这个时候可以调用e->deleter这个回调函数清理资源，然后释放这个
// 元素的内存；如果减少引用计数之后，元素的引用计数为1，说明这个元素需要从二级LRU链表中
// 退化到一级LRU链表中。
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) { // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {  // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

// 将元素e从其所在的双向链表中移除
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

// 将元素e添加到双向循环链表list的尾部。因为在LRUCache中，无论是一级LRU链表，
// 还是二级LRU链表，都是将最新访问的元素挂载到链表的末尾，而最久没有访问的元素
// 慢慢就会跑到链表的头部。
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

// Lookup()函数用于在LRUCache中查找key和hash都满足的元素，上面说到在LRUCache
// 中，为了加快查询速度，使用了hash表，而不是从两个LRU链表中去查询。
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  // 加锁，函数返回之后会自动释放锁。因为当函数返回的时候，局部变量l就会
  // 被析构，然后执行对应的虚构函数，在虚构函数中会调用mutex_的释放锁函数。
  MutexLock l(&mutex_);

  // 从hash表中查找相应的元素。
  LRUHandle* e = table_.Lookup(key, hash);
  // e != NULL，说明在LRUCache中找到了这个元素，那么对该元素增加一个引用计数
  // 因为此时这个元素就是最近被访问的函数，所以要放在二级LRU链表的尾部。
  if (e != NULL) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

// Release()减少该元素的引用计数
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

// Insert()函数实现了将一个元素加入到LRUCache的操作，包括以下内容：
// 1. 首先申请一块内存，然后将函数参数中的一些信息填充到内存中，比如key，value，hash等
// 2. 如果开启了缓存的话，那么会将这个元素加入到二级LRU链表，并存放到hash表中。
Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  memcpy(e->key_data, key.data(), key.size());

  // 如果capacity_大于0，说明开启缓存，则将这个元素加入到二级LRU链表中，
  // 并存放到hash表中。为什么要调用FinishErase()呢？我们知道调用hash表
  // 的Insert()函数的时候，如果在hash表中存放一个当待插入元素hash值和key
  // 值都一样的元素的话，那么会将已经在hash表中的那个元素从hash表中移除，
  // 然后将待插入元素替换进去，并返回被移除的元素，所以下面调用FinishErase()
  // 函数的作用是如果本次插入操作替换出了一个元素的话，那么需要将那个元素
  // 从缓存的里面擦除，这个元素已经没有意义了，因为有了同样key和hash值的新数据
  // 自然老的数据就没有意义了。
  // 如果capacity_等于0的话，说明不开启缓存，直接将元素返回给调用者。
  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = NULL;
  }

  // usage_大于capacity_，说明LRUCache中存放的内容所占用内存已经超过了上限值了
  // 这个时候需要将一级LRU链表lru_中的元素从LRUCache中移除。
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;   // 从lru_的头部开始处理老的元素
    assert(old->refs == 1);

	// 先将元素从hash表中移除，然后将其引用计数减一，并从一级LRU链表中移除。
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  // 返回本次插入的元素。
  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != NULL, finish removing *e from the cache; it has already been removed
// from the hash table.  Return whether e != NULL.  Requires mutex_ held.
// FinishErase()函数会将元素e从一级LRU链表中移除，然后调用Unref()函数减少引用计数，
// 然后调用e->deleter回调清理资源，并释放内存。
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != NULL) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;  //元素从LRUCache中移除，更新总的内存占用
    Unref(e);
  }
  return e != NULL;
}

// 从LRUCache中移除元素，包括LRU链表和hash表中的记录。
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

// Prune()函数用于精简LRUCache，其实就是对缓存中的内容做一个清理，具体做法
// 就是清理一级LRU链表中的全部元素。
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}


// SharedLRUCache类中会分16个LRUCache来存放元素，当元素插入到SharedLRUCache
// 中的时候，会用元素hash值高4位来决定这个元素会落在16个LRUCache中的哪个。
// 这从Shared()可以看出。
static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

// ShardedLRUCache()是Cache的实现类，其内部用了16个LRUCache来对元素做具体
// 的缓存、查询、删除等操作。
class ShardedLRUCache : public Cache {
 private:
  // shared_是一个LRUCache类的数组，是ShardedLRUCache内实际存放元素的地方。
  // 而每一个LRUCache类又是一个用lru思想来管理其内部元素的类。
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  // HashSize()函数用来计算入参对应的hash值。
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  // Shared()函数用来计算hash的高4位，即将32位的hash值向右移动28位。
  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    // 计算每个LRUCache类里面存放的元素内存占用的容量，capacity是SharedLRUCache
    // 类总的内存占用，然后均分到16个LRUCache中，即为per_shard。
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  // SharedLRUCache类中实现的元素插入接口Insert()主要步骤是：
  // 1. 先利用元素计算出来的hash值取出高4位，算出元素会落在哪个LRUCache槽中。
  // 2. 然后调用LRUCache类的Insert()函数实现将元素插入到LRUCache中。
  // charge是这个元素所占用的内存大小，deleter回调函数是函数从LRUCache中移除
  // 时调用的，一般用来给上层调用者清理资源。
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }

  // Lookup()也是通过调用LRUCache来实现函数的查询。
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }

  // Value()函数用于获取handle中的具体元素值。我们在Lookup函数中
  // 可以看到，Lookup函数返回的是一个类型为Handle*的值，在LRUCache
  // 实现中，会将其内部存储的元素在返回给上层调用者时将其强转为
  // Handle* 类型，所以在获取其内部值时需要将其类型转回到LRUHandle*
  // 然后从其中获取具体的元素值。
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }

  // NewId()用于返回一个新的可用id。获取id时候需要枷锁
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }

  // SharedLRUCache的缓存清理操作，清理的是其内部16个LRUCache的缓存。
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }

  // 返回SharedLRUCache的当前总的内存占用。
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

// NewLRUCache()函数用于创建一个Cache，其具体实现类为SharedLRUCache。
Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
