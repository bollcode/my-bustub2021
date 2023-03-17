//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                      LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(static_cast<page_id_t>(instance_index)),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool. pages就是我们的bufferpool
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  // 将所有的pages数组索引加入到空闲链表中
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].is_dirty_ = false;
    pages_[i].pin_count_ = 0;
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

/**
 *  这个函数用来将page_id对应的page写到磁盘上去，并将这个页面更改为非dirty，
 *  这个页面暂时还在pages数组,索引下标就是我们说的frame_id_t,我们一般使用lru来删除
 */
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t id = iter->second;
  Page *page = pages_ + id;
  page->is_dirty_ = false;
  disk_manager_->WritePage(page_id, page->GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.begin();
  // 遍历所有的内存中的页,然后写入到磁盘中
  while (iter !=page_table_.end()) {
    page_id_t pageid = iter->first;
    frame_id_t frameid = iter->second;
    Page *page = pages_ + frameid;
    disk_manager_->WritePage(pageid, page->GetData());
    iter++;
  }
}
/**
 *  这个函数是为一个页在bufferpool中分配一个位置，然后分配一个pageid，并将其pageid和frameid关系加入到pagetable中
 *  然后返回一个Page指针，指向了Pages数组中某一个page地址，就是在pages数组中申请了一个page空间
 */
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frameid = -1;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    // 说明空闲链表中有空闲的frame_id,也就是在bufferpool中有空位
    // 获取一个空闲的位置, 将page指向bufferpool的对应的page的位置
    frameid = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frameid];
  } else if (replacer_->Victim(&frameid)) {
    // 当空闲链表中找不到空闲的位置，说明bufferpool中位置被占满了，
    // 这个时候需要使用lru移除一个，调用Victim()移除一个，并返回这个移除的位置
    page = &pages_[frameid];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    // 从pagetable中移除pageid和frameid的对应关系
    page_table_.erase(page->GetPageId());
  }

  if (page !=nullptr) {
    // 说明上面在bufferpool中获取到了位置，现在应该将这个申请一个id
    *page_id = AllocatePage();
    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page->ResetMemory();
    page_table_[*page_id] = frameid;
    replacer_->Pin(frameid);
    return page;
  }
  return nullptr;
}
/**
 *  从bufferpool中获取一个页，如果这个页不在的话，就先在bufferpool中找一个位置（先freelist，在lru），找到之后，将之前的页
 *  判断是否为脏页来决定是否写入磁盘，然后从磁盘中读取对应的pageid的页到刚才找到的位置
 */
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frameid = iter->second;
    Page *page = &pages_[frameid];
    page->pin_count_++;
    replacer_->Pin(frameid);
    return page;
  }
  // 表示pageid对应的页不在内存中
  frame_id_t frameid = -1;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frameid = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frameid];
  } else if (replacer_->Victim(&frameid)) {  // 从lru中移除一个frameid
    page = &pages_[frameid];
    // 如果是脏页就刷进磁盘
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
    // 清除掉pagetable中的pageid和frameid的关系
    page_table_.erase(page->GetPageId());
  }

  // 判断是否找到了页的位置
  if (page != nullptr) {
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    // 在内存中找到了位置之后，我们开始将磁盘中对应的pageid给读进来，放入这个bufferpool中
    // printf("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh %d\n",page_id);
    disk_manager_->ReadPage(page_id, page->GetData());
    page_table_[page_id] = frameid;
    replacer_->Pin(frameid);
  }
  return page;
}
/**
 *  这个函数用来删除一个内存中的页
 */
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  DeallocatePage(page_id);
  auto iter = page_table_.find(page_id);

  if (iter == page_table_.end()) {
    return true;
  }
  frame_id_t frameid = iter->second;
  Page *page = &pages_[frameid];
  if (page->GetPinCount() > 0) {
    return false;
  }
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }
  replacer_->Pin(frameid);  // 表示正在被使用，从lru中剔除(从链表中剔除，从lru_map中剔除);
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
  free_list_.push_back(frameid);
  return true;
}
/**
 *  这个函数是将pageid对应page的程序正在使用数减一，如果减为0，那么表示没有程序使用这个页了，就将这个页加入到lru中
 */
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t frameid = iter->second;
  Page *page = pages_ + frameid;

  if (page->GetPinCount() <=0) {
    return false;
  }
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  page->pin_count_--;
  if (page->GetPinCount() <=0) {
    // 现在没有程序使用这个页了，把他加入到lru中
    replacer_->Unpin(frameid);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
