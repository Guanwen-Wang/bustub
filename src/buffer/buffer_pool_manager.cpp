//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  cur_size_ = 0;
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

// For FetchPageImpl,you should return NULL if no page is available in the free list and all other pages are currently pinned.
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // case 1: if this page_id exist in the page table
  if (page_table_.count(page_id)) {
    int frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  // case 2: if this page_id does not exist in the page table, find a replacement page from free list or replacer
  frame_id_t replace_frame_id = -1;
  if (!free_list_.empty()) {
    replace_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&replace_frame_id);
  }

  // if all pages pinned now, cannot find a replacement
  if (replace_frame_id == -1) { return nullptr; }

  page_id_t replace_page_id = pages_[replace_frame_id].page_id_;

  // check the replacement page dirty status
  if (pages_[replace_frame_id].IsDirty()) {
    FlushPageImpl(replace_page_id);
  }

  // delete the replacement page and insert the required page
  auto it = page_table_.find(replace_page_id);
  page_table_.erase(it);
  page_table_[page_id] = replace_frame_id;

  // fetch data from the disk
  update_page_metadata(replace_frame_id, page_id);
  disk_manager_->ReadPage(page_id, pages_[replace_frame_id].data_);

  return &pages_[replace_frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  if (page_id == INVALID_PAGE_ID || !page_table_.count(page_id)) return false;

  // decrement the pin_count, and call the replacer unpin if pin_count == 0
  if (--pages_[page_table_[page_id]].pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
    free_list_.push_back(page_table_[page_id]);
  }

  // check the dirty status of this page
  pages_[page_table_[page_id]].is_dirty_ |= is_dirty;

  // if this page is dirty, then write back to disk
  if (pages_[page_table_[page_id]].is_dirty_) {
    FlushPageImpl(page_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (!page_table_.count(page_id)) { return false; }

  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // corner case: if buffer pool is full and all pages are pinned
  int frame_id_tmp = 0;
  if (cur_size_ == pool_size_ && free_list_.empty() && !replacer_->Victim(&frame_id_tmp)) {
    return nullptr;
  }

  int new_page_id = disk_manager_->AllocatePage();
  *page_id = new_page_id;

  // case 1: if the current buffer pool is not full
  if (cur_size_ < pool_size_) {
    frame_id_tmp = free_list_.front();
    free_list_.pop_front();
    update_page_metadata(frame_id_tmp, new_page_id);
    cur_size_++;
  }
  // case 2: if the current buffer pool has been full
  else if (cur_size_ == pool_size_) {
    // first find the free list
    if (!free_list_.empty()) {
      frame_id_tmp = free_list_.front();
      free_list_.pop_front();
    } else { // if free list is empty, then find the replacer victim
      replacer_->Victim(&frame_id_tmp);
    }

    // find the page_id needs to be evicted
    page_id_t evict_page_id = pages_[frame_id_tmp].page_id_;
    auto it = page_table_.find(evict_page_id);
    page_table_.erase(it);
    update_page_metadata(frame_id_tmp, new_page_id);
  }

  return &pages_[frame_id_tmp];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  if (!page_table_.count(page_id)) { return true; }

  int frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ != 0) { return false; }

  auto it = page_table_.find(page_id);
  page_table_.erase(it);
  cur_size_--;
  free_list_.push_back(page_table_[page_id]);
  reset_page_metadata(frame_id);

  return true;
}


void BufferPoolManager::FlushAllPagesImpl() {
  for (auto & it : page_table_) {
    FlushPageImpl(it.first);
  }
}

void BufferPoolManager::update_page_metadata(frame_id_t frame_id, page_id_t page_id) {
  page_table_[page_id] = frame_id;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  replacer_->Pin(frame_id);
}

void BufferPoolManager::reset_page_metadata(frame_id_t frame_id) {
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
}


}  // namespace bustub
