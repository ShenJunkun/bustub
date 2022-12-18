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
#include <utility>
#include "common/logger.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// size_t BufferPoolManagerInstance::findPage(page_id_t page_id) {
//   for (size_t i = 0; i < pool_size_; i++) {
//     if (pages_->GetPageId() == page_id) {
//       return i; 
//     }
//   }
//   return -1;
// }

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  //TODO
  // std::lock_guard<std::mutex> lk(latch_);
  // size_t idx = findPage(page_id);
  // if (idx == -1)
  //   return false;
  frame_id_t idx;
  if (page_table_.find(page_id) != page_table_.end()) {
    idx = page_table_[page_id];
  } else {
    return false;
  }
  pages_[idx].WLatch();
  disk_manager_->WritePage(page_id, pages_[idx].GetData());
  pages_[idx].WUnlatch();
  return true;
  
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  // std::lock_guard<std::mutex> lk(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    pages_[i].WLatch();
    disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    pages_[i].WUnlatch();
  }
}

void BufferPoolManagerInstance::InitPage(frame_id_t frame_id, page_id_t page_id) {
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;

}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  //TODO
  std::lock_guard<std::mutex> lk(latch_);
  // bool flag = false;
  // for (int i = 0; i < pool_size_; i++) {
  //   if(pages_[i].GetPinCount() == 0) {
  //     flag = true;
  //   }
  // }

  // if (flag == false) {
  //   return nullptr;
  // }
  if(free_list_.empty() && replacer_->Size()== 0) {
    return nullptr;
  }

  auto pid = AllocatePage();
  *page_id = pid;
  frame_id_t frame_id = -1;

    //从free list中找可以使用的frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_[*page_id] = frame_id;
    InitPage(frame_id, *page_id);
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }

  //从LRU中找可以使用的frame
  if (replacer_->Size()) {
    bool res = replacer_->Victim(&frame_id);
    if (res) {
      auto evit_pid = pages_[frame_id].GetPageId();


      if (pages_[frame_id].IsDirty()) {
        bool res = FlushPgImp(pages_[frame_id].GetPageId());
        if (!res) {
          LOG_WARN("把脏页刷新到磁盘失败");
        }
      }
      page_table_.erase(evit_pid);
      page_table_[*page_id] = frame_id;
      InitPage(frame_id, *page_id);
      pages_[frame_id].pin_count_++;
      replacer_->Pin(frame_id);
      return &pages_[frame_id];
    } else {
      LOG_WARN("replacer中没有找到可以去除的frame");
    }
  }
  return nullptr;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  
  //TODO 上层应用请求page
  std::lock_guard<std::mutex> lk(latch_);
  if (page_table_.find(page_id)!=page_table_.end()) {
    frame_id_t idx = page_table_[page_id];
    pages_[idx].pin_count_++;
    return &pages_[idx];
  }

    //从free list中找可以使用的frame
  if (!free_list_.empty()) {
    auto frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = frame_id;
    InitPage(frame_id, page_id);
    pages_[frame_id].pin_count_++;
    //从磁盘中读取数据
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    return &pages_[frame_id];
  }

  //从LRU中找可以使用的frame
  if (replacer_->Size()) {
    frame_id_t frame_id = -1;
    bool res = replacer_->Victim(&frame_id);
    if (res) {
      if (pages_[frame_id].IsDirty()) {
        bool res = FlushPgImp(pages_[frame_id].GetPageId());
        if (!res) {
          LOG_WARN("把脏页刷新到磁盘失败");
        }
      }
      auto evit_pid = pages_[frame_id].GetPageId();

      page_table_.erase(evit_pid);

      page_table_[page_id] = frame_id;
      InitPage(frame_id, page_id);
      pages_[frame_id].pin_count_++;
      replacer_->Pin(frame_id);
      disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
      return &pages_[frame_id];
    } else {
      LOG_WARN("replacer中没有找到可以去除的frame");
    }
  }
  LOG_WARN("没有找到可以使用的frame");
  return nullptr;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  //TODO
  std::lock_guard<std::mutex> lk(latch_);
  DeallocatePage(page_id);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  } else {
    frame_id_t idx = page_table_[page_id];
    if (pages_[idx].GetPinCount() != 0) {
      return false;
    } else {
      if (pages_[idx].IsDirty()) {
        bool res = FlushPgImp(pages_[idx].GetPageId());
        if (!res) {
          LOG_WARN("把脏页刷新到磁盘失败");
        }
      }
      page_table_.erase(page_id);
      InitPage(idx, INVALID_PAGE_ID);
      free_list_.push_back(idx);
    }
  }
  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  //TODO  
  std::lock_guard<std::mutex> lk(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t idx = page_table_[page_id];
    pages_[idx].is_dirty_ = is_dirty;
    pages_[idx].pin_count_--;
    if (pages_[idx].pin_count_ <= 0) {
      replacer_->Unpin(idx);
    }
    return true;
  }
  return false; 
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
