//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // std::lock_guard<std::mutex> lk(latch_);
  //txn state为aborted，直接return
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UNLOCK_ON_SHRINKING);
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  
  txn->SetState(TransactionState::GROWING);
  
  std::unique_lock<std::mutex> lk(latch_);
  //将requestTask插入队列
  auto& lockRequestQueue = lock_table_[rid];

  lockRequestQueue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);

  txn->GetSharedLockSet().get()->emplace(rid);

  bool should_grand = true;
  bool has_kill = false;
  //遍历队列，满足would-wait algorithm
  for (auto& lockRequest : lockRequestQueue.request_queue_) {
    if (lockRequest.lock_mode_ == LockMode::EXCLUSIVE) {
      if (lockRequest.txn_id_ > txn->GetTransactionId()) {
        auto* req_txn = TransactionManager::GetTransaction(lockRequest.txn_id_);
        req_txn->SetState(TransactionState::ABORTED);
        has_kill = true;
      } else {
        should_grand = false;
      }
    }

    if (lockRequest.txn_id_ == txn->GetTransactionId()) {
      lockRequest.granted_ = should_grand;
      break;
    }

  }
  auto& cv = lockRequestQueue.cv_;

  if (has_kill) {
    cv.notify_all();  
  }

  while (!should_grand) {
    for (auto& lockRequest : lockRequestQueue.request_queue_) {
      if (lockRequest.lock_mode_ == LockMode::EXCLUSIVE && 
          TransactionManager::GetTransaction(lockRequest.txn_id_)->GetState() !=
          TransactionState::ABORTED) {
        break;
      }
      if (lockRequest.txn_id_ == txn->GetTransactionId()) {
        should_grand = true;
        lockRequest.granted_ = true;
        break;
      }
    }

    if (!should_grand) {
      cv.wait(lk);
    }

    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  txn->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> lk(latch_);


  auto& lockRequestQueue = lock_table_[rid];

  lockRequestQueue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);

  bool should_grant = true;
  bool has_kill = false;
  for (auto & lockRequest : lockRequestQueue.request_queue_) {

    if (lockRequest.txn_id_ == txn->GetTransactionId()) {
      lockRequest.granted_ = should_grant;
      break;
    }
    
    if (lockRequest.txn_id_ > txn->GetTransactionId()) {
      TransactionManager::GetTransaction(lockRequest.txn_id_)->SetState(TransactionState::ABORTED);
      has_kill = true;
    } else {
      should_grant = false;
    }

  }

  auto& cv = lockRequestQueue.cv_;
  if (has_kill) {
    cv.notify_all();
  }

  while (!should_grant) {
    for (auto & lockRequest : lockRequestQueue.request_queue_) {
      if (TransactionManager::GetTransaction(lockRequest.txn_id_)->GetState() != TransactionState::ABORTED) {
        break;
      }
      if (lockRequest.txn_id_ == txn->GetTransactionId()) {
        lockRequest.granted_ = true;
        should_grant = true;
        break;
      }
    }

    if (!should_grant) {
      cv.wait(lk);
    }

    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  txn->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> lk(latch_);

  auto& lockRequestQueue = lock_table_[rid];

  if (lockRequestQueue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  lockRequestQueue.upgrading_ = txn->GetTransactionId();

  bool should_grant = false;
  auto& cv = lockRequestQueue.cv_;

  while (!should_grant) {
    auto itor = lockRequestQueue.request_queue_.begin();
    auto target = itor;
    should_grant = true;
    bool has_kill = false;

    while (itor != lockRequestQueue.request_queue_.end() && itor->granted_) {
      if (itor->txn_id_ == txn->GetTransactionId()) {
        target = itor;
      } else if (itor->txn_id_ > txn->GetTransactionId()) {
        TransactionManager::GetTransaction(itor->txn_id_)->SetState(TransactionState::ABORTED);
        has_kill = true;
      } else {
        should_grant = false;
      }
      ++itor;
    }

    if (has_kill) {
      cv.notify_all();
    }

    if (!should_grant) {
      cv.wait(lk);
    } else {
      target->lock_mode_ = LockMode::EXCLUSIVE;
      lockRequestQueue.upgrading_ = INVALID_TXN_ID;
    }

    if(txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }

  std::unique_lock<std::mutex> lk(latch_);
  auto & lockRequestQueue = lock_table_[rid];
  auto it = lockRequestQueue.request_queue_.begin();
  while(it->txn_id_ != txn->GetTransactionId()) {
    ++it;
  }
  lockRequestQueue.request_queue_.erase(it);
  lockRequestQueue.cv_.notify_all();
  
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
