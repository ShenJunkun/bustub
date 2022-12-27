//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "include/storage/table/table_iterator.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), 
                    cursor_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
                    end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {
  this->plan_ = plan;
}

void SeqScanExecutor::Init() {
  auto tableOid = plan_->GetTableOid();
  auto catalog = this->GetExecutorContext()->GetCatalog();
  auto tableInfo = catalog->GetTable(tableOid);
  auto tableHeap = tableInfo->table_.get();
  auto txn = this->GetExecutorContext()->GetTransaction();
  cursor_ = tableHeap->Begin(txn);
  end_ = tableHeap->End();
  schema_ = &(tableInfo->schema_);
}

void SeqScanExecutor::getOutPutTuple(Tuple *tuple, RID *rid) {
  auto output_schema = this->plan_->OutputSchema();
  std::vector<Value> values;
  for (auto column : output_schema->GetColumns()) {
    values.push_back(column.GetExpr()->Evaluate(&(*cursor_), schema_));
  }
  *tuple = Tuple(values, output_schema);
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
//TODO 当predicate为null的时候
  if (cursor_ == end_) {
    return false;
  }
  auto* txn_mgr = this->GetExecutorContext()->GetTransactionManager();
  auto* txn = this->GetExecutorContext()->GetTransaction();
  auto* lock_mgr = this->GetExecutorContext()->GetLockManager();

  if (this->plan_->GetPredicate() == nullptr) {
    // *tuple = *cursor_;
    *rid = (*cursor_).GetRid();

    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED || 
        txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (!txn->IsSharedLocked(*rid) && !txn->IsExclusiveLocked(*rid) && !lock_mgr->LockShared(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    }

    getOutPutTuple(tuple, rid);

    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_mgr->Unlock(txn, *rid);
    }
    cursor_++;
    return true;
    
  } else {
    bool res = this->plan_->GetPredicate()->Evaluate(&(*cursor_), GetOutputSchema()).GetAs<bool>();
    
    while (!res)
    {
      cursor_++;
      if (cursor_ == end_) {
        break;
      }
      res = this->plan_->GetPredicate()->Evaluate(&(*cursor_), GetOutputSchema()).GetAs<bool>();
    }

    if (cursor_ != end_) {
      // *tuple = *cursor_;
      *rid = (*cursor_).GetRid();

      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED || 
          txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if (!txn->IsSharedLocked(*rid) && !txn->IsExclusiveLocked(*rid) && !lock_mgr->LockShared(txn, *rid)) {
          txn_mgr->Abort(txn);
        }
      }

      getOutPutTuple(tuple, rid);

      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        lock_mgr->Unlock(txn, *rid);
      }

      cursor_++;
      return true;
    } 
    
    return false; 
  }
}
    
}  // namespace bustub
