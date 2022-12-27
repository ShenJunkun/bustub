//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
  this->plan_ = plan;
  
}

void DeleteExecutor::Init() {
  auto tableOid = this->plan_->TableOid();
  table_info_ = this->GetExecutorContext()->GetCatalog()->GetTable(tableOid);
  child_executor_.get()->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 

  auto* txn_mgr = this->GetExecutorContext()->GetTransactionManager();
  auto* txn = this->GetExecutorContext()->GetTransaction();
  auto* lock_mgr = this->GetExecutorContext()->GetLockManager();

  bool res = child_executor_.get()->Next(tuple, rid);
  if (!res) {
    return false;
  }

  
  if (txn->IsSharedLocked(*rid)) {
    if (!lock_mgr->LockUpgrade(txn, *rid)) {
      txn_mgr->Abort(txn);
    }
  } else if (!txn->IsExclusiveLocked(*rid) && !lock_mgr->LockExclusive(txn, *rid)) {
    txn_mgr->Abort(txn);
  }

  bool deleteRes = table_info_->table_.get()->MarkDelete(*rid, exec_ctx_->GetTransaction());
  if (!deleteRes) {
    LOG_WARN("delete executor删除失败");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    lock_mgr->Unlock(txn, *rid);
  }  

  //delete indexes
  std::vector<IndexInfo *> indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  
  for (auto index : indexes) {
    Tuple index_tuple = tuple->KeyFromTuple(*(plan_->GetChildPlan()->OutputSchema()) , index->key_schema_, index->index_.get()->GetKeyAttrs());
    index->index_.get()->DeleteEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
    txn->GetIndexWriteSet()->emplace_back(*rid, plan_->TableOid(), WType::DELETE, *tuple, 
          index->index_oid_, this->GetExecutorContext()->GetCatalog());
  }

  return true; 
}

}  // namespace bustub
