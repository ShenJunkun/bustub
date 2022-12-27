//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "concurrency/transaction_manager.h"
#include "common/logger.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),  child_executor_(std::move(child_executor)) {
  this->plan_ = plan;
}

void UpdateExecutor::Init() {
  auto talbeOid = this->plan_->TableOid();
  this->table_info_ = this->GetExecutorContext()->GetCatalog()->GetTable(talbeOid);
  child_executor_.get()->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  
  auto* txn_mgr = this->GetExecutorContext()->GetTransactionManager();
  auto* txn = this->GetExecutorContext()->GetTransaction();
  auto* lock_mgr = this->GetExecutorContext()->GetLockManager();

  bool res =child_executor_.get()->Next(tuple, rid);
  if (!res) {
    return false;
  }

  Tuple newTuple = this->GenerateUpdatedTuple(*tuple);
  

  if (txn->IsSharedLocked(*rid)) {
    if (!lock_mgr->LockUpgrade(txn, *rid)) {
      txn_mgr->Abort(txn);
    }
  } else if (!txn->IsExclusiveLocked(*rid) && !lock_mgr->LockExclusive(txn, *rid)) {
    txn_mgr->Abort(txn);
  }

  bool updateRes = table_info_->table_.get()->UpdateTuple(newTuple, *rid, exec_ctx_->GetTransaction());
  if (!updateRes) {
    LOG_WARN("update executor更新失败");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    lock_mgr->Unlock(txn, *rid);
  }

  //update indexes
  std::vector<IndexInfo *> indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto index : indexes) {

    Tuple older_index = tuple->KeyFromTuple(*(plan_->GetChildPlan()->OutputSchema()) , index->key_schema_, index->index_.get()->GetKeyAttrs());
    Tuple new_index = newTuple.KeyFromTuple(*(plan_->GetChildPlan()->OutputSchema()) , index->key_schema_, index->index_.get()->GetKeyAttrs());
    index->index_.get()->DeleteEntry(older_index, *rid, exec_ctx_->GetTransaction());
    index->index_.get()->InsertEntry(new_index, *rid, exec_ctx_->GetTransaction());

    txn->GetIndexWriteSet()->emplace_back(*rid, plan_->TableOid(), WType::DELETE, *tuple, 
          index->index_oid_, this->GetExecutorContext()->GetCatalog());
    txn->GetIndexWriteSet()->emplace_back(*rid, plan_->TableOid(), WType::INSERT, newTuple, 
          index->index_oid_, this->GetExecutorContext()->GetCatalog());

  }
  
  return true; 
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
