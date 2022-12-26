//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

#include "execution/expressions/column_value_expression.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
  : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_.get()->Init();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple child_tuple;
  RID child_rid;

  while(child_executor_.get()->Next(&child_tuple, &child_rid)) {
    DistinctKey distinctKey;
    
    //处理key
    // for (auto column : child_executor_.get()->GetOutputSchema()->GetColumns()) {
    //   const ColumnValueExpression* cvExpr = dynamic_cast<const ColumnValueExpression*>(column.GetExpr());
    //   if (cvExpr != nullptr) {
    //     auto val = cvExpr->Evaluate(&child_tuple, child_executor_.get()->GetOutputSchema());
    //     distinctKey.group_bys_.push_back(val);
    //   } else {
    //     LOG_WARN("AbstractExpression转ColumnValueExpression失败");
    //   }
    // }

    for(size_t i = 0; i < child_executor_.get()->GetOutputSchema()->GetColumns().size(); i++) {
      distinctKey.group_bys_.push_back(child_tuple.GetValue(child_executor_.get()->GetOutputSchema(), i));
    }

    if (ht_.count(distinctKey) == 0) {
      ht_.insert(distinctKey);
      *tuple = child_tuple;
      return true;
    }
  }
  return false;

}

}  // namespace bustub
