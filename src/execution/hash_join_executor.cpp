//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
  : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_child)),
    right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_.get()->Init();
  right_child_.get()->Init();
  //build hash table
  Tuple left_tuple;
  RID left_rid;
  
  while(left_child_.get()->Next(&left_tuple, &left_rid)) {
    const ColumnValueExpression* columnValueExpression = dynamic_cast<const ColumnValueExpression*>(plan_->LeftJoinKeyExpression());
    Value value = columnValueExpression->Evaluate(&left_tuple, this->left_child_.get()->GetOutputSchema());
    HashjoinKey hashjoinKey;
    hashjoinKey.group_bys_.push_back(value);
    if (ht_.count(hashjoinKey) == 0) {
      HashjoinValue hashjoinValue;
      hashjoinValue.tuples.push_back(left_tuple);
      ht_[hashjoinKey] = hashjoinValue;
    } else {
      ht_[hashjoinKey].tuples.push_back(left_tuple);
    }
  }
  find_already_ = false;
}

void HashJoinExecutor::GetoutputTuple(Tuple* tuple, RID *rid) {
  auto output_schema = this->plan_->OutputSchema();
  std::vector<Value> values;
  for (auto column : output_schema->GetColumns()) {
    const ColumnValueExpression* columnValueExpression = dynamic_cast<const ColumnValueExpression*>(column.GetExpr());
    if (columnValueExpression != nullptr) {
      if (columnValueExpression->GetTupleIdx() == 0) {
        values.push_back(columnValueExpression->Evaluate( &(*iter_), this->left_child_.get()->GetOutputSchema()));
      } else if (columnValueExpression->GetTupleIdx() == 1) {
        values.push_back(columnValueExpression->Evaluate(&right_tuple_, this->right_child_.get()->GetOutputSchema()));
      }
    } else {
      LOG_WARN("AbstractExpression转ColumnValueExpression失败");
    }
  }
  *tuple = Tuple(values, output_schema);
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (ht_.empty()) {
    LOG_WARN("HashJoin executor: left table为空");
    return false;
  }

  if (find_already_) {
    if(iter_ != end_) {
      GetoutputTuple(tuple, rid);
      iter_++;
      return true;
    } else {
      find_already_ = false;
    }
  }

  while (this->right_child_.get()->Next(&right_tuple_, &right_rid_)) {
    const ColumnValueExpression* columnValueExpression = dynamic_cast<const ColumnValueExpression*>(plan_->RightJoinKeyExpression());
    Value value = columnValueExpression->Evaluate(&right_tuple_, this->right_child_.get()->GetOutputSchema());
    HashjoinKey hashjoinKey;
    hashjoinKey.group_bys_.push_back(value);
    if (ht_.count(hashjoinKey) != 0) {
      find_already_ = true;
      iter_ = ht_[hashjoinKey].tuples.begin();
      end_ = ht_[hashjoinKey].tuples.end();
      GetoutputTuple(tuple, rid);
      iter_++;
      return true;
    }
  }
  return false; 
}

}  // namespace bustub
