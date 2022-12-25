//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/column_value_expression.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
  : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  this->left_executor_.get()->Init();
  this->right_executor_.get()->Init();
  start_ = false;
}

void NestedLoopJoinExecutor::GetoutputTuple(Tuple* tuple, RID *rid) {
  auto output_schema = this->plan_->OutputSchema();
  std::vector<Value> values;
  for (auto column : output_schema->GetColumns()) {
    const ColumnValueExpression* columnValueExpression = dynamic_cast<const ColumnValueExpression*>(column.GetExpr());
    if (columnValueExpression != nullptr) {
      if (columnValueExpression->GetTupleIdx() == 0) {
        values.push_back(columnValueExpression->Evaluate(&left_tuple_, this->left_executor_.get()->GetOutputSchema()));
      } else if (columnValueExpression->GetTupleIdx() == 1) {
        values.push_back(columnValueExpression->Evaluate(&right_tuple_, this->right_executor_.get()->GetOutputSchema()));
      }
    } else {
      LOG_WARN("AbstractExpression转ColumnValueExpression失败");
    }
  }
  *tuple = Tuple(values, output_schema);
}


bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!start_) {
    start_ = true;
    bool res = this->left_executor_.get()->Next(&left_tuple_, &left_rid_);
    if (!res) {
      return false;
    }
  }

  //遍历inner table，判断是否相等
  while (true) {
    //对于左边一个tuple, 扫描右表
    while (this->right_executor_.get()->Next(&right_tuple_, &right_rid_)) {
      //判断左右tuple是否相等
      auto val = this->plan_->Predicate()->EvaluateJoin(&left_tuple_, this->left_executor_.get()->GetOutputSchema(), 
                  &right_tuple_, this->right_executor_.get()->GetOutputSchema()).GetAs<bool>();
      if (val) {
        GetoutputTuple(tuple, rid);
        return true;
      }
    }

    //右表扫描完毕，没找到匹配的,进行左表下一个tuple
    bool left_res = this->left_executor_.get()->Next(&left_tuple_, &left_rid_);
    // LOG_WARN("进行下一个左tuple");
    if (!left_res)
      return false;
    this->right_executor_.get()->Init();
    
  }

}

}  // namespace bustub
