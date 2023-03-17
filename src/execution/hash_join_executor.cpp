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

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

/**
 * 基本流程：
 *  
 */

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) { // 先将左表的tuple给hash一下并插入到哈希表中，key就是join的字段，value就是tuple
    jht_.Insert(MakeLeftHashJoinKey(&left_tuple), left_tuple); 
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!tmp_results_.empty()) {
    *tuple = tmp_results_.front();
    *rid = tuple->GetRid();
    tmp_results_.pop();
    return true;
  }

  Tuple right_tuple;
  RID right_rid;

  if (!right_child_->Next(&right_tuple, &right_rid)) { // 然后不断的获取右表的tuple
    return false;
  }

  if (jht_.Count(MakeRightHashJoinKey(&right_tuple)) == 0) { // 然后对指定的右表的tuple的列做hash，
                                                              // 然后看看在左表形成的hash表中有没有对应的key，如果有，说明满足join条件，
                                                              // 然后去构建新的tuple
    return Next(tuple, rid);
  }

  std::vector<Tuple> left_tuples = jht_.Get(MakeRightHashJoinKey(&right_tuple));
  for (const auto &left_tuple : left_tuples) {
    std::vector<Value> output;
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_child_->GetOutputSchema(), &right_tuple,
                                                   right_child_->GetOutputSchema()));
    }
    tmp_results_.push(Tuple(output, GetOutputSchema()));
  }

  return Next(tuple, rid);
}

}  // namespace bustub
