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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()),
      schema_(&exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->schema_),
      iter_(table_heap_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
    // 初始化这个执行器，将iter指向第一个tuple
    iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}
/**
 * 
 */
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
    if (iter_ == table_heap_->End()) {
        return false;
    }
    *tuple = *iter_;
    *rid = tuple->GetRid();
    LockManager *lock_manager = exec_ctx_->GetLockManager();
    Transaction *txn  = exec_ctx_->GetTransaction();

    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        // 我们需要上读锁
        if (!lock_manager->LockShared(txn,*rid)) {
            // 如果上锁失败
            throw TransactionAbortException(txn->GetTransactionId(),AbortReason::DEADLOCK);
        }
    }
    // 构建返回的tuple
    std::vector<Value> values;
    for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
        // 1. 先找到要构建的第i个column，比如说是name字段，2. 通过这个expression去tuple中找对应的name字段并返回value
        // 这个expression是ColumnValueExpression
        Value val = plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(tuple,schema_);
        values.push_back(val);
    }
    *tuple = Tuple(values,plan_->OutputSchema());
    
    // 如果我们的隔离级别是读已提交，那么我们就可以在读完之后就解锁,并且不更改事务状态
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        if (!lock_manager->Unlock(txn, *rid)) {
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
        }
    }
    iter_++;
    // 遍历表时，其中的每一个tuple都要经过plan_->GetPredicate()->Evaluate()判断是否满足条件
    // 如果不满足就继续往下找，直到找到一个满足的，返回true，并在tuple中储存结果
    const AbstractExpression *predict = plan_->GetPredicate();
    if (predict != nullptr && !predict->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
        return Next(tuple, rid);
    }
    return true;
}

}  // namespace bustub
