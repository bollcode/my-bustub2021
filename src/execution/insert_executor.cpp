//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      catalog_(exec_ctx->GetCatalog()),
      table_info_(catalog_->GetTable(plan->TableOid())) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  } else {
    iter_ = plan_->RawValues().begin(); // 获取指向第一个tuple的迭代器
  }
}

/**
 * 基本流程： 首先判断是不是RawInsert，如果是直接插入到page中，如果不是就继续往下找，找到最后一个
 * 插入到page中，是通过table_heap_->InsertTuple()方法插入的，他会逐个遍历这个表page，有空就插入，如果没有空了就新建一个page插入
 * 然后插入完成之后，要更新索引
 */
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<Tuple> tuples;

  if (!plan_->IsRawInsert()) {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
  } else {
    if (iter_ == plan_->RawValues().end()) {
      return false;
    }
    *tuple = Tuple(*iter_, &table_info_->schema_);
    iter_++;
  }
  // 将元组插入到某个page中，只有插入之后才会获得rid，所以要在这后面加锁‘
  // 那么这里就会出现幻读现象
  if (!table_info_->table_.get()->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    LOG_DEBUG("INSERT FAIL");
    return false;
  }

  Transaction *txn = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  // 和update一样
  if (txn->IsSharedLocked(*rid)) {
    if (!lock_mgr->LockUpgrade(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  } else {
    if (!lock_mgr->LockExclusive(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  /**
   * 更新索引
   * for循环中的index是indexinfo
   * 因为我们插入一个元组，这个元组里也包含那个索引，所以要把这个插入到我们的索引中去
  */
  for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    index->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
        *rid,
        exec_ctx_->GetTransaction());
  }
  return Next(tuple, rid);
}

}  // namespace bustub
