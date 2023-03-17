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



#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) { // hello
  // 首先判断这个事务是不是已经aborted了
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 查看事务的隔离级别,READ_UNCOMMITTED不能加任何锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  // 在GROWING阶段才可以加锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  // 是否这个事务已经获取了这个tuple的shared锁
  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  // 接下来继续上锁，并采取wound-wait算法进行上锁，老的抢占年轻的锁
  std::unique_lock<std::mutex> guard(latch_);  // 下面是对共享资源lock_table_更改，所以要上锁
  LockRequestQueue * lock_request_queue = &lock_table_[rid];
  LockRequest lockrequest(txn->GetTransactionId(), LockMode::SHARED);
  lock_request_queue->request_queue_.emplace_back(lockrequest);
  txn->GetSharedLockSet()->emplace(rid);

  // 上锁要讲究顺序，采取的算法是WOUND-WAIT算法，老的抢占年轻的，年轻的等待
  while (NeedWait(txn, lock_request_queue)) {
    // 如果需要等待，我们这里使用线程等待，使用wait
    lock_request_queue->cv_.wait(guard);
    // 这个等待会被唤醒，同时我们要判断我们这个事务是不是在等待过程中被别的事务给中止了
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    // 如果我们在等待过程中没有被中止，那我们再判断一下是不是还需要等待
  }
  // 如果不需要等待，说明我们可以授予锁了
  for (auto e : lock_request_queue->request_queue_) {
    if (e.txn_id_ == txn->GetTransactionId()) {
      e.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 首先判断这个事务是不是已经aborted了
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 在GROWING阶段才可以加锁
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  // 是否这个事务已经获取了这个tuple的exclusive锁
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // 接下来继续上锁，并采取wound-wait算法进行上锁，老的抢占年轻的锁
  std::unique_lock<std::mutex> guard(latch_);  // 下面是对共享资源lock_table_更改，所以要上锁
  LockRequestQueue * lock_request_queue = &lock_table_[rid];
  LockRequest lockrequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_request_queue->request_queue_.emplace_back(lockrequest);
  txn->GetExclusiveLockSet()->emplace(rid);
  // 上锁要讲究顺序，采取的算法是WOUND-WAIT算法，老的抢占年轻的，年轻的等待
  while (NeedWait(txn, lock_request_queue)) {
    // 如果需要等待，我们这里使用线程等待，使用wait
    lock_request_queue->cv_.wait(guard);
    // 这个等待会被唤醒，同时我们要判断我们这个事务是不是在等待过程中被别的事务给中止了
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    // 如果我们在等待过程中没有被中止，那我们再判断一下是不是还需要等待
  }
  // 如果不需要等待，说明我们可以授予锁了
  for (auto e : lock_request_queue->request_queue_) {
    if (e.txn_id_ == txn->GetTransactionId()) {
      e.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 首先判断这个锁是不是拥有这个shared锁
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() != TransactionState::GROWING) {
    return false;
  }
  if (!txn->GetSharedLockSet()->count(rid)) {
    return false;
  }
  

  // 拿到锁管理器
  std::unique_lock<std::mutex> guard(latch_);
  LockRequestQueue * lock_request_queue = &lock_table_[rid];
  // 设置一个EXCLUSIVE的锁请求
  LockRequest lockrequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequest * l = &lockrequest;
  for (auto& e : lock_request_queue->request_queue_) {
    if (e.txn_id_ == txn->GetTransactionId() && e.lock_mode_ == LockMode::SHARED) {
      // 升级锁，将请求改为EXCLUSIVE
      e.lock_mode_ = LockMode::EXCLUSIVE;
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->emplace(rid);
      *l = e;
      break;
    }
  }
  // 升级完成锁，我们还需要看看是不是要等待获取锁
  while (UpdateWait(txn, lock_request_queue)) {
    lock_request_queue->cv_.wait(guard);
  }
  l->granted_ = true;
  return true;
}


// READ_COMMITTED只需要防止脏读，解决方案就是读时上读锁，读完解读锁；
// 写时上写锁，但等到commit时才解写锁；读时上读锁，读完解读锁。
// 这样，永远不会读到未commit的数据，因为上面有写锁。
bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> guard(latch_);
  LockRequestQueue * lockqueue = &lock_table_[rid];
  LockMode mode;
  for (auto iter = lockqueue->request_queue_.begin(); iter != lockqueue->request_queue_.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      mode = iter->lock_mode_;
      lockqueue->request_queue_.erase(iter);
    }
  }
  // 当我们解锁完成后，我们需要设置这个事务处于什么阶段
  // 当隔离级别为 REPEATABLE_READ 时，S/X 锁释放会使事务进入 Shrinking 状态。当为 READ_COMMITTED 时，
  // 只有 X 锁释放使事务进入 Shrinking 状态。当为 READ_UNCOMMITTED 时，X 锁释放使事务 Shrinking，S 锁不会出现。
  if (!(mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  // 这个我们需要唤醒所有的等待加锁的事务,
  lockqueue->cv_.notify_all();
  return true;
}

bool LockManager::NeedWait(Transaction *txn, LockRequestQueue *lock_queue) {
  auto self = lock_queue->request_queue_.back();
  auto first_iter = lock_queue->request_queue_.begin();
  if (self.lock_mode_ == LockMode::SHARED) {
    if (first_iter->txn_id_ == txn->GetTransactionId() || first_iter->lock_mode_ == LockMode::SHARED) {
      // 从这里我们可以知道，唤醒之后，是执行事务请求队列中的第一个事务
      // 1. 看看这个锁请求队列中是不是只有一个事务，如果只有一个事务，那么就把这个锁给这个请求者（不需要等待）
      // 第一个一定是获取锁请求的，所以如果此时获取锁是SHARED那么是可以共存的s锁，不需要等待
      return false;
    }
  } else {
    if (first_iter->txn_id_ == txn->GetTransactionId()) {
      return false;
    }
  }
  // need wait, try to prevent it.
  bool need_wait = false;
  bool has_aborted = false;
  // 采取的是年轻的等待老的，老的直接抢占新的，事务的id越小越老
  for (auto iter = first_iter; iter->txn_id_ != txn->GetTransactionId(); iter++) {
    if (iter->txn_id_ > txn->GetTransactionId()) {  // 这个是比自己年轻的
      bool situation1 = self.lock_mode_ == LockMode::SHARED && iter->lock_mode_ == LockMode::EXCLUSIVE;
      // 情景一：自己申请的锁是S并且比自己年轻的锁是X
      bool situation2 = self.lock_mode_ == LockMode::EXCLUSIVE;  // 情景二：自己申请的锁是X锁
      if (situation1 || situation2) {
        // abort younger ，事务编号越大越年轻，以上两种情况，都不允许年轻的事务还持有锁，直接给他abort
        Transaction *younger_txn = TransactionManager::GetTransaction(iter->txn_id_);
        if (younger_txn->GetState() != TransactionState::ABORTED) {
          LOG_DEBUG("%d: Abort %d", txn->GetTransactionId(), iter->txn_id_);
          younger_txn->SetState(TransactionState::ABORTED);
          has_aborted = true;
        }
      }
      continue;
    }
    // 下面都是比自己老的事务在持有锁，所以我们要等待
    if (self.lock_mode_ == LockMode::EXCLUSIVE) {
      need_wait = true;
    }

    if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
      need_wait = true;  // 因为老事务的锁是EXCLUSIVE，那么我需要等待，无论我是SHARED还是EXCLUSIVE
    }
  }

  if (has_aborted) {
    lock_queue->cv_.notify_all();
    // 这个时候我们应该所有等待的事务都唤醒（也就是线程），唤醒后判断是否被aborted了，
    // 如果别中止了，那个线程就直接返回false了，也就是上锁失败了
  }

  return need_wait;
}

bool LockManager::UpdateWait(Transaction *txn, LockRequestQueue* lock_queue) {
  auto first_iter = lock_queue->request_queue_.begin();
  // 如果第一个不是，就继续等待
  if (first_iter->txn_id_ == txn->GetTransactionId()) {
      return false;
  }
  return true;
}

}  // namespace bustub
