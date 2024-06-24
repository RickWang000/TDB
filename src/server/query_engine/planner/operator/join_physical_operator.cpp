#include "include/query_engine/planner/operator/join_physical_operator.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

JoinPhysicalOperator::JoinPhysicalOperator() = default;

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
  if (left_operator_->next() == RC::SUCCESS) {
    left_tuple_ = left_operator_->current_tuple();
  }
  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  RC rc;

  while (left_tuple_ != nullptr) {
    if (right_operator_->next() == RC::SUCCESS) {
      right_tuple_ = right_operator_->current_tuple();
    } else {
      if (left_operator_->next() == RC::SUCCESS) {
        left_tuple_ = left_operator_->current_tuple();
        right_operator_->open(trx_);
        continue;
      } else {
        return RC::RECORD_EOF;
      }
    }

    if ((rc = evaluate_join_condition()) == RC::SUCCESS) {
      joined_tuple_.set_left(left_tuple_);
      joined_tuple_.set_right(right_tuple_);
      return RC::SUCCESS;
    }
  }

  return RC::RECORD_EOF;
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  left_operator_->close();
  right_operator_->close();
  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}

RC JoinPhysicalOperator::evaluate_join_condition()
{
  if (join_condition_ == nullptr) {
    return RC::SUCCESS;
  }

  Value left_value;
  Value right_value;

  RC rc = join_condition_->get_value(*left_tuple_, left_value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = join_condition_->get_value(*right_tuple_, right_value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 假设 join_condition_ 是一个等值连接条件
  if (left_value.compare(right_value) == 0) {
    return RC::SUCCESS;
  }

  return RC::RECORD_EOF;
}
