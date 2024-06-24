#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

  void set_left_operator(PhysicalOperator *left_operator)
  {
    left_operator_ = left_operator;
  }

  void set_right_operator(PhysicalOperator *right_operator)
  {
    right_operator_ = right_operator;
  }

  void set_join_condition(Expression *join_condition)
  {
    join_condition_ = join_condition;
  }

private:
  RC evaluate_join_condition();

  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  PhysicalOperator *left_operator_;
  PhysicalOperator *right_operator_;
  Tuple *left_tuple_ = nullptr;
  Tuple *right_tuple_ = nullptr;
  Expression *join_condition_ = nullptr; //连接条件
};
