#pragma once

#include <memory>
#include "logical_node.h"

//TODO [Lab3] 请根据需要实现自己的JoinLogicalNode，当前实现仅为建议实现
class JoinLogicalNode : public LogicalNode
{
public:
  JoinLogicalNode() = default;
  ~JoinLogicalNode() override = default;

  LogicalNodeType type() const override
  {
    return LogicalNodeType::JOIN;
  }

  void set_condition(std::unique_ptr<Expression> &&condition)
  {
    condition_ = std::move(condition);
  }

  std::unique_ptr<Expression> &condition()
  {
    return condition_;
  }

  void set_left_child(std::unique_ptr<LogicalNode> &&left)
  {
    left_child_ = std::move(left);
  }

  void set_right_child(std::unique_ptr<LogicalNode> &&right)
  {
    right_child_ = std::move(right);
  }

  LogicalNode* left_child() const
  {
    return left_child_.get();
  }

  LogicalNode* right_child() const
  {
    return right_child_.get();
  }

private:
  // Join的条件，目前只支持等值连接
  std::unique_ptr<Expression> condition_;
  std::unique_ptr<LogicalNode> left_child_;
  std::unique_ptr<LogicalNode> right_child_;
};
