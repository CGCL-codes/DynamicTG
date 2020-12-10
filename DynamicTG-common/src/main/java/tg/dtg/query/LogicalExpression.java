package tg.dtg.query;

import java.util.ArrayList;
import java.util.Map;

public class LogicalExpression implements Expression {
  private Expression left;
  private Expression right;

  public final LogicalOperater operater;

  /**
   * create a logical expression.
   * @param left the left expression
   * @param right the right expression
   * @param operater the type of logical operator, "and" or "or"
   */
  public LogicalExpression(Expression left, Expression right,
      LogicalOperater operater) {
    this.left = left;
    this.right = right;
    this.operater = operater;
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  @Override
  public boolean isLogical() {
    return true;
  }

  @Override
  public Map<Character,Predicate> predicates() {
    Map<Character, Predicate> predicates = left.predicates();
    predicates.putAll(right.predicates());
    return predicates;
  }

  public enum LogicalOperater {
    and, or
  }
}
