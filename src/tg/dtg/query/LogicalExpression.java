package tg.dtg.query;

import java.util.ArrayList;

public class LogicalExpression extends Expression {
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
  public ArrayList<Predicate> predicates() {
    ArrayList<Predicate> predicates = left.predicates();
    predicates.addAll(right.predicates());
    return predicates;
  }

  public enum LogicalOperater {
    and, or
  }
}
