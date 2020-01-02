package tg.dtg.query;

import java.util.ArrayList;
import java.util.function.Function;

import tg.dtg.common.values.Value;

public class Predicate extends Expression {
  private static char ctag = 'a';
  public final Operator op;
  public final int leftOperand;
  public final int rightOperand;
  public final Function<Value, Value> func;
  public final char tag;

  public Predicate(Operator op, int leftOperand, int rightOperand,
                   Function<Value, Value> func, char tag) {
    this.op = op;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
    this.func = func;
    this.tag = tag;
  }

  public Predicate(Operator op, int leftOperand, int rightOperand) {
    this(op, leftOperand, rightOperand, value -> value, ctag);
    ctag += 1;
  }

  @Override
  public ArrayList<Predicate> predicates() {
    ArrayList<Predicate> predicates = new ArrayList<>(1);
    predicates.add(this);
    return predicates;
  }
}
