package tg.dtg.query;

import java.util.ArrayList;
import java.util.function.Function;
import tg.dtg.common.values.Value;

public class Predicate extends Expression{
  public final Operator op;
  public final int leftOperand;
  public final int rightOperand;
  public final Function<Value, Value> func;

  public Predicate(Operator op, int leftOperand, int rightOperand,
      Function<Value, Value> func) {
    this.op = op;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
    this.func = func;
  }

  public Predicate(Operator op, int leftOperand, int rightOperand) {
    this(op,leftOperand,rightOperand,value -> value);
  }

  @Override
  public ArrayList<Predicate> predicates() {
    ArrayList<Predicate> predicates = new ArrayList<>(1);
    predicates.add(this);
    return predicates;
  }
}
