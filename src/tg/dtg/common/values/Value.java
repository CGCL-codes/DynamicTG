package tg.dtg.common.values;

public abstract class Value implements Comparable<Value> {

  Object value;

  protected Value(Object value) {
    this.value = value;
  }

  public String strVal() {
    return (String) value;
  }

  public double numericVal() {
    return (Double) value;
  }

  public enum ValType {
    str, numeric
  }

  public static Value str(String value) {
    return new StrVal(value);
  }

  public static Value numeric(double value) {
    return new NumericValue(value);
  }

  @Override
  public String toString() {
    return "Value("+value.toString()+")";
  }
}
