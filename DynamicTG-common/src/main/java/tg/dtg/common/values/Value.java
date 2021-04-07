package tg.dtg.common.values;

import java.io.Serializable;

public abstract class Value implements Comparable<Value>, Serializable {

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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Value value1 = (Value) o;
    return value.equals(value1.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public String toString() {
    return "Value(" + value.toString() + ")";
  }
}
