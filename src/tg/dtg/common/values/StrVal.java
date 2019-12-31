package tg.dtg.common.values;

import java.util.Objects;

public class StrVal extends Value {

  public StrVal(String value) {
    super(value);
  }

  @Override
  public int compareTo(Value o) {
    Objects.requireNonNull(o);
    if (o instanceof StrVal) {
      String t = (String) value;
      return t.compareTo((String) o.value);
    } else {
      throw new IllegalArgumentException("must be string");
    }
  }
}
