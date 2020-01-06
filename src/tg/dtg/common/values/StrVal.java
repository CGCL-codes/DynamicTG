package tg.dtg.common.values;

import javax.annotation.Nonnull;

public class StrVal extends Value {

  public StrVal(String value) {
    super(value);
  }

  @Override
  public int compareTo(@Nonnull Value o) {
    if (o instanceof StrVal) {
      String t = (String) value;
      return t.compareTo((String) o.value);
    } else {
      throw new IllegalArgumentException("must be string");
    }
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
