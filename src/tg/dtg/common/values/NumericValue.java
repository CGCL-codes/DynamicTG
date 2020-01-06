package tg.dtg.common.values;

import static tg.dtg.util.Global.numericValueComparator;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;

public class NumericValue extends Value {

  protected NumericValue(double value) {
    super(value);
  }

  @Override
  public int compareTo(@Nonnull Value o) {
    Preconditions.checkNotNull(o);
    if (o instanceof NumericValue) {
      return numericValueComparator().compare(this, (NumericValue) o);
    } else {
      throw new IllegalArgumentException("must be numeric");
    }
  }

  @Override
  public String strVal() {
    throw new UnsupportedOperationException("not a str value");
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
