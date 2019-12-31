package tg.dtg.common.values;

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
      return Double.compare((double) value, (double) o.value);
    } else {
      throw new IllegalArgumentException("must be numeric");
    }
  }
}
