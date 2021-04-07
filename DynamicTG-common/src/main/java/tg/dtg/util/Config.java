package tg.dtg.util;

import tg.dtg.common.values.NumericValue;

import java.util.Comparator;

public class Config {
  // value configs
  private static boolean isSetValueConfig = false;
  private static double precison;

  private static Comparator<NumericValue> DEFAULT_NUMERIC_COMPARATOR = new NumericValueComparator();

  public static Comparator<NumericValue> numericValueComparator(double step) {
    return new NumericValueComparator(step);
  }

  public static Comparator<NumericValue> numericValueComparator() {
    return DEFAULT_NUMERIC_COMPARATOR;
  }

  public static class NumericValueComparator implements Comparator<NumericValue> {

    private final double step;

    public NumericValueComparator(double step) {
      this.step = step;
    }

    public NumericValueComparator() {
      this(0.0001);
    }

    @Override
    public int compare(NumericValue o1, NumericValue o2) {
      double diff = o1.numericVal() - o2.numericVal();
      if (diff > step) {
        return 1;
      } else if (diff < -step) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  public static void initValue(double precison) {
    if (isSetValueConfig) {
      throw new DuplicateSetGlobalError("value");
    }
    Config.precison = precison;
    DEFAULT_NUMERIC_COMPARATOR = numericValueComparator(precison / 10000);
    isSetValueConfig = true;
  }

  public static final class DuplicateSetGlobalError extends RuntimeException {

    public DuplicateSetGlobalError() {
    }

    public DuplicateSetGlobalError(String position) {
      super("duplicate set global in " + position);
    }
  }
}
