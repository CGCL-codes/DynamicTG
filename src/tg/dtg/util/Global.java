package tg.dtg.util;

import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import tg.dtg.common.values.NumericValue;

public final class Global {

  // value configs
  private static boolean isSetValueConfig = false;
  private static double precison;
  private static Comparator<NumericValue> DEFAULT_NUMERIC_COMPARATOR;

  // parallel configs
  private static boolean isSetParallel = false;
  private static ExecutorService executor;

  public static void initParallel(int parallism) {
    if (isSetParallel) {
      throw new DuplicateSetGlobalError("parallel");
    }
    executor = Executors.newFixedThreadPool(parallism);
    isSetParallel = true;
  }

  public static void initValue(double precison) {
    if (isSetValueConfig) {
      throw new DuplicateSetGlobalError("value");
    }
    Global.precison = precison;
    DEFAULT_NUMERIC_COMPARATOR = numericValueComparator(precison / 1000);
    isSetValueConfig = true;
  }

  public static ExecutorService getExecutor() {
    return executor;
  }

  public static void close(long timeout, TimeUnit timeUnit) {
    try {
      executor.shutdown();
      executor.awaitTermination(timeout, timeUnit);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void log(String s) {
    System.out.println(s + " time " + System.currentTimeMillis());
  }

  public static Comparator<NumericValue> numericValueComparator(double step) {
    return new NumericValueComparator(step);
  }

  public static Comparator<NumericValue> numericValueComparator() {
    assert isSetValueConfig;
    return DEFAULT_NUMERIC_COMPARATOR;
  }

  private static final class DuplicateSetGlobalError extends RuntimeException {

    public DuplicateSetGlobalError() {
    }

    public DuplicateSetGlobalError(String position) {
      super("duplicate set global in " + position);
    }
  }

  private static class NumericValueComparator implements Comparator<NumericValue> {

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
}