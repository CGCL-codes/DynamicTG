package tg.dtg.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import tg.dtg.common.values.NumericValue;

public final class Global {

  // value configs
  private static boolean isSetValueConfig = false;
  private static double precison;
  private static Comparator<NumericValue> DEFAULT_NUMERIC_COMPARATOR;

  // parallel configs
  private static boolean isSetParallel = false;
  private static int parallism;
  private static ExecutorService executor;

  public static void initParallel(int parallism) {
    if (isSetParallel) {
      throw new DuplicateSetGlobalError("parallel");
    }
    Global.parallism = parallism;
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

  public static int getParallism() {
    return parallism;
  }

  public static ExecutorService getExecutor() {
    return executor;
  }
  
  public static <T> ArrayList<T> callAndSync(ArrayList<Callable<T>> tasks)
      throws ExecutionException, InterruptedException {
    ArrayList<Future<T>> futures = new ArrayList<>(tasks.size());
    for (Callable<T> task:tasks) {
      futures.add(executor.submit(task));
    }
    ArrayList<T> results = new ArrayList<>(tasks.size());
    for(Future<T> future: futures) {
      results.add(future.get());
    }
    return results;
  }

  public static void runAndSync(ArrayList<Runnable> tasks)
      throws ExecutionException, InterruptedException {
    ArrayList<Future<?>> futures = new ArrayList<>(tasks.size());
    for (Runnable task:tasks) {
      futures.add(executor.submit(task));
    }
    for(Future<?> future: futures) {
      future.get();
    }
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

  public static int tableSizeFor(int cap) {
    int n = cap - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : n + 1;
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
