package tg.dtg.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
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

  // debug
  public static boolean pdfs = false;

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
    DEFAULT_NUMERIC_COMPARATOR = numericValueComparator(precison / 10000);
    isSetValueConfig = true;
  }

  public static int getParallism() {
    return parallism;
  }

  public static ExecutorService getExecutor() {
    return executor;
  }
  
  public static <T> ArrayList<T> callAndSync(ArrayList<Supplier<T>> tasks)
      throws ExecutionException, InterruptedException {
    AtomicInteger counter = new AtomicInteger();
    final int size = tasks.size();
    final int step = Math.max(1,(int) Math.floor(size / 100.0));
    System.out.print("total " + size + " tasks" + "  ");
    ArrayList<CompletableFuture<T>> futures = new ArrayList<>(tasks.size());
    for (Supplier<T> task:tasks) {
      CompletableFuture<T> future = CompletableFuture.supplyAsync(task,executor)
          .thenApply((x)->{
            int count = counter.incrementAndGet();
            if (count % step == 0) {
              System.out.print(".");
            }
            return x;
          });
      futures.add(future);
    }
    System.out.println();
    ArrayList<T> results = new ArrayList<>();
    for(Future<T> future: futures) {
      results.add(future.get());
    }
    return results;
  }

  public static void runAndSync(List<Runnable> tasks)
      throws ExecutionException, InterruptedException {
    AtomicInteger counter = new AtomicInteger();
    final int size = tasks.size();
    final int step = Math.max(1,(int) Math.floor(size / 100.0));
    System.out.println("total " + size + " tasks" + "  ");
    ArrayList<CompletableFuture<Void>> futures = new ArrayList<>(tasks.size());
    for (Runnable task:tasks) {
      CompletableFuture<Void> future = CompletableFuture
          .runAsync(task,executor)
          .thenAccept((x)->{
            int count = counter.incrementAndGet();
            if (count % step == 0) {
              System.out.print(".");
            }
          });
      futures.add(future);
    }
    for(Future<?> future: futures) {
      future.get();
    }
    System.out.println();
  }

  private static synchronized void progress(int percent) {
    System.out.print(String.format("%02d%%",percent));
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
}
