package tg.dtg.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Global {

  // parallel configs
  private static boolean isSetParallel = false;
  private static int parallism;
  private static ExecutorService executor;

  // debug
  public static boolean pdfs = false;

  public static void initParallel(int parallism) {
    if (isSetParallel) {
      throw new Config.DuplicateSetGlobalError("parallel");
    }
    Global.parallism = parallism;
    executor = Executors.newFixedThreadPool(parallism);
    isSetParallel = true;
  }

  public static void initValue(double precison) {
    Config.initValue(precison);
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

  private static long prevTm = 0L;
  public static void log(String s) {
    long tm = System.currentTimeMillis();
    if(prevTm <= 0) System.out.println(s + " time " + tm);
    else {
      System.out.println(s + " time " + tm + ", " + (tm-prevTm));
    }
    prevTm = tm;
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

  public static String threadInfo(){
    ThreadMXBean mxbean = ManagementFactory.getThreadMXBean();
    long[] ids = mxbean.getAllThreadIds();
    ThreadInfo[] infos = mxbean.getThreadInfo(ids);
    return Stream.of(infos).filter(info->info.getThreadName().contains("pool"))
            .map(info->String.format("%s: %d, %d, %d",
                    info.getThreadName(),
                    TimeUnit.NANOSECONDS.toMillis(mxbean.getThreadCpuTime(info.getThreadId())),
                    TimeUnit.NANOSECONDS.toMillis(mxbean.getThreadUserTime(info.getThreadId())),
                    info.getBlockedTime()))
            .collect(Collectors.joining("\n"));
  }

  public static long toMillis(long nano){
    return TimeUnit.NANOSECONDS.toMillis(nano);
  }
}
