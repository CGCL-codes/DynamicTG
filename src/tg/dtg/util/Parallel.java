package tg.dtg.util;

import com.google.common.base.Preconditions;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Parallel {
  private final ExecutorService executor;

  private static Parallel instance;

  private Parallel(int parallism) {
    executor = Executors.newFixedThreadPool(parallism);
  }

  public static void init(int parallism) {
    instance = new Parallel(parallism);
  }

  public static Parallel getInstance() {
    Preconditions.checkNotNull(instance);
    return instance;
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public void close() {
    try {
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
