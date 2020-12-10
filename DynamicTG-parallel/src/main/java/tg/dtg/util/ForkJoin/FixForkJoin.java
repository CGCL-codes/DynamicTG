package tg.dtg.util.ForkJoin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import tg.dtg.util.Global;

public class FixForkJoin {

  private final ExecutorService executorService;

  public FixForkJoin() {
    this.executorService = Global.getExecutor();
  }

  public static abstract class ForkJoinTask<T> {

    private ArrayList<ForkJoinTask<T>> childTasks = new ArrayList<>();
    private CompletableFuture<T> result;
    private ExecutorService executor;

    public void setExecutor(ExecutorService executor) {
      this.executor = executor;
    }

    protected void fork(ForkJoinTask<T> newTask) {
      childTasks.add(newTask);
    }

    protected void join()
        throws InterruptedException, ExecutionException {
      ArrayList<CompletableFuture<T>> futures = new ArrayList<>(childTasks.size());
      for (ForkJoinTask<T> task : childTasks) {
        futures.add(CompletableFuture.supplyAsync(task::compute, executor));
      }

      CompletableFuture<List<T>> nfuture = CompletableFuture.completedFuture(new ArrayList<>());
      for (CompletableFuture<T> future : futures) {
        nfuture = nfuture.thenCombineAsync(future, (list, r) -> {
          list.add(r);
          return list;
        }, executor);
      }
      result = nfuture.thenApply(this::merge);
    }

    protected abstract T merge(List<T> list);

    protected abstract T compute();

    private T get() throws ExecutionException, InterruptedException {

      return result.get();
    }
  }
}
