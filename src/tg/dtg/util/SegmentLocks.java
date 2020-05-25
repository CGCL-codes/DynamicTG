package tg.dtg.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class SegmentLocks<T> {
  private final Lock[] locks;
  private Function<T,Integer> func;

  public SegmentLocks(int n) {
    locks = new Lock[n];
    init();
  }

  private void init() {
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantLock();
    }
  }

  public void registerMapping(Function<T,Integer> mapFunc) {
    this.func = mapFunc;
  }

  public Lock acquireLock(T seed) {
    int id = func.apply(seed) % locks.length;
    return locks[id];
  }
}
