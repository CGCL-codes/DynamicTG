package tg.dtg.util;

import java.util.Iterator;

public class Iters {

  public static Iterator<Integer> stepIndices(int start, int step, int max) {
    return new StepIterator(start,step,max);
  }

  private static class StepIterator implements Iterator<Integer> {
    private int cur;
    private final int step;
    private final int max;

    private StepIterator(int start, int step, int max) {
      this.cur = start;
      this.step = step;
      this.max = max;
    }


    @Override
    public boolean hasNext() {
      return cur < max;
    }

    @Override
    public Integer next() {
      int temp = cur;
      cur += step;
      return temp;
    }
  }

}
