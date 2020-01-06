package tg.dtg.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class MergedIterator<E> implements Iterator<E> {

  private final PriorityQueue<Tuple<E, Iterator<E>>> queue;

  public MergedIterator(Iterable<Iterator<E>> iterators, Comparator<E> cmp) {
    queue = new PriorityQueue<>(2, (t1, t2) -> cmp.compare(t1.left, t2.left));
    for (Iterator<E> it : iterators) {
      if (it.hasNext()) {
        queue.offer(Tuple.of(it.next(), it));
      }
    }

  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty();
  }

  @Override
  public E next() {
    Tuple<E, Iterator<E>> tuple = queue.remove();
    if (tuple.right.hasNext()) {
      queue.offer(Tuple.of(tuple.right.next(), tuple.right));
    }
    return tuple.left;
  }
}
