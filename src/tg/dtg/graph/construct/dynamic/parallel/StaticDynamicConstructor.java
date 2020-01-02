package tg.dtg.graph.construct.dynamic.parallel;

import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.graph.construct.dynamic.RangeAttributeVertex;
import tg.dtg.query.Predicate;
import tg.dtg.util.MergedIterator;
import tg.dtg.util.Parallel;

public class StaticDynamicConstructor extends Constructor {

  private final EventProcessor[] processors;
  private final BlockingQueue<EventVertex> queue;
  private final NumericValue start;
  private final NumericValue end;
  private final NumericValue step;
  private Future<?>[] futures;
  private ArrayList<RangeAttributeVertex> vertices;

  public StaticDynamicConstructor(int parallism, Predicate predicate,
      NumericValue start, NumericValue end,
      NumericValue step) {
    super(predicate);
    ExecutorService executor = Parallel.getInstance().getExecutor();
    queue = new LinkedBlockingQueue<>();
    processors = new EventProcessor[parallism];
    for (int i = 0; i < processors.length; i++) {
      processors[i] = new EventProcessor(queue, predicate);
    }
    futures = new Future[parallism];
    for (int i = 0; i < processors.length; i++) {
      futures[i] = executor.submit(processors[i]);
    }
    this.start = start;
    this.end = end;
    this.step = step;
  }

  @Override
  public void link(EventVertex eventVertex) {
    queue.offer(eventVertex);
  }

  @Override
  public void invokeEventsEnd() {
    try {
      for (int i = 0; i < processors.length; i++) {
        processors[i].stop();
        futures[i].get();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void manage() {
    // merge gaps
    ArrayList<Iterator<NumericValue>> its = new ArrayList<>(processors.length);
    ArrayList<Iterator<TupleEdge<NumericValue, EventVertex, Object>>> fromtIts = new ArrayList<>(
        processors.length);
    ArrayList<Iterator<TupleEdge<EventVertex, NumericValue, Object>>> toIts = new ArrayList<>(
        processors.length);

    for (EventProcessor processor : processors) {
      its.add(processor.getGaps().iterator());
      fromtIts.add(processor.getFromEdges().iterator());
      toIts.add(processor.getToEdges().iterator());
    }

    System.out.println("mange ranges");
    vertices = mergeGaps1(its);

    System.out.println("manage from edges");
    // manage edges
    Iterator<TupleEdge<NumericValue, EventVertex, Object>> fromEdges = new MergedIterator<>(
        fromtIts,
        Comparator.comparing(TupleEdge::getSource));
    int i = 0;
    while (fromEdges.hasNext()) {
      TupleEdge<NumericValue, EventVertex, Object> edge = fromEdges.next();
      while (!vertices.get(i).getRange().contains(edge.getSource())) {
        i++;
      }
      vertices.get(i).linkToEvent(edge.getTarget());
    }

    System.out.println("manage to edges");
    Iterator<TupleEdge<EventVertex, NumericValue, Object>> toEdges = new MergedIterator<>(toIts,
        Comparator.comparing(TupleEdge::getTarget));
    i = 0;
    while (toEdges.hasNext()) {
      TupleEdge<EventVertex, NumericValue, Object> edge = toEdges.next();
      while (!vertices.get(i).getRange().contains(edge.getTarget())) {
        i++;
      }
      switch (predicate.op) {
        case gt:
          for (int j = i + 1; j < vertices.size(); j++) {
            edge.getSource().linkToAttr(predicate.tag, vertices.get(j));
          }
          break;
        case eq:
          edge.getSource().linkToAttr(predicate.tag, vertices.get(i));
          break;
        default:
          break;
      }
    }
  }

  private ArrayList<RangeAttributeVertex> mergeGaps1(ArrayList<Iterator<NumericValue>> its) {
    NumericValue lower = this.start;
    NumericValue prevGap = null;
    ArrayList<RangeAttributeVertex> ranges = new ArrayList<>();
    MergedIterator<NumericValue> it = new MergedIterator<>(its, Ordering.natural());
    Range<NumericValue> range;
    while (it.hasNext()) {
      NumericValue gap = it.next();
      if (prevGap != null && gap.compareTo(prevGap) == 0) {
        continue;
      }
      switch (predicate.op) {
        case gt:
          NumericValue upper = (NumericValue) Value.numeric(
              gap.numericVal() + step.numericVal());
          range = Range.closedOpen(lower, upper);
          lower = upper;
          ranges.add(new RangeAttributeVertex(range));
          break;
        case eq:
          range = Range.singleton(gap);
          ranges.add(new RangeAttributeVertex(range));
          break;
        default:
          break;
      }
      prevGap = gap;
    }
    switch (predicate.op) {
      case gt:
        range = Range.closedOpen(lower, end);
        ranges.add(new RangeAttributeVertex(range));
        break;
      default:
        break;
    }
    return ranges;
  }

  @Override
  public Iterator<AttributeVertex> attributes() {
    return Iterators.transform(vertices.iterator(), rv -> rv);
  }
}
