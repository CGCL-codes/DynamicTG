package tg.dtg.graph.construct.dynamic.parallel;

import static tg.dtg.util.Global.getExecutor;
import static tg.dtg.util.Global.log;

import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.dynamic.RangeAttributeVertex;
import tg.dtg.graph.construct.dynamic.StaticManager;
import tg.dtg.query.Predicate;
import tg.dtg.util.Global;
import tg.dtg.util.MergedIterator;

public class ParallelStaticDynamicConstructor extends ParallelDynamicConstructor {

  private final EventProcessor[] processors;

  private Future<?>[] futures;

  private ArrayList<AttributeVertex> vertices;


  public ParallelStaticDynamicConstructor(int parallism, Predicate predicate,
      NumericValue start, NumericValue end,
      NumericValue step) {
    super(predicate, start, end, step);

    processors = new EventProcessor[parallism];
    for (int i = 0; i < processors.length; i++) {
      processors[i] = new EventProcessor(predicate, cmp);
    }
    futures = new Future[parallism];

  }

  @Override
  public void parallelLink(ArrayList<Iterator<EventVertex>> iterators) {
    ExecutorService executor = Global.getExecutor();
    for (int i = 0; i < iterators.size(); i++) {
      processors[i].setVertices(iterators.get(i));
    }
    for (int i = 0; i < processors.length; i++) {
      futures[i] = executor.submit(processors[i]);
    }
  }

  @Override
  public void invokeEventsEnd() {
    try {
      for (int i = 0; i < processors.length; i++) {
        futures[i].get();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void manage() {
    manage2();
  }

  public void manage2() {
    ParallelManager manager = new ParallelManager(getExecutor());

    ArrayList<Iterator<NumericValue>> its = new ArrayList<>(processors.length);
    ArrayList<Iterator<TupleEdge<NumericValue, EventVertex, Object>>> fromtIts = new ArrayList<>(
        processors.length);
    ArrayList<Iterator<TupleEdge<EventVertex, NumericValue, Object>>> toIts = new ArrayList<>(
        processors.length);

    for (EventProcessor processor : processors) {
      its.add(processor.getGaps().iterator());
      fromtIts.add(processor.getFromEdges());
      toIts.add(processor.getToEdges());
    }
    try {
      log("mange ranges");
//      manager.mergeTest(its, start,end,step,predicate.op);
      vertices = manager.mergeGaps(its, start, end, step, predicate.op);
//       vertices = mergeGaps1(its);
      // manage edges
      log("manage from edges");
      countF = manager.reduceFromEdges(fromtIts, vertices);

      log("manage to edges");
      countT = manager.reduceToEdges(toIts, predicate, vertices);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void manage1() {
    // merge gaps
    ArrayList<Iterator<NumericValue>> its = new ArrayList<>(processors.length);
    ArrayList<Iterator<TupleEdge<NumericValue, EventVertex, Object>>> fromtIts = new ArrayList<>(
        processors.length);
    ArrayList<Iterator<TupleEdge<EventVertex, NumericValue, Object>>> toIts = new ArrayList<>(
        processors.length);

    for (EventProcessor processor : processors) {
      its.add(processor.getGaps().iterator());
      fromtIts.add(processor.getFromEdges());
      toIts.add(processor.getToEdges());
    }

    StaticManager manager = new StaticManager(start, end, step, cmp, predicate);

    log("mange ranges");
    MergedIterator<NumericValue> it = new MergedIterator<>(its, cmp);
    vertices = manager.mergeGaps(it);

    log("manage from edges");
    // manage edges
    Iterator<TupleEdge<NumericValue, EventVertex, Object>> fromEdges = new MergedIterator<>(
        fromtIts,
        Ordering.from(cmp).onResultOf(TupleEdge::getSource));
    countF = manager.reduceFromEdges(fromEdges, vertices);

    log("manage to edges");
    Iterator<TupleEdge<EventVertex, NumericValue, Object>> toEdges = new MergedIterator<>(toIts,
        //Comparator.comparing(TupleEdge::getTarget));
        Ordering.from(cmp).onResultOf(TupleEdge::getTarget));
    countT = manager.reduceToEdges(toEdges, vertices);
  }

  private ArrayList<RangeAttributeVertex> mergeGaps1(ArrayList<Iterator<NumericValue>> its) {
    NumericValue lower = this.start;
    NumericValue prevGap = null;
    ArrayList<RangeAttributeVertex> ranges = new ArrayList<>();
    MergedIterator<NumericValue> it = new MergedIterator<>(its, cmp);
    Range<NumericValue> range;
    while (it.hasNext()) {
      NumericValue gap = it.next();
      if (prevGap != null && cmp.compare(gap, prevGap) == 0) {
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
  public int countAttr() {
    return vertices.size();
  }

  @Override
  public ArrayList<AttributeVertex> attributes() {
    return vertices;
  }
}
