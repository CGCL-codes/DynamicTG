package tg.dtg.graph.construct.dynamic.parallel;

import tg.dtg.common.values.NumericValue;
import tg.dtg.events.Event;
import tg.dtg.graph.EventVertex;
import tg.dtg.query.Predicate;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class EventProcessor implements Runnable {
  private final BlockingQueue<EventVertex> queue;
  private final Predicate predicate;
  private boolean isRun = true;
  private TreeSet<NumericValue> gaps;
  private TreeSet<TupleEdge<NumericValue, EventVertex, Object>> fromEdges;
  private TreeSet<TupleEdge<EventVertex, NumericValue, Object>> toEdges;

  public EventProcessor(BlockingQueue<EventVertex> queue, Predicate predicate) {
    this.queue = queue;
    this.predicate = predicate;
    gaps = new TreeSet<>();
    fromEdges = new TreeSet<>(Comparator.comparing(TupleEdge::getSource));
    toEdges = new TreeSet<>(Comparator.comparing(TupleEdge::getTarget));
  }

  private void stop() {
    isRun = false;
  }

  @Override
  public void run() {
    try {
      while (isRun) {
        EventVertex vertex = queue.poll(100, TimeUnit.MILLISECONDS);
        processVertex(vertex);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void processVertex(EventVertex vertex) {
    Event event = vertex.event;
    NumericValue tv = (NumericValue) predicate.func.apply(event.get(predicate.rightOperand));
    NumericValue fv = (NumericValue) event.get(predicate.leftOperand);
    gaps.add(tv);
    fromEdges.add(new TupleEdge<>(fv, vertex, null));
    toEdges.add(new TupleEdge<>(vertex, tv, null));
  }

  public TreeSet<NumericValue> getGaps() {
    return gaps;
  }

  public TreeSet<TupleEdge<EventVertex, NumericValue, Object>> getToEdges() {
    return toEdges;
  }

  public TreeSet<TupleEdge<NumericValue, EventVertex, Object>> getFromEdges() {
    return fromEdges;
  }
}
