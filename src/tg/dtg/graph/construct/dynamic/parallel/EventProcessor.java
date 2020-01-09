package tg.dtg.graph.construct.dynamic.parallel;

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import tg.dtg.common.values.NumericValue;
import tg.dtg.events.Event;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.dynamic.KeySortedMultimap;
import tg.dtg.query.Predicate;

public class EventProcessor implements Runnable {

  private Iterator<EventVertex> vertices;
  private final Predicate predicate;
  private TreeSet<NumericValue> gaps;
  private KeySortedMultimap<NumericValue, TupleEdge<NumericValue, EventVertex, Object>> fromEdges;
  private KeySortedMultimap<NumericValue, TupleEdge<EventVertex, NumericValue, Object>> toEdges;

  public EventProcessor(Iterator<EventVertex> vertices, Predicate predicate,
      Comparator<NumericValue> cmp) {
    this.vertices = vertices;
    this.predicate = predicate;
    gaps = new TreeSet<>(cmp);
    fromEdges = new KeySortedMultimap<>(cmp);
    toEdges = new KeySortedMultimap<>(cmp);
  }

  public EventProcessor(Predicate predicate,
      Comparator<NumericValue> cmp) {
    this(null, predicate, cmp);
  }

  @Override
  public void run() {
    Preconditions.checkNotNull(vertices);
    while (vertices.hasNext()) {
      processVertex(vertices.next());
    }
  }

  public void setVertices(Iterator<EventVertex> vertices) {
    this.vertices = vertices;
  }

  private void processVertex(EventVertex vertex) {
    Event event = vertex.event;
    NumericValue tv = (NumericValue) predicate.func.apply(event.get(predicate.rightOperand));
    NumericValue fv = (NumericValue) event.get(predicate.leftOperand);
    gaps.add(tv);

    fromEdges.put(fv, new TupleEdge<>(fv, vertex, null));

    toEdges.put(tv, new TupleEdge<>(vertex, tv, null));
  }

  public TreeSet<NumericValue> getGaps() {
    return gaps;
  }

  public Iterator<TupleEdge<NumericValue, EventVertex, Object>> getFromEdges() {
    return fromEdges.valueIterator();
  }

  public Iterator<TupleEdge<EventVertex, NumericValue, Object>> getToEdges() {
    return toEdges.valueIterator();
  }
}
