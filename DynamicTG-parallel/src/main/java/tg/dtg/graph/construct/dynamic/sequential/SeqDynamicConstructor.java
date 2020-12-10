package tg.dtg.graph.construct.dynamic.sequential;

import static tg.dtg.util.Global.log;

import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.events.Event;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.dynamic.KeySortedMultimap;
import tg.dtg.graph.construct.dynamic.RangeAttributeVertex;
import tg.dtg.graph.construct.dynamic.parallel.TupleEdge;
import tg.dtg.query.Predicate;

public class SeqDynamicConstructor extends SequentialDynamicConstructor {

  protected final TreeMap<NumericValue, RangeAttributeVertex> vertices;
  protected final KeySortedMultimap<NumericValue,
      TupleEdge<NumericValue, EventVertex, Object>> fromEdges;
  protected final KeySortedMultimap<RangeAttributeVertex,
      TupleEdge<EventVertex, RangeAttributeVertex, Object>> toEdges;

  /**
   * dynamic constructor.
   *
   * @param start start of value range
   * @param end end of value range
   * @param step precision
   */
  public SeqDynamicConstructor(Predicate predicate,
      NumericValue start, NumericValue end, NumericValue step) {
    super(predicate, start, end, step);
    vertices = new TreeMap<>(cmp);
    fromEdges = new KeySortedMultimap<>(cmp);
    toEdges = new KeySortedMultimap<>(Ordering.natural());

    RangeAttributeVertex root = new RangeAttributeVertex(Range.closedOpen(start, end));
    vertices.put(start, root);
  }

  @Override
  public void link(EventVertex eventVertex) {
    Event event = eventVertex.event;
    // from edge
    NumericValue fv = (NumericValue) event.get(predicate.leftOperand);
    fromEdges.put(fv, new TupleEdge<>(fv, eventVertex, null));

    // to edge
    NumericValue tv = (NumericValue) predicate.func.apply(event.get(predicate.rightOperand));

    switch (predicate.op) {
      case gt:
        processGt(tv, eventVertex);
        break;
      default:
        break;
    }
  }

  private void processGt(NumericValue tv, EventVertex eventVertex) {
    Entry<NumericValue, RangeAttributeVertex> entry = vertices.floorEntry(tv);
    // split first key
    RangeAttributeVertex vertex = entry.getValue();
    Range<NumericValue> range = vertex.getRange();
    NumericValue upper = (NumericValue) Value.numeric(
        tv.numericVal() + step.numericVal());
    if (upper.compareTo(range.upperEndpoint()) != 0) {
      // the max value in the range do not split the range
      Range<NumericValue> left = Range.closedOpen(range.lowerEndpoint(), upper);
      vertex.setRange(left);
      Range<NumericValue> right = Range.closedOpen(upper, range.upperEndpoint());

      RangeAttributeVertex rvertex = new RangeAttributeVertex(right);
      vertices.put(upper, rvertex);

      // copy edges
      ArrayList<TupleEdge<EventVertex, RangeAttributeVertex, Object>> edges = toEdges
          .getMultiple(vertex);
      if (edges != null) {
        ArrayList<TupleEdge<EventVertex, RangeAttributeVertex, Object>> nedges = new ArrayList<>(
            edges.size());
        for (TupleEdge<EventVertex, RangeAttributeVertex, Object> edge : edges) {
          nedges.add(new TupleEdge<>(edge.getSource(), rvertex, null));
        }
        toEdges.putMultiple(rvertex, nedges);
      }
    }

    // add edges
    for (RangeAttributeVertex rangeAttributeVertex : vertices.tailMap(upper).values()) {
      toEdges.put(rangeAttributeVertex, new TupleEdge<>(eventVertex, rangeAttributeVertex, null));
    }
  }

  @Override
  public void manage() {
    // manage from edges
    log("manage from edges");
    Iterator<TupleEdge<NumericValue, EventVertex, Object>> fromIt = fromEdges.valueIterator();
    Iterator<RangeAttributeVertex> it = vertices.values().iterator();
    RangeAttributeVertex vertex = it.next();
    TupleEdge<NumericValue, EventVertex, Object> fedge;
    while (fromIt.hasNext()) {
      fedge = fromIt.next();
      while (!vertex.getRange().contains(fedge.getSource())) {
        if (it.hasNext()) {
          vertex = it.next();
        } else {
          throw new IllegalStateException("not suitable attribute vertices");
        }
      }
      vertex.linkToEvent(fedge.getTarget());
      countF++;
    }

    log("manage to edges");
    // manage to edges
    Iterator<TupleEdge<EventVertex, RangeAttributeVertex, Object>> tit = toEdges.valueIterator();
    TupleEdge<EventVertex, RangeAttributeVertex, Object> tedge;
    while (tit.hasNext()) {
      tedge = tit.next();
      tedge.getSource().linkToAttr(predicate.tag, tedge.getTarget());
      countT++;
    }
  }

  @Override
  public ArrayList<AttributeVertex> attributes() {
    return new ArrayList<>(this.vertices.values());
  }

  @Override
  public int countAttr() {
    return vertices.size();
  }
}
