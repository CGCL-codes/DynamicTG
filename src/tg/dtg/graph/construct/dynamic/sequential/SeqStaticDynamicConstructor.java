package tg.dtg.graph.construct.dynamic.sequential;

import static tg.dtg.util.Global.log;

import java.util.ArrayList;
import java.util.TreeSet;
import tg.dtg.common.values.NumericValue;
import tg.dtg.events.Event;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.dynamic.KeySortedMultimap;
import tg.dtg.graph.construct.dynamic.StaticManager;
import tg.dtg.graph.construct.dynamic.parallel.TupleEdge;
import tg.dtg.query.Predicate;

public class SeqStaticDynamicConstructor extends SequentialDynamicConstructor {

  private TreeSet<NumericValue> gaps;
  private KeySortedMultimap<NumericValue, TupleEdge<NumericValue, EventVertex, Object>> fromEdges;
  private KeySortedMultimap<NumericValue, TupleEdge<EventVertex, NumericValue, Object>> toEdges;
  private ArrayList<AttributeVertex> vertices;


  public SeqStaticDynamicConstructor(Predicate predicate,
      NumericValue start, NumericValue end,
      NumericValue step) {
    super(predicate, start, end, step);
    gaps = new TreeSet<>(cmp);
    fromEdges = new KeySortedMultimap<>(cmp);
    toEdges = new KeySortedMultimap<>(cmp);
  }

  @Override
  public void link(EventVertex eventVertex) {
    Event event = eventVertex.event;
    NumericValue tv = (NumericValue) predicate.func.apply(event.get(predicate.rightOperand));
    NumericValue fv = (NumericValue) event.get(predicate.leftOperand);
    gaps.add(tv);

    fromEdges.put(fv, new TupleEdge<>(fv, eventVertex, null));

    toEdges.put(tv, new TupleEdge<>(eventVertex, tv, null));
  }

  @Override
  public void manage() {
    StaticManager manager = new StaticManager(start, end, step, cmp, predicate);

    log("mange ranges");
    vertices = manager.mergeGaps(gaps.iterator());

    log("manage from edges");
    // manage edges
    countF = manager.reduceFromEdges(fromEdges.valueIterator(), vertices);

    log("manage to edges");
    countT = manager.reduceToEdges(toEdges.valueIterator(), vertices);
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
