package tg.dtg.graph.construct.dynamic;

import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.dynamic.parallel.TupleEdge;
import tg.dtg.query.Predicate;

public class StaticManager {
  private final NumericValue start;
  private final NumericValue end;
  private final NumericValue step;
  private final Comparator<NumericValue> cmp;
  private final Predicate predicate;

  public StaticManager(NumericValue start, NumericValue end, NumericValue step,
      Comparator<NumericValue> cmp, Predicate predicate) {
    this.start = start;
    this.end = end;
    this.step = step;
    this.cmp = cmp;
    this.predicate = predicate;
  }

  public ArrayList<RangeAttributeVertex> mergeGaps(Iterator<NumericValue> it) {
    NumericValue lower = this.start;
    NumericValue prevGap = null;
    ArrayList<RangeAttributeVertex> ranges = new ArrayList<>();
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

  public int reduceFromEdges(Iterator<TupleEdge<NumericValue, EventVertex, Object>> fromEdges,
      ArrayList<RangeAttributeVertex> vertices) {
    int i = 0;
    int countF = 0;
    while (fromEdges.hasNext()) {
      TupleEdge<NumericValue, EventVertex, Object> edge = fromEdges.next();
      while (!vertices.get(i).getRange().contains(edge.getSource())) {
        i++;
      }
      vertices.get(i).linkToEvent(edge.getTarget());
      countF += 1;
    }
    return countF;
  }

  public int reduceToEdges(Iterator<TupleEdge<EventVertex, NumericValue, Object>> toEdges,
      ArrayList<RangeAttributeVertex> vertices) {
    int i = 0;
    int count = 0;
    int countT = 0;

    while (toEdges.hasNext()) {
      TupleEdge<EventVertex, NumericValue, Object> edge = toEdges.next();
      count++;
      while (!vertices.get(i).getRange().contains(edge.getTarget())) {
        i++;
      }
      switch (predicate.op) {
        case gt:
          for (int j = i + 1; j < vertices.size(); j++) {
            edge.getSource().linkToAttr(predicate.tag, vertices.get(j));
            countT++;
          }
          break;
        case eq:
          edge.getSource().linkToAttr(predicate.tag, vertices.get(i));
          break;
        default:
          break;
      }
    }
    System.out.println("raw to edges: " + count);
    return countT;
  }
}
