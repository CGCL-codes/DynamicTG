package tg.dtg.graph.construct.dynamic;

import com.google.common.collect.Range;
import java.util.ArrayList;
import tg.dtg.common.values.NumericValue;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;

public class RangeAttributeVertex implements AttributeVertex, Comparable<RangeAttributeVertex> {

  protected final ArrayList<EventVertex> vertices;
  protected Range<NumericValue> range;

  public RangeAttributeVertex(
      Range<NumericValue> range) {
    this.range = range;
    vertices = new ArrayList<>();
  }

  public void linkToEvent(EventVertex eventVertex) {
    linkToEvent(null, eventVertex);
  }

  @Override
  public void linkToEvent(NumericValue value, EventVertex eventVertex) {
    vertices.add(eventVertex);
  }

  public Range<NumericValue> getRange() {
    return range;
  }

  public void setRange(Range<NumericValue> range) {
    this.range = range;
  }

  @Override
  public String toString() {
    return "RangeAttributeVertex{"
        + "range=" + range
        + '}';
  }

  @Override
  public int compareTo(RangeAttributeVertex o) {
    return range.lowerEndpoint().compareTo(o.range.lowerEndpoint());
  }
}
