package tg.dtg.graph.construct.dynamic;

import java.util.ArrayList;
import java.util.List;

import tg.dtg.common.values.NumericValue;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;

public abstract class DynamicAttributeVertex implements AttributeVertex {

  protected NumericValue start;
  protected NumericValue end;
  protected final OuterEdge head;

  protected DynamicAttributeVertex(NumericValue start, NumericValue end, OuterEdge edge) {
    this.start = start;
    this.end = end;
    this.head = new OuterEdge(null);
    head.next = edge;
  }

  @Override
  public void linkToEvent(NumericValue value, EventVertex eventVertex) {
    OuterEdge edge = head.next;
    OuterEdge prev = head;
    while (edge != null) {
      int cmp = edge.value.compareTo(value);
      if (cmp == 0) {
        edge.nodes.add(eventVertex);
        return;
      } else if (cmp > 0) {
        OuterEdge nedge = new OuterEdge(value);
        nedge.nodes.add(eventVertex);
        nedge.next = edge;
        prev.next = nedge;
        return;
      } else {
        edge = edge.next;
        prev = prev.next;
      }
    }
    OuterEdge nedge = new OuterEdge(value);
    nedge.nodes.add(eventVertex);
    nedge.next = edge;
    prev.next = nedge;
  }

  protected static class OuterEdge {

    final NumericValue value;
    final List<EventVertex> nodes;
    OuterEdge next;

    public OuterEdge(NumericValue value) {
      this.value = value;
      nodes = new ArrayList<>();
    }

    @Override
    public String toString() {
      return "OuterEdge{"
          + "value=" + value
          + '}';
    }
  }
}
