package tg.dtg.graph;

import tg.dtg.common.values.NumericValue;

public interface AttributeVertex extends Vertex {

  void linkToEvent(NumericValue value, EventVertex eventVertex);

  default void linkFromEvent(EventVertex eventVertex) {
    eventVertex.linkAttribute(this);
  }
}
