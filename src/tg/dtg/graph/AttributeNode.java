package tg.dtg.graph;

import tg.dtg.common.values.NumericValue;

public interface AttributeNode {

  void linkToEvent(NumericValue value, EventNode eventNode);

  default void linkFromEvent(EventNode eventNode) {
    eventNode.linkAttribute(this);
  }
}
