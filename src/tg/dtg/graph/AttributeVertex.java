package tg.dtg.graph;

import java.util.ArrayList;
import java.util.List;

public interface AttributeVertex extends Vertex {

  List<EventVertex> getEdges();

  void linkToEvent(EventVertex eventVertex);

  String shortString();

  default List<String> edgeStrings() {
    List<String> strings = new ArrayList<>();
    for (EventVertex vertex : getEdges()) {
      strings.add(this.shortString() + "->" + vertex.shortString());
    }
    return strings;
  }
}
