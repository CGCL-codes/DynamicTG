package tg.dtg.graph;

import java.util.List;
import tg.dtg.common.values.NumericValue;

public interface AttributeVertex extends Vertex {

  void linkToEvent(NumericValue value, EventVertex eventVertex);

  String shortString();

  List<String> edgeStrings();
}
