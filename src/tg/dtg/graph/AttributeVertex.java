package tg.dtg.graph;

import java.util.List;

public interface AttributeVertex extends Vertex {

  List<EventVertex> getEdges();

  String shortString();

  List<String> edgeStrings();
}
