package tg.dtg.graph.construct.dynamic;

import java.util.ArrayList;
import java.util.List;
import tg.dtg.common.values.Value;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;

public class ValueAttributeVertex implements AttributeVertex {
  protected final ArrayList<EventVertex> vertices;
  protected final Value value;

  public ValueAttributeVertex(Value value) {
    this.value = value;
    vertices = new ArrayList<>();
  }


  @Override
  public List<EventVertex> getEdges() {
    return vertices;
  }

  @Override
  public void linkToEvent(EventVertex eventVertex) {
    vertices.add(eventVertex);
  }

  @Override
  public String shortString() {
    return ""+value;
  }
}
