package tg.dtg.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import tg.dtg.events.Event;

public class EventVertex implements Vertex {
  public final Event event;

  private final Map<Character, ArrayList<AttributeVertex>> edges;

  public EventVertex(Event event) {
    this.event = event;
    edges = new HashMap<>();
  }

  public void linkToAttr(char c, AttributeVertex vertex) {
    ArrayList<AttributeVertex> egs;
    if(!edges.containsKey(c)){
      egs = new ArrayList<>();
      edges.put(c,egs);
    }
    else egs = edges.get(c);
    egs.add(vertex);
  }

  @Override
  public String toString() {
    return "EventVertex{" +
        "event=" + event +
        '}';
  }
}
