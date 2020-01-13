package tg.dtg.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import tg.dtg.events.Event;

public class EventVertex implements Vertex {
  public final Event event;

  private final Map<Character, ArrayList<AttributeVertex>> edges;

  public EventVertex(Event event) {
    this.event = event;
    edges = new HashMap<>();
  }

  public <T extends AttributeVertex> void linkToAttr(char c, T vertex) {
    ArrayList<AttributeVertex> egs;
    if (!edges.containsKey(c)) {
      egs = new ArrayList<>();
      edges.put(c, egs);
    } else {
      egs = edges.get(c);
    }
    egs.add(vertex);
  }

  public long timestamp() {
    return event.timestamp;
  }

  @Override
  public String toString() {
    return "EventVertex{"
        + "event=" + event
        + '}';
  }

  public Map<Character, ArrayList<AttributeVertex>> getEdges() {
    return edges;
  }

  /**
   * for debug, show all edges in string.
   * @return edges in string
   */
  public List<String> edgeStrings() {
    List<String> strings = new ArrayList<>();
    for(Entry<Character, ArrayList<AttributeVertex>> entry: edges.entrySet()) {
      char c = entry.getKey();
      for(AttributeVertex vertex: entry.getValue()) {
        strings.add(c + " " + this.shortString() + "->" + vertex.toString());
      }
    }
    return strings;
  }

  public String shortString() {
    return event.timestamp + "";
  }
}
