package tg.dtg.graph;

import java.util.ArrayList;
import java.util.List;
import tg.dtg.events.Event;

public class EventNode {
    public final Event event;
    protected List<AttributeNode> edges;

    public EventNode(Event event) {
      this.event = event;
      edges = new ArrayList<>();
    }

    public void linkAttribute(AttributeNode node) {
      edges.add(node);
    }
}
