package tg.dtg.graph;

import tg.dtg.events.Event;

public class EventVertex implements Vertex {
  public final Event event;

  public EventVertex(Event event) {
    this.event = event;
  }
}
