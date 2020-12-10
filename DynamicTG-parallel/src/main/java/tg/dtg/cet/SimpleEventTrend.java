package tg.dtg.cet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import tg.dtg.events.Event;

public class SimpleEventTrend extends EventTrend{
  private ArrayList<Event> events;

  public SimpleEventTrend() {
    this(new ArrayList<>());
  }

  public SimpleEventTrend(Event event) {
    this();
    events.add(event);
  }

  private SimpleEventTrend(ArrayList<Event> events) {
    this.events = events;
  }

  @Override
  public List<Event> events() {
    return events;
  }

  public void append(Event event) {
    events.add(event);
  }

  public void append(EventTrend trend) {
    events.addAll(trend.events());
  }

  @Override
  public Event get(int index) {
    return events.get(index);
  }

  @Override
  public int size() {
    return events.size();
  }

  public SimpleEventTrend copy() {
    ArrayList<Event> events = new ArrayList<>(this.events);
    return new SimpleEventTrend(events);
  }

  public String shortString() {
    return "[" + events.stream()
        .map(e->e.timestamp+"")
        .collect(Collectors.joining(", ")) + "]";
  }

  @Override
  public String toString() {
    return "EventTrend{"+shortString()+"}";
  }
}
