package tg.dtg.cet;

import java.util.ArrayList;
import java.util.stream.Collectors;
import tg.dtg.events.Event;

public class EventTrend {
  private ArrayList<Event> events;

  public EventTrend() {
    this(new ArrayList<>());
  }

  public EventTrend(Event event) {
    this();
    events.add(event);
  }

  private EventTrend(ArrayList<Event> events) {
    this.events = events;
  }

  public void prepend(EventTrend trend) {
    ArrayList<Event> events = new ArrayList<>(trend.events);
    events.addAll(this.events);
    this.events = events;
  }

  public void append(Event event) {
    events.add(event);
  }

  public void append(EventTrend trend) {
    events.addAll(trend.events);
  }

  public long start() {
    return events.get(0).timestamp;
  }

  public long end() {
    return events.get(events.size() - 1).timestamp;
  }

  public EventTrend copy() {
    ArrayList<Event> events = new ArrayList<>(this.events);
    return new EventTrend(events);
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
