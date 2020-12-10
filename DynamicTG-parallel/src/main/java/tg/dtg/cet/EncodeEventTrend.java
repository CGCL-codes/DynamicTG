package tg.dtg.cet;

import java.util.ArrayList;
import java.util.List;
import tg.dtg.events.Event;

public class EncodeEventTrend extends EventTrend{

  private final EncodeEventTrend previous;
  private List<Event> events;
  private int size;
  private EncodeEventTrend head;

  public EncodeEventTrend() {
    this(null);
    head = this;
  }

  public EncodeEventTrend(EncodeEventTrend previous) {
    this.previous = previous;
  }

  @Override
  public List<Event> events() {
    ArrayList<Event> events = new ArrayList<>(size);
    EncodeEventTrend cur = this;
    for (int i = size-1; i >= 0 ; ) {
      int j = i - cur.events.size() + 1;
      for(Event e:cur.events) {
        events.set(j,e);
        j++;
      }
      i = i - cur.events.size();
      cur = this.previous;
    }
    return events;
  }

  @Override
  public void append(Event event) {
    events.add(event);
    size += 1;
  }

  @Override
  public void append(EventTrend eventTrend) {
    List<Event> events = eventTrend.events();
    this.events.addAll(events);
    size += events.size();
  }

  @Override
  public Event get(int index) {
    EncodeEventTrend cur = this;
    int pos = size - 1;
    int x;
    while ((x = pos - cur.events.size()) >= index) {
      pos = x;
      cur = cur.previous;
    }
    return cur.events.get(cur.events.size()-1);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Event head() {
    return head.events.get(0);
  }

  @Override
  public Event tail() {
    return events.get(events.size()-1);
  }

  @Override
  public EventTrend copy() {
    return null;
  }

  @Override
  public String shortString() {
    return "";
  }
}
