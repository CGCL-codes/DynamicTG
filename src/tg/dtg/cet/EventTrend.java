package tg.dtg.cet;

import java.util.List;
import tg.dtg.events.Event;

public abstract class EventTrend {

  public abstract List<Event> events();

  public abstract void append(Event event);
  public abstract void append(EventTrend eventTrend);

  public abstract Event get(int index);

  public Event head() {
    return get(0);
  }

  public Event tail() {
    return get(size()-1);
  }

  public abstract int size();

  public abstract EventTrend copy();

  public long start() {
    return head().timestamp;
  }

  public long end() {
    return tail().timestamp;
  }

  public abstract String shortString();

  @Override
  public String toString() {
    return "EventTrend{"+shortString()+"}";
  }
}
