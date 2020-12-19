package tg.dtg.events;

import java.util.Arrays;

import tg.dtg.common.values.Value;

public class Event {
  public final long timestamp;
  protected Value[] values;

  public Event(long timestamp, Value[] values) {
    this.timestamp = timestamp;
    this.values = values;
  }

  public Value get(int i) {
    return values[i];
  }

  public int size() {return values.length;}

  @Override
  public String toString() {
    return "Event{" + timestamp
        + ", " + Arrays.toString(values)
        + '}';
  }
}
