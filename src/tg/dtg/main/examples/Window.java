package tg.dtg.main.examples;

import java.util.ArrayList;
import tg.dtg.events.Event;

public class Window {
  final long start;
  final ArrayList<Event> events;

  public Window(long start) {
    this.start = start;
    events = new ArrayList<>();
  }
}
