package tg.dtg.main;

import java.util.ArrayList;
import java.util.Iterator;
import tg.dtg.events.Event;
import tg.dtg.graph.Graph;
import tg.dtg.main.examples.Example;
import tg.dtg.query.Query;
import tg.dtg.util.Parallel;

public class Main {

  public static void main(String[] args) {
    Example example = Example.getExample(args);

    ArrayList<Window> windowedEvents = new ArrayList<>();
    Iterator<Event> it = example.readInput();
    Query query = example.getQuery();
    long nextW = 0;
    while (it.hasNext()) {
      Event event = it.next();
      while (event.timestamp >= nextW) {
        Window window = new Window(nextW);
        windowedEvents.add(window);
        nextW = nextW + query.sl;
      }
      for (Window window : windowedEvents) {
        if (window.start <= event.timestamp && event.timestamp < window.start + query.wl) {
          window.events.add(event);
        }
      }
    }

    Graph graph = new Graph(windowedEvents.get(0).events, example.getTemplate(),
        example.getQuery(), example.getConstructors());
    graph.construct();

    Parallel.getInstance().close();
  }

  static class Window {

    final long start;
    final ArrayList<Event> events;

    public Window(long start) {
      this.start = start;
      events = new ArrayList<>();
    }
  }
}
