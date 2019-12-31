package tg.dtg.main;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import tg.dtg.events.Event;
import tg.dtg.graph.Graph;
import tg.dtg.main.examples.Example;
import tg.dtg.main.examples.Stock;
import tg.dtg.query.Query;

public class Main {

  public static void main(String[] args) {
    Preconditions.checkArgument(args.length > 1);
    String[] nargs = Arrays.copyOfRange(args, 1, args.length);

    Example example = null;
    if ("stock".equals(args[0])) {
      example = new Stock(nargs);
    }else {
      System.err.println("not a given case.");
      System.exit(0);
    }

    ArrayList<Window> windowed_events = new ArrayList<>();
    Iterator<Event> it = example.readInput();
    Query query = example.getQuery();
    long nextW = 0;
    while (it.hasNext()) {
      Event event = it.next();
      while (event.timestamp >= nextW) {
        Window window = new Window(nextW);
        windowed_events.add(window);
        nextW = nextW + query.sl;
      }
      for (Window window : windowed_events) {
        if (window.start <= event.timestamp && event.timestamp < window.start + query.wl) {
          window.events.add(event);
        }
      }
    }

    Graph graph = new Graph(windowed_events.get(0).events,example.getTemplate(),example.getQuery(), example.getConstructors());
    graph.construct();
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
