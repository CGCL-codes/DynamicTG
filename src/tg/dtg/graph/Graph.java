package tg.dtg.graph;

import static tg.dtg.util.Global.getParallism;
import static tg.dtg.util.Global.log;

import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.graph.construct.dynamic.parallel.ParallelDynamicConstructor;
import tg.dtg.query.Query;

public class Graph {

  protected final EventTemplate eventTemplate;
  protected final Query query;
  protected final ArrayList<EventVertex> eventVertices;
  protected List<Event> events;
  protected ArrayList<Constructor> constructors;

  /**
   * create a new graph.
   *
   * @param events input events
   * @param eventTemplate event template
   * @param query query
   * @param constructors constructors for graph construction
   */
  public Graph(List<Event> events,
      EventTemplate eventTemplate, Query query,
      ArrayList<Constructor> constructors) {
    this.events = events;
    this.eventTemplate = eventTemplate;
    this.query = query;
    this.constructors = constructors;
    eventVertices = new ArrayList<>(events.size());
  }

  /**
   * construct the graph.
   */
  public void construct() {
    processInputStream();
    manageGraph();
  }

  protected void processInputStream() {
    for(Event event: events) {
      eventVertices.add(new EventVertex(event));
    }
    ArrayList<Constructor> parallels = new ArrayList<>();
    ArrayList<Constructor> sequentials = new ArrayList<>();
    for(Constructor constructor:constructors) {
      if(constructor instanceof ParallelDynamicConstructor) parallels.add(constructor);
      else sequentials.add(constructor);
    }
    log("begin graph construction, events " + events.size());
    if(!sequentials.isEmpty()) {
      for (EventVertex eventVertex : eventVertices) {
        for (Constructor constructor : sequentials) {
          constructor.link(eventVertex);
        }
      }
    }
    if(!parallels.isEmpty()) {
      int parallism = getParallism();
      for(Constructor constructor: parallels) {
        ArrayList<Iterator<EventVertex>> iterators = new ArrayList<>();
        for (int i = 0; i < parallism; i++) {
          iterators.add(new ParallelInputInterator(eventVertices,i,parallism));
        }
        constructor.parallelLink(iterators);
      }
    }
    for (Constructor constructor : constructors) {
      constructor.invokeEventsEnd();
    }
    log("finish stream");
  }

  protected void manageGraph() {
    for (Constructor constructor : constructors) {
      constructor.manage();
    }
    log("finish manage");
    System.out.println("events: " + eventVertices.size() + "\n"
        + "attrs: " + constructors.stream().mapToInt(Constructor::countAttr).reduce(Integer::sum)
        .getAsInt() + "\n"
        + "from edges: " + constructors.stream().mapToInt(Constructor::countFrom)
        .reduce(Integer::sum).getAsInt() + "\n"
        + "to edges: " + constructors.stream().mapToInt(Constructor::countTo).reduce(Integer::sum)
        .getAsInt()
    );
  }

  /**
   * for debug.
   *
   * @return attribute vertices
   */
  public ArrayList<AttributeVertex> attributes() {
    ArrayList<AttributeVertex> nodes = new ArrayList<>();

    Iterator<AttributeVertex> it = Iterators.concat(
        constructors.stream().map(Constructor::attributes).iterator()
    );
    Iterators.addAll(nodes, it);
    return nodes;
  }

  public ArrayList<EventVertex> events() {
    return eventVertices;
  }

  private static class ParallelInputInterator implements Iterator<EventVertex> {
    private final ArrayList<EventVertex> source;
    private  int index;
    private final int step;

    private ParallelInputInterator(ArrayList<EventVertex> source, int startIndex, int step) {
      this.source = source;
      this.index = startIndex;
      this.step = step;
    }

    @Override
    public boolean hasNext() {
      return index < source.size();
    }

    @Override
    public EventVertex next() {
      EventVertex event = source.get(index);
      index += step;
      return event;
    }
  }
}
