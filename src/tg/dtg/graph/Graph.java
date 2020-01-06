package tg.dtg.graph;

import static tg.dtg.util.Global.log;

import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.construct.Constructor;
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
    log("begin graph construction, events " + events.size());
    for (Event event : events) {
      EventVertex eventVertex = new EventVertex(event);
      eventVertices.add(eventVertex);
      for (Constructor constructor : constructors) {
        constructor.link(eventVertex);
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
}
