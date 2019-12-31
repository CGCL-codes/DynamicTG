package tg.dtg.graph;

import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import tg.dtg.common.values.Value;
import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Predicate;
import tg.dtg.query.Query;

public class Graph {

  protected final EventTemplate eventTemplate;
  protected final Query query;

  protected List<Event> events;

  protected ArrayList<Predicate> predicates;
  protected ArrayList<Constructor> constructors;
  protected final ArrayList<EventVertex> eventVertices;

  /**
   * create a new graph.
   *
   * @param events        input events
   * @param eventTemplate event template
   * @param query         query
   * @param constructors  constructors for graph construction
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
    for (Event event : events) {
      EventVertex eventVertex = new EventVertex(event);
      eventVertices.add(eventVertex);

      for (int i = 0; i < predicates.size(); i++) {
        Predicate predicate = predicates.get(i);
        Constructor constructor = constructors.get(i);
        constructor.link(eventVertex);
      }
    }
  }

  protected void manageGraph() {
    for (Constructor constructor : constructors) {
      constructor.manage();
    }
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
