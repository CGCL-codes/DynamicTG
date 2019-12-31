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
  protected final ArrayList<EventNode> eventNodes;

  public Graph(List<Event> events,
      EventTemplate eventTemplate, Query query,
      ArrayList<Constructor> constructors) {
    this.events = events;
    this.eventTemplate = eventTemplate;
    this.query = query;
    predicates = query.condition.predicates();
    this.constructors = constructors;
    eventNodes = new ArrayList<>(events.size());
  }

  public void construct() {
    for(Event event: events){
      EventNode eventNode = new EventNode(event);
      eventNodes.add(eventNode);

      for (int i = 0; i < predicates.size(); i++) {
        Predicate predicate = predicates.get(i);
        Constructor constructor = constructors.get(i);
        Value from = event.get(predicate.leftOperand);
        Value to = predicate.func.apply(event.get(predicate.rightOperand));
        constructor.link(from, to, eventNode, predicate.op);
      }
    }
  }

  public ArrayList<AttributeNode> attributes() {
    ArrayList<AttributeNode> nodes = new ArrayList<>();

    Iterator<AttributeNode> it = Iterators.concat(constructors.stream().map(Constructor::attributes).iterator());
    Iterators.addAll(nodes, it);
    return nodes;
  }

  public ArrayList<EventNode> events() {
    return eventNodes;
  }
}
