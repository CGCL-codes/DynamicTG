package tg.dtg.graph.detect.traversal.anchors;

import java.util.ArrayList;
import tg.dtg.cet.EventTrend;
import tg.dtg.graph.EventVertex;

public class TrendVertex {
  final EventTrend eventTrend;
  private ArrayList<AnchorVertex> vertices;

  public TrendVertex(EventTrend eventTrend) {
    this.eventTrend = eventTrend;
    vertices = new ArrayList<>();
  }

  private TrendVertex(EventTrend eventTrend, ArrayList<AnchorVertex> edges) {
    this.eventTrend = eventTrend;
    vertices = edges;
  }

  public void addEdge(AnchorVertex attributeVertex) {
    vertices.add(attributeVertex);
  }

  public void prepend(TrendVertex prev) {
    eventTrend.prepend(prev.eventTrend);
  }

  public TrendVertex copyAndAppend(TrendVertex next) {
    EventTrend eventTrend = this.eventTrend.copy();
    eventTrend.append(next.eventTrend);
    return new TrendVertex(eventTrend, next.vertices);
  }

  public void addEvent(EventVertex vertex) {
    eventTrend.append(vertex.event);
  }

  public ArrayList<AnchorVertex> getEdges() {
    return vertices;
  }

  public String shortString() {
    return eventTrend.shortString();
  }

  @Override
  public String toString() {
    return eventTrend.shortString();
  }
}
