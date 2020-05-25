package tg.dtg.graph.detect.traversal.anchors;

import java.util.ArrayList;
import java.util.List;
import tg.dtg.cet.EventTrend;

public class TrendVertex implements Vertex {
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

  public TrendVertex copyAndAppend(TrendVertex next) {
    EventTrend eventTrend = this.eventTrend.copy();
    eventTrend.append(next.eventTrend);
    return new TrendVertex(eventTrend, next.vertices);
  }

  public ArrayList<AnchorVertex> getEdges() {
    return vertices;
  }

  @Override
  public List<? extends Vertex> cedges() {
    return getEdges();
  }

  public String shortString() {
    return eventTrend.shortString();
  }

  @Override
  public String toString() {
    return eventTrend.shortString();
  }
}
