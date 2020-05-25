package tg.dtg.graph.detect.traversal.anchors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AnchorVertex implements Vertex{

  private final String tag;
  private ArrayList<TrendVertex> vertices;

  public AnchorVertex(String tag) {
    this.tag = tag;
    vertices = new ArrayList<>();
  }

  public List<TrendVertex> getEdges() {
    return vertices;
  }

  public void addEdge(TrendVertex vertex) {
    vertices.add(vertex);
  }

  public void newEdges(Collection<TrendVertex> vertices) {
    this.vertices = new ArrayList<>();
    this.vertices.addAll(vertices);
  }

  public void mergeInto(ArrayList<AnchorVertex> vertices) {
    int size = 0;
    for(AnchorVertex anchorVertex:vertices) {
      size += anchorVertex.getEdges().size();
    }
    this.vertices = new ArrayList<>(size);
    for(AnchorVertex anchorVertex:vertices) {
      this.vertices.addAll(anchorVertex.getEdges());
    }
  }

  public boolean isStart(){
    return tag.startsWith("start");
  }

  public boolean isEnd() {
    return tag.startsWith("end");
  }

  public String shortString() {
    return tag;
  }

  public List<String> edgeStrings() {
    List<String> strings = new ArrayList<>();
    for (TrendVertex vertex : vertices) {
      strings.add(this.shortString() + "->" + vertex.shortString());
    }
    return strings;
  }

  @Override
  public String toString() {
    return "AnchorVertex{" + tag + '}';
  }

  @Override
  public List<? extends Vertex> cedges() {
    return getEdges();
  }
}
