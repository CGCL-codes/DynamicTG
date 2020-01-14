package tg.dtg.graph.detect.traversal.anchors;

import static tg.dtg.util.Global.runAndSync;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import tg.dtg.cet.EventTrend;

public class AnchorGraph {

  private final AnchorVertex startVertex;
  private final AnchorVertex endVertex;
  private final Function<Collection<AnchorVertex>,
      HashSet<AnchorVertex>> selectAnchors;
  private HashSet<AnchorVertex> anchors;
  private final Consumer<EventTrend> outputFunc;

  public AnchorGraph(HashSet<AnchorVertex> anchors,
      AnchorVertex startVertex,
      AnchorVertex endVertex,
      Function<Collection<AnchorVertex>,
        HashSet<AnchorVertex>> selectAnchors,
      Consumer<EventTrend> outputFunc) {
    this.anchors = anchors;
    this.startVertex = startVertex;
    this.endVertex = endVertex;
    this.selectAnchors = selectAnchors;
    this.outputFunc = outputFunc;
  }

  public AnchorGraph(HashSet<AnchorVertex> anchors,
      AnchorVertex startVertex,
      AnchorVertex endVertex,
      Consumer<EventTrend> outputFunc) {
    this(anchors, startVertex, endVertex, null, outputFunc);
  }

  public void detectOnePredicate(int numIteration) {
    Preconditions.checkArgument(numIteration > 0);
    try {
      anchors.remove(endVertex); // avoid selecting end vertex
      while (numIteration > 0) {
        anchors.remove(startVertex); // start vertex is selected in default
        HashSet<AnchorVertex> newAnchors = selectAnchors.apply(anchors);
        newAnchors.add(startVertex);
        ArrayList<Runnable> tasks = new ArrayList<>(newAnchors.size());
        for (AnchorVertex vertex : newAnchors) {
          tasks.add(() -> doBFS(vertex, newAnchors));
        }
        runAndSync(tasks);
        //doBFS(startVertex,newAnchors);
        anchors = newAnchors;
        numIteration--;
      }
      anchors.add(endVertex);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void doBFS(AnchorVertex start, HashSet<AnchorVertex> isAnchors) {
//    ArrayListMultimap<TrendVertex, EventTrend> trends = ArrayListMultimap.create();
//    for (TrendVertex estart : start.getEdges()) {
//      trends.put(estart, estart.eventTrend);
//    }
    Collection<TrendVertex> vertices = start.getEdges();

    do {
      HashSet<TrendVertex> nextLayer = new HashSet<>();
      ArrayListMultimap<TrendVertex, EventTrend> nextTrends = ArrayListMultimap.create();
      HashSet<TrendVertex> newVertex = new HashSet<>();
      for (TrendVertex vertex : vertices) {
        ArrayList<AnchorVertex> edges = vertex.getEdges();
        HashSet<TrendVertex> outer = new HashSet<>();
        for (AnchorVertex anchor : edges) {
          if(anchor == this.endVertex){
            if(start == startVertex) {
              // from start to end, output
              outputFunc.accept(vertex.eventTrend);
            }else {
              //List<EventTrend> eventTrends = trends.get(vertex);
              newVertex.add(vertex);
            }
        } else if (!isAnchors.contains(anchor)) {
            // not anchor, traverse its trend vertices
            for (TrendVertex v : anchor.getEdges()) {
              if (v.eventTrend.start() > vertex.eventTrend.end()) {
                outer.add(v);
              }
            }
          } else {
            // anchor, make new trend vertices and connect with edges
            newVertex.add(vertex);
          }
        }
        for (TrendVertex tv: outer) {
          TrendVertex nv = vertex.copyAndAppend(tv);
          nextLayer.add(nv);
        }
        //nextLayer.addAll(outer);
      }
      start.newEdges(newVertex);
      vertices = nextLayer;
    } while (!vertices.isEmpty());
  }

  public void computeResults() {
    for(TrendVertex trendVertex: startVertex.getEdges()) {
      for(AnchorVertex anchorVertex: trendVertex.getEdges()) {
        if(anchorVertex == endVertex) {
          outputFunc.accept(trendVertex.eventTrend);
        }else {
          doDFS(anchorVertex, trendVertex.eventTrend);
        }
      }
    }
  }

  private void doDFS(AnchorVertex anchor, EventTrend trend) {
    for(TrendVertex trendVertex: anchor.getEdges()) {
      if(trendVertex.eventTrend.start() <= trend.end()) continue;
      ArrayList<AnchorVertex> edges = trendVertex.getEdges();
      int i = 0;
      for (; i < edges.size() - 1; i++) {
        AnchorVertex anchorVertex = edges.get(i);
        EventTrend eventTrend = trend.copy();
        eventTrend.append(trendVertex.eventTrend);
        if(anchorVertex == endVertex) {
          outputFunc.accept(eventTrend);
        }else {
          doDFS(anchorVertex, eventTrend);
        }
      }
      // avoid one copy
      if(i < edges.size()) {
        AnchorVertex anchorVertex = edges.get(i);
        EventTrend eventTrend = trend.copy();
        eventTrend.append(trendVertex.eventTrend);
        if (anchorVertex == endVertex) {
          outputFunc.accept(eventTrend);
        } else {
          doDFS(anchorVertex, eventTrend);
        }
      }
    }
  }

  private void doDFSParallel() {

  }

  private void output(List<EventTrend> eventTrends) {
    eventTrends.forEach(outputFunc);
  }
}
