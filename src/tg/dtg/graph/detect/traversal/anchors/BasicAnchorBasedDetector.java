package tg.dtg.graph.detect.traversal.anchors;

import static tg.dtg.util.Global.log;
import static tg.dtg.util.Global.runAndSync;

import com.google.common.collect.ArrayListMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import tg.dtg.cet.EventTrend;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Predicate;
import tg.dtg.query.Query;

public class BasicAnchorBasedDetector extends AnchorBasedDetector {
  protected final int selectivity;

  public BasicAnchorBasedDetector(ArrayList<EventVertex> eventVertices,
      ArrayList<Constructor> constructors,
      Query query,
      int selectivity, int numIteration,
      boolean isWrite) {
    super(eventVertices, constructors, query, numIteration, isWrite);
    this.selectivity = selectivity;
  }

  @Override
  protected Map<Character, Collection<AttributeVertex>> selectAnchors() {
    Map<Character, Collection<AttributeVertex>> anchors = new HashMap<>();
    for (Entry<Character, ArrayList<AttributeVertex>> entry : p2attrVertices.entrySet()) {
      ArrayList<AttributeVertex> vertices = entry.getValue();
      ArrayList<AttributeVertex> list = new ArrayList<>(vertices.size() / selectivity);
      for (int i = selectivity - 1; i < vertices.size(); i += selectivity) {
        list.add(vertices.get(i));
      }
      anchors.put(entry.getKey(), list);
    }
    return anchors;
  }

  @Override
  protected Collection<EventTrend> detectOnePredicate(Collection<AttributeVertex> anchors,
      Predicate predicate) {
    Consumer<EventTrend> output;
    LinkedBlockingQueue<EventTrend> eventTrends = new LinkedBlockingQueue<>();
    AtomicLong counter = new AtomicLong();
    if(isWrite) {
      output = eventTrends::offer;
    }else {
      output = (et)->counter.incrementAndGet();
      //output = eventTrends::offer;
    }
    try {
      final HashMap<AttributeVertex, AnchorVertex> isAnchors = new HashMap<>();

      ArrayList<Runnable> tasks = new ArrayList<>(anchors.size() + 1);
      HashSet<AnchorVertex> anchorVertices = new HashSet<>(anchors.size() + 2);
      AnchorVertex startVertex = new AnchorVertex("start");
      AnchorVertex endVertex = new AnchorVertex("end");
      anchorVertices.add(startVertex);
      tasks.add(() -> doBFS(predicate.tag, startVertex, starts, isAnchors, startVertex, endVertex,
          output));
      for (AttributeVertex vertex : anchors) {
        if(vertex.getEdges().isEmpty()) continue;
        final AnchorVertex anchorVertex = new AnchorVertex(vertex.shortString());
        anchorVertices.add(anchorVertex);
        isAnchors.put(vertex, anchorVertex);
        tasks.add(() ->
            doBFS(predicate.tag, anchorVertex, vertex.getEdges(), isAnchors, startVertex, endVertex,
                output)
        );
      }
      runAndSync(tasks);
      log("finish first iteration");
      anchorVertices.add(endVertex);
      AnchorGraph anchorGraph = new AnchorGraph(anchorVertices, startVertex, endVertex,
          this::selectAnchors, output);
      if (numIteration > 1) {
        anchorGraph.detectOnePredicate(numIteration - 1);
      }
      anchorGraph.computeResults();
      System.out.println(counter.get());
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
    return eventTrends;
  }

  private HashSet<AnchorVertex> selectAnchors(Collection<AnchorVertex> anchors) {
    HashSet<AnchorVertex> newAnchors = new HashSet<>();
    Iterator<AnchorVertex> iterator = anchors.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      if(i % selectivity == 0) {
        newAnchors.add(iterator.next());
      }else iterator.next();
      i++;
    }
    return newAnchors;
  }

  protected void doBFS(char c, AnchorVertex anchor, Collection<EventVertex> eventVertices,
      HashMap<AttributeVertex, AnchorVertex> isAnchors,
      AnchorVertex start, AnchorVertex end,
      Consumer<EventTrend> outputFunc) {
    ArrayListMultimap<EventVertex, EventTrend> trends = ArrayListMultimap.create();
    HashSet<EventVertex> vertices = new HashSet<>();
    for (EventVertex vertex : eventVertices) {
      if(starts.contains(vertex) && anchor != start) {
        continue;
      }
      EventTrend trend = new EventTrend(vertex.event);
      if (ends.contains(vertex)) {
        if(anchor == start) {
          outputFunc.accept(trend);
        }else {
          TrendVertex tv = new TrendVertex(trend);
          anchor.addEdge(tv);
          tv.addEdge(end);
        }
      } else {
        trends.put(vertex, trend);
        vertices.add(vertex);
      }
    }
    do {
      HashSet<EventVertex> nextLayer = new HashSet<>();
      ArrayListMultimap<EventVertex, EventTrend> nextTrends = ArrayListMultimap.create();
      for (EventVertex vertex : vertices) {
        ArrayList<AttributeVertex> edges = vertex.getEdges().get(c);
        HashSet<EventVertex> outer = new HashSet<>();
        ArrayList<AttributeVertex> outAnchors = new ArrayList<>();
        for (AttributeVertex attr : edges) {
          if (!isAnchors.containsKey(attr)) {
            for (EventVertex v : attr.getEdges()) {
              if (v.timestamp() > vertex.timestamp()) {
                if (!ends.contains(v)) {
                  outer.add(v);
                } else {
                  // reach end vertex, link to end anchor
                  List<EventTrend> eventTrends = trends.get(vertex);
                  for (EventTrend et : eventTrends) {
                    // duplicate storage because copy
                    EventTrend trend = et.copy();
                    trend.append(v.event);
                    if (start == anchor) {
                      // from start to end, output
                      outputFunc.accept(trend);
                    } else {
                      TrendVertex trendVertex = new TrendVertex(trend);
                      anchor.addEdge(trendVertex);
                      trendVertex.addEdge(end);
                    }
                  }
                }
              }
            }
          } else {
            // ready to set edges
            outAnchors.add(attr);
          }
        }
        List<EventTrend> eventTrends = trends.get(vertex);

        for (EventTrend et : eventTrends) {
          TrendVertex trendVertex = new TrendVertex(et);
          anchor.addEdge(trendVertex);
          for (AttributeVertex attr : outAnchors) {
            trendVertex.addEdge(isAnchors.get(attr));
          }
        }

        for (EventTrend et : eventTrends) {
          for (EventVertex ev : outer) {
            // duplicate storage because copy
            EventTrend trend = et.copy();
            trend.append(ev.event);
            nextTrends.put(ev, trend);
          }
        }
        nextLayer.addAll(outer);
      }
      vertices = nextLayer;
      trends = nextTrends;
    } while (!vertices.isEmpty());
  }

}
