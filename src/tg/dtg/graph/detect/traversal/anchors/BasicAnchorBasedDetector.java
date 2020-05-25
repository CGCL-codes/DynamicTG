package tg.dtg.graph.detect.traversal.anchors;

import static tg.dtg.util.Global.log;
import static tg.dtg.util.Global.runAndSync;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import tg.dtg.cet.EventTrend;
import tg.dtg.cet.SimpleEventTrend;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Predicate;
import tg.dtg.query.Query;
import tg.dtg.util.Global;

public class BasicAnchorBasedDetector extends AnchorBasedDetector {

  protected final int selectivity;
  private ConcurrentHashMap<AnchorVertex, Long> memoryCounts;

  public BasicAnchorBasedDetector(ArrayList<EventVertex> eventVertices,
      ArrayList<Constructor> constructors,
      Query query,
      int selectivity, int numIteration,
      String writePath) {
    super(eventVertices, constructors, query, numIteration, writePath);
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
    LinkedBlockingQueue<EventTrend> simpleEventTrends = new LinkedBlockingQueue<>();
    AtomicLong counter = new AtomicLong();
    if (writePath != null) {
      output = simpleEventTrends::offer;
    } else {
      output = (et) -> counter.incrementAndGet();
      //output = eventTrends::offer;
    }
    try {
      final HashMap<AttributeVertex, AnchorVertex> isAnchors = new HashMap<>();

      HashSet<AnchorVertex> anchorVertices = new HashSet<>(anchors.size() + 2);
      ArrayList<AnchorVertex> startVertices = new ArrayList<>();
      final int maxNumStarts = 1024;
      AnchorVertex endVertex = new AnchorVertex("end");

//      anchorVertices.add(startVertex);
      for (AttributeVertex vertex : anchors) {
        if (vertex.getEdges().isEmpty()) {
          continue;
        }
        final AnchorVertex anchorVertex = new AnchorVertex(vertex.shortString());
        anchorVertices.add(anchorVertex);
        isAnchors.put(vertex, anchorVertex);
      }
      //ConcurrentHashMap<Thread, LinkedBlockingQueue<Long>> taskTms = new ConcurrentHashMap<>();
      int mode = 1;
      if (mode == 1) {
        ArrayList<Runnable> tasks = new ArrayList<>(anchors.size() + 1);
        memoryCounts = new ConcurrentHashMap<>();

        int size = (int) (Math.ceil(starts.size() * 1.0 / 1024));
        Iterator<EventVertex> iterator = starts.iterator();

        for (int i = 0; i < maxNumStarts; i++) {
          if (!iterator.hasNext()) {
            break;
          }
          ArrayList<EventVertex> evs = new ArrayList<>(size);
          AnchorVertex subStart = new AnchorVertex("start-" + i);
          startVertices.add(subStart);
          for (int j = 0; j < size && iterator.hasNext(); j++) {
            evs.add(iterator.next());
          }
          tasks.add(() -> {
            long start = System.currentTimeMillis();
            doBFS(predicate.tag, subStart, evs, isAnchors, endVertex,
                output);
            long end = System.currentTimeMillis();
//          LinkedBlockingQueue<Long> queue = taskTms.get(Thread.currentThread());
//          if (queue == null) {
//            queue = new LinkedBlockingQueue<>();
//            taskTms.put(Thread.currentThread(), queue);
//          }
//          queue.offer(end - start);
          });
        }
        for (HashMap.Entry<AttributeVertex, AnchorVertex> entry : isAnchors.entrySet()) {
          tasks.add(() -> {
                long start = System.currentTimeMillis();
                doBFS(predicate.tag, entry.getValue(), entry.getKey().getEdges(), isAnchors,
                    endVertex,
                    output);
                long end = System.currentTimeMillis();
//                LinkedBlockingQueue<Long> queue = taskTms.get(Thread.currentThread());
//                if (queue == null) {
//                  queue = new LinkedBlockingQueue<>();
//                  taskTms.put(Thread.currentThread(), queue);
//                }
//                queue.offer(end - start);
              }
          );
        }
        runAndSync(tasks);
        System.out.println("memory costs " + memoryCounts.values().stream().reduce(0L, Long::sum));
      } else {
        /*ArrayBlockingQueue<BFSTask> tasks = new ArrayBlockingQueue<>(isAnchors.size() * 2);
        tasks.add(
            new BFSTask(predicate.tag, starts, isAnchors, endVertex, ends,
                starts));

        for (HashMap.Entry<AttributeVertex, AnchorVertex> entry : isAnchors.entrySet()) {
          tasks.add(
              new BFSTask(predicate.tag, entry.getValue(), entry.getKey().getEdges(), isAnchors,
                  endVertex, startVertex, ends, starts));
        }
         */
      }
      log("finish first iteration");
      anchorVertices.add(endVertex);
      AnchorGraph anchorGraph = new AnchorGraph(anchorVertices, startVertices, endVertex,
          this::selectAnchors, output);
      if (numIteration > 1) {
        anchorGraph.detectOnePredicate(numIteration - 1);
      }
      if (!Global.pdfs) {
        anchorGraph.computeResults();
      } else {
        anchorGraph.doParallelDFS();
      }
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
    long size;
    if (writePath != null) {
      size = simpleEventTrends.size();
      writeTrends(simpleEventTrends.iterator(), true);
    } else {
      size = counter.get();
    }
    System.out.println("cet number " + size);
    return simpleEventTrends;
  }

  private HashSet<AnchorVertex> selectAnchors(Collection<AnchorVertex> anchors) {
    HashSet<AnchorVertex> newAnchors = new HashSet<>();
    Iterator<AnchorVertex> iterator = anchors.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      if (i % selectivity == 0) {
        newAnchors.add(iterator.next());
      } else {
        iterator.next();
      }
      i++;
    }
    return newAnchors;
  }

  protected void doBFS(char c, AnchorVertex anchor, Collection<EventVertex> eventVertices,
      HashMap<AttributeVertex, AnchorVertex> isAnchors,
      AnchorVertex end,
      Consumer<EventTrend> outputFunc) {
//    if(!memoryCounts.containsKey(anchor)) memoryCounts.put(anchor,0L);
//    long mCount = memoryCounts.get(anchor);
    ArrayListMultimap<EventVertex, EventTrend> trends = ArrayListMultimap.create();
    HashSet<EventVertex> vertices = new HashSet<>();
    for (EventVertex vertex : eventVertices) {
//      if (starts.contains(vertex) && anchor.isStart()) {
//        continue;
//      }
      EventTrend trend = new SimpleEventTrend(vertex.event);
//      mCount += 1;
      if (ends.contains(vertex)) {
        if (anchor.isStart()) {
          outputFunc.accept(trend);
//          mCount -= trend.size();
        } else {
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
//                    mCount+=et.size() + 1;
                    if (anchor.isStart()) {
                      // from start to end, output
                      outputFunc.accept(trend);
//                      mCount -= trend.size();
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
//            mCount+=et.size() + 1;
            nextTrends.put(ev, trend);
          }
        }
        nextLayer.addAll(outer);
      }
      vertices = nextLayer;
      trends = nextTrends;
    } while (!vertices.isEmpty());
//    memoryCounts.put(anchor,mCount);
  }

  private static class BFSTask {

    private final HashSet<EventVertex> ends;
    private final HashSet<EventVertex> starts;
    char c;
    AnchorVertex anchor;
    Collection<EventVertex> eventVertices;
    HashMap<AttributeVertex, AnchorVertex> isAnchors;
    AnchorVertex end;
    AnchorVertex start;

    public BFSTask(char c, AnchorVertex anchor,
        Collection<EventVertex> eventVertices,
        HashMap<AttributeVertex, AnchorVertex> isAnchors,
        AnchorVertex end, AnchorVertex start,
        HashSet<EventVertex> ends, HashSet<EventVertex> starts) {
      this.c = c;
      this.anchor = anchor;
      this.eventVertices = eventVertices;
      this.isAnchors = isAnchors;
      this.end = end;
      this.start = start;
      this.ends = ends;
      this.starts = starts;
    }

    public void run() {
      HashMap<EventVertex, ArrayList<EventTrend>> trends = new HashMap<>();
      HashSet<EventVertex> vertices = new HashSet<>();

      for (EventVertex ev : eventVertices) {
        if (starts.contains(ev) && anchor != start) {
          continue;
        }
        ArrayList<EventTrend> ets = Lists.newArrayList(new SimpleEventTrend(ev.event));
        trends.put(ev, ets);
        vertices.add(ev);
      }
      doBFS(vertices, trends);
    }

    private void doBFS(Collection<EventVertex> evs,
        HashMap<EventVertex, ArrayList<EventTrend>> trends) {

      HashSet<EventVertex> nextLayer = new HashSet<>();
      HashMap<EventVertex, ArrayList<EventTrend>> nextTrends = new HashMap<>();

      for (EventVertex ev : evs) {
        ArrayList<EventTrend> etrends = trends.get(ev);

        if (ends.contains(ev)) {
          // reach end anchor
          for (EventTrend trend : etrends) {
            TrendVertex vertex = new TrendVertex(trend);
            this.anchor.addEdge(vertex);
            vertex.addEdge(end);
          }
          continue;
        }

        HashSet<EventVertex> localNextLayer = new HashSet<>();

        for (AttributeVertex av : ev.getEdges().get(c)) {
          if (isAnchors.containsKey(av)) {
            // reach an anchor
            AnchorVertex anchorVertex = isAnchors.get(av);
            for (EventTrend trend : etrends) {
              TrendVertex vertex = new TrendVertex(trend.copy());
              this.anchor.addEdge(vertex);
              vertex.addEdge(anchorVertex);
            }
          } else {
            // not anchor
            for (EventVertex nev : av.getEdges()) {
              if (nev.timestamp() > ev.timestamp()) {
                localNextLayer.add(nev);
              }
            }
          }
        }
        if (localNextLayer.size() > 0) {
          Iterator<EventVertex> evit = localNextLayer.iterator();
          EventVertex remain = evit.next();
          while (evit.hasNext()) {
            EventVertex lev = evit.next();
            ArrayList<EventTrend> ntrends = nextTrends
                .computeIfAbsent(lev, k -> new ArrayList<>(etrends.size()));
            for (EventTrend trend : etrends) {
              EventTrend et = trend.copy();
              et.append(lev.event);
              ntrends.add(et);
            }
          }
          ArrayList<EventTrend> ntrends = nextTrends
              .computeIfAbsent(remain, k -> new ArrayList<>(etrends.size()));
          for (EventTrend trend : etrends) {
            trend.append(remain.event);
            ntrends.add(trend);
          }
        }
        nextLayer.addAll(localNextLayer);
      }
      if (!nextLayer.isEmpty()) {
        doBFS(nextLayer, nextTrends);
      }
    }
  }

}
