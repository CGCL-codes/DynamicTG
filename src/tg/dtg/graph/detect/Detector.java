package tg.dtg.graph.detect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import tg.dtg.cet.EventTrend;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Query;

public abstract class Detector {
  protected final ArrayList<EventVertex> eventVertices;
  protected final HashMap<Character, ArrayList<AttributeVertex>> p2attrVertices;
  protected final Query query;

  protected HashSet<EventVertex> starts;
  protected HashSet<EventVertex> ends;
  protected Consumer<EventTrend> outputFunc;
  protected ArrayList<EventTrend> outputs = new ArrayList<>();

  protected boolean isWrite;

  public Detector(ArrayList<EventVertex> eventVertices,
      ArrayList<Constructor> constructors, Query query, boolean isWrite) {
    this.eventVertices = eventVertices;
    this.query = query;
    p2attrVertices = new HashMap<>();
    for(Constructor constructor:constructors) {
      p2attrVertices.put(constructor.getPredicate().tag, constructor.attributes());
    }
    this.isWrite = isWrite;
    if(isWrite) {
      outputFunc = outputs::add;
    }else {
      outputFunc = et->{};
    }
  }

  public abstract void detect();

  protected void prefilter() {
    starts = new HashSet<>(eventVertices);
    ends = new HashSet<>(eventVertices);

    // forward one step
    for(EventVertex vertex: eventVertices) {
      Map<Character, ArrayList<AttributeVertex>> p2edegs = vertex.getEdges();
      Map<Character, HashSet<EventVertex>> p2results = new HashMap<>();
      for(Entry<Character, ArrayList<AttributeVertex>> entry: p2edegs.entrySet()) {
        ArrayList<AttributeVertex> edges = entry.getValue();
        HashSet<EventVertex> outers = new HashSet<>();
        for(AttributeVertex attr: edges) {
          for(EventVertex v: attr.getEdges()) {
            if(v.timestamp() > vertex.timestamp()) outers.add(v);
          }
        }
        p2results.put(entry.getKey(), outers);
      }
      HashSet<EventVertex> results = DetectUtil.syncByQuery(p2results,query);
      if(!results.isEmpty()) {
        starts.removeAll(results);
        ends.remove(vertex);
      }
    }
  }

  /**
   * prefilter do not need to traverse one step, to judge an end vertex, we only need to
   * check if it has a follower. Similarly, to judge an start vertex, we only need to
   * judge if it has a preview vertex. Vertices match by the edges and time. So if we set
   * the time metrics in attr vertices. We don't need to match further.
   */
  protected void fastPrefilter() {
    HashMap<Character, HashSet<EventVertex>> p2starts = new HashMap<>();
    HashMap<Character, HashSet<EventVertex>> p2ends = new HashMap<>();
    for(char c: p2attrVertices.keySet()) {
      HashSet<EventVertex> starts = new HashSet<>(eventVertices);
      HashSet<EventVertex> ends = new HashSet<>(eventVertices);
      HashMap<AttributeVertex, long[]> metas = new HashMap<>();

      // forward one step
      for (EventVertex vertex : eventVertices) {
        ArrayList<AttributeVertex> edges = vertex.getEdges().get(c);
        for (AttributeVertex attr : edges) {
          long[] meta = metas.get(attr);
          if (meta == null) {
            // have cal min and max, compare
            long max = 0;
            for (EventVertex ev : attr.getEdges()) {
              if (ev.timestamp() > max)
                max = ev.timestamp();
            }
            meta = new long[]{max, max + 1};
            metas.put(attr, meta);
          }

          if (vertex.timestamp() < meta[0])
            ends.remove(vertex);
          if (vertex.timestamp() < meta[1])
            meta[1] = vertex.timestamp();
        }
      }
      for (Entry<AttributeVertex, long[]> entry : metas.entrySet()) {
        long[] meta = entry.getValue();
        if (meta[1] < meta[0]) {
          for (EventVertex ev : entry.getKey().getEdges()) {
            if (ev.timestamp() > meta[1])
              starts.remove(ev);
          }
        }
      }
      p2starts.put(c,starts);
      p2ends.put(c,ends);
    }
    starts = DetectUtil.syncByQuery(p2starts, query);
    ends = DetectUtil.syncByQuery(p2starts, query);
  }

}
