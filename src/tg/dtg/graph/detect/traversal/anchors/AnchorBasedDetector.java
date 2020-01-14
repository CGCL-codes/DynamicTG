package tg.dtg.graph.detect.traversal.anchors;

import static tg.dtg.util.Global.log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import tg.dtg.cet.EventTrend;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.graph.detect.Detector;
import tg.dtg.query.Predicate;
import tg.dtg.query.Query;

public abstract class AnchorBasedDetector extends Detector {
  protected final int numIteration;
  public AnchorBasedDetector(ArrayList<EventVertex> eventVertices,
      ArrayList<Constructor> constructors,
      Query query,
      int numIteration, boolean isWrite) {
    super(eventVertices, constructors, query, isWrite);
    this.numIteration = numIteration;
    if(isWrite) {
      LinkedBlockingQueue<EventTrend> outputs = new LinkedBlockingQueue<>();
      outputFunc = outputs::offer;
    }
  }

  protected abstract Map<Character,Collection<AttributeVertex>> selectAnchors();

  @Override
  public void detect() {
    log("begin detect");
    Map<Character,Collection<AttributeVertex>> anchors = selectAnchors();
    log("finish select anchors");
    prefilter();
    log("finish prefilter");
    detectByAnchors(anchors);
    log("finish detect");
  }

  protected void detectByAnchors(Map<Character,Collection<AttributeVertex>> anchors) {
    HashMap<Character, Collection<EventTrend>> results = new HashMap<>();
    for(Entry<Character, Collection<AttributeVertex>> entry: anchors.entrySet()) {
      Predicate predicate = query.condition.predicates().get(entry.getKey());
      results.put(entry.getKey(), detectOnePredicate(
          entry.getValue(),
          predicate
      ));
    }
    Collection<EventTrend> result = mergeByQuery(results);
  }

  protected abstract Collection<EventTrend> detectOnePredicate(Collection<AttributeVertex> anchors,
      Predicate predicate);

  private Collection<EventTrend> mergeByQuery(HashMap<Character, Collection<EventTrend>> results) {
    return results.values().iterator().next();
  }

}
