package tg.dtg.graph.construct.dynamic.parallel;

import static tg.dtg.util.Global.log;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import tg.dtg.common.values.Value;
import tg.dtg.events.Event;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.graph.construct.dynamic.ValueAttributeVertex;
import tg.dtg.query.Predicate;
import tg.dtg.util.Global;
import tg.dtg.util.Iters;
import tg.dtg.util.Tuple;

public class ParallelStaticDynamicEqConstructor extends Constructor {

  private final EqEventProcessor[] processors;
  private final HashMap<Value, ValueAttributeVertex> attrs;
  private Future<?>[] futures;

  public ParallelStaticDynamicEqConstructor(Predicate predicate) {
    super(predicate);
    processors = new EqEventProcessor[Global.getParallism()];
    futures = new Future[Global.getParallism()];

    attrs = new HashMap<>();
  }

  @Override
  public void parallelLink(ArrayList<Iterator<EventVertex>> iterators) {
    ExecutorService executor = Global.getExecutor();
    for (int i = 0; i < iterators.size(); i++) {
      processors[i] = new EqEventProcessor(iterators.get(i), predicate);
    }
    for (int i = 0; i < processors.length; i++) {
      futures[i] = executor.submit(processors[i]);
    }
  }

  @Override
  public void manage() {
    log("mange attrs");
    HashSet<Value> attrVals = new HashSet<>();
    for (EqEventProcessor processor : processors) {
      attrVals.addAll(processor.getAttrs());
    }
    for (Value v : attrVals) {
      attrs.put(v, new ValueAttributeVertex(v));
    }
    log("manage from edges");
    //from edges
    ArrayList<Value> vals = new ArrayList<>(attrVals);
    List<Runnable> tasks = new ArrayList<>();
    int parallism = Global.getParallism();
    for (int i = 0; i < parallism; i++) {
      final Iterator<Integer> indice = Iters.stepIndices(i, parallism, vals.size());
      tasks.add(() -> {
        while (indice.hasNext()) {
          Value v = vals.get(indice.next());
          AttributeVertex av = attrs.get(v);

          for (EqEventProcessor processor : processors) {
            Collection<EventVertex> col = processor.getFroms().get(v);
            if (col != null) {
              for (EventVertex ev : col) {
                av.linkToEvent(ev);
              }
            }
          }
        }
      });
    }
    try {
      Global.runAndSync(tasks);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }

    log("manage to edges");
    // to edges
    tasks.clear();
    tasks = Arrays.stream(processors)
        .map(processor -> new Runnable() {
          @Override
          public void run() {
            ArrayList<Tuple<EventVertex, Value>> tos = processor.getTos();
            for (Tuple<EventVertex, Value> edge : tos) {
              edge.left.linkToAttr(predicate.tag, attrs.get(edge.right));
            }
          }
        }).collect(Collectors.toList());
    try {
      Global.runAndSync(tasks);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void invokeEventsEnd() {
    try {
      for (int i = 0; i < processors.length; i++) {
        futures[i].get();
        countF += processors[i].getCountF();
        countT += processors[i].getCountT();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  private ArrayList<AttributeVertex> reserved_attrs = null;

  @Override
  public ArrayList<AttributeVertex> attributes() {
    if(reserved_attrs == null) {
      TreeMap<Value,AttributeVertex> values = new TreeMap<>(
          Ordering.natural().onResultOf(Value::hashCode)
      );
      values.putAll(attrs);
      reserved_attrs = new ArrayList<>(values.values());
    }
    return reserved_attrs;
  }

  @Override
  public int countAttr() {
    return attrs.size();
  }

  private static class EqEventProcessor implements Runnable {

    private final Iterator<EventVertex> source;
    private final Predicate predicate;
    private int countF;
    private int countT;
    private Multimap<Value, EventVertex> froms;
    private ArrayList<Tuple<EventVertex, Value>> tos;
    private HashSet<Value> attrs;

    public EqEventProcessor(Iterator<EventVertex> source, Predicate predicate) {
      this.source = source;
      this.predicate = predicate;
      countF = countT = 0;
      froms = HashMultimap.create();
      tos = new ArrayList<>();
      attrs = new HashSet<>();
    }

    @Override
    public void run() {
      while (source.hasNext()) {
        EventVertex vertex = source.next();
        Event event = vertex.event;
        Value tv = predicate.func.apply(event.get(predicate.rightOperand));
        Value fv = event.get(predicate.leftOperand);

        froms.put(fv, vertex);
        tos.add(Tuple.of(vertex, tv));
        attrs.add(tv);
        attrs.add(fv);
        countF++;
        countT++;
      }
    }

    public HashSet<Value> getAttrs() {
      return attrs;
    }

    public Multimap<Value, EventVertex> getFroms() {
      return froms;
    }

    public ArrayList<Tuple<EventVertex, Value>> getTos() {
      return tos;
    }

    public int getCountT() {
      return countT;
    }

    public int getCountF() {
      return countF;
    }
  }
}
