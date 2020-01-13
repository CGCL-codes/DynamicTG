package tg.dtg.graph.construct.dynamic.parallel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.dynamic.RangeAttributeVertex;
import tg.dtg.query.Operator;
import tg.dtg.query.Predicate;
import tg.dtg.util.Global;
import tg.dtg.util.MergedIterator;

public class ParallelManager {

  private final ExecutorService executor;

  public ParallelManager(ExecutorService executor) {
    this.executor = executor;
  }

  public ArrayList<AttributeVertex> mergeGaps(ArrayList<Iterator<NumericValue>> its,
      NumericValue start, NumericValue end, NumericValue step,
      Operator operator) throws ExecutionException, InterruptedException {
    Preconditions.checkArgument(its.size() > 0);
    int mid = its.size() / 2;
    return mergeGaps(its, 0, mid).thenCombineAsync(mergeGaps(its, mid, its.size()),
        (it1, it2) -> merge2range(it1, it2, start, end, step, operator),
        executor).get();
  }

  private CompletableFuture<Iterator<NumericValue>> mergeGaps(ArrayList<Iterator<NumericValue>> its,
      int start,
      int end) {
    if (start == end) {
      return CompletableFuture.completedFuture(Collections.emptyIterator());
    } else if (start + 1 == end) {
      return CompletableFuture.completedFuture(its.get(start));
    } else {
      int mid = (start + end) / 2;
      return mergeGaps(its, start, mid).thenCombineAsync(mergeGaps(its, mid, end), (it1, it2) ->
          merge(it1, it2).iterator(), executor
      );
    }
  }

  private LinkedList<NumericValue> merge(Iterator<NumericValue> left,
      Iterator<NumericValue> right) {
    Comparator<NumericValue> cmp = Global.numericValueComparator();
    LinkedList<NumericValue> list = new LinkedList<>();
    Iterator<NumericValue> it = new DistinctMergeIterator<>(left, right, cmp);
    Iterators.addAll(list, it);
    return list;
  }

  private ArrayList<AttributeVertex> merge2range(Iterator<NumericValue> left,
      Iterator<NumericValue> right,
      NumericValue start,
      NumericValue end,
      NumericValue step,
      Operator operator) {
    Comparator<NumericValue> cmp = Global.numericValueComparator();
    Iterator<NumericValue> it = new DistinctMergeIterator<>(left, right, cmp);
    ArrayList<AttributeVertex> ranges = new ArrayList<>();
    Range<NumericValue> range;
    NumericValue lower = start;
    NumericValue gap;
    while (it.hasNext()) {
      gap = it.next();
      switch (operator) {
        case gt:
          NumericValue upper = (NumericValue) Value.numeric(
              gap.numericVal() + step.numericVal());
          range = Range.closedOpen(lower, upper);
          lower = upper;
          ranges.add(new RangeAttributeVertex(range));
          break;
        default:
          break;
      }
    }
    switch (operator) {
      case gt:
        range = Range.closedOpen(lower, end);
        ranges.add(new RangeAttributeVertex(range));
        break;
      default:
        break;
    }
    return ranges;
  }

  public int reduceFromEdges(ArrayList<Iterator<TupleEdge<NumericValue, EventVertex, Object>>> its,
      ArrayList<AttributeVertex> vertices)
      throws ExecutionException, InterruptedException {
    // partition by the index of vertex
    ArrayList<Future<ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>>>> futures
        = new ArrayList<>(its.size());
    for (Iterator<TupleEdge<NumericValue, EventVertex, Object>> it : its) {
      futures.add(executor.submit(new PartitionByInt(vertices, it)));
    }
    ArrayList<ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>>> multimaps
        = new ArrayList<>(its.size());
    for (Future<ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>>> future : futures) {
      multimaps.add(future.get());
    }

    ArrayList<ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>>> partitions = new ArrayList<>();
    for (int i = 0; i < vertices.size(); i++) {
      partitions.add(new ArrayList<>());
    }
    for (ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>> multimap : multimaps) {
      for (int j = 0; j < multimap.size(); j++) {
        partitions.get(j).add(multimap.get(j));
      }
    }
    // link events
    ArrayList<Set<Integer>> keys = new ArrayList<>();
    for (int i = 0; i < vertices.size(); i++) {
      keys.add(new HashSet<>());
    }
    for (int i = 0; i < partitions.size(); ) {
      for (int j = 0; j < vertices.size(); j++) {
        keys.get(j).add(i);
        i++;
      }
    }
    ArrayList<Future<Integer>> nfutures = new ArrayList<>();
    for (int i = 0; i < vertices.size(); i++) {
      nfutures.add(executor.submit(new ReduceToVertices(partitions, vertices, keys.get(i))));
    }
    int count = 0;
    for (Future<Integer> future : nfutures) {
      count += future.get();
    }
    return count;
  }

  public int reduceToEdges(ArrayList<Iterator<TupleEdge<EventVertex, NumericValue, Object>>> its,
      Predicate predicate, ArrayList<AttributeVertex> vertices)
      throws ExecutionException, InterruptedException {
    ArrayList<Future<Integer>> futures = new ArrayList<>();
    for (Iterator<TupleEdge<EventVertex, NumericValue, Object>> it : its) {
      futures.add(executor.submit(new CopyToEdge(it, vertices, predicate)));
    }
    int count = 0;
    for (Future<Integer> future : futures) {
      count += future.get();
    }
    return count;
  }

  private static class DistinctMergeIterator<T> implements Iterator<T> {

    private T cur = null;
    private Iterator<T> raw;
    private Comparator<T> cmp;

    private DistinctMergeIterator(Iterator<T> left, Iterator<T> right, Comparator<T> cmp) {
      raw = new MergedIterator<>(Arrays.asList(left, right), cmp);
      this.cmp = cmp;
      if (raw.hasNext()) {
        cur = raw.next();
      }
    }

    @Override
    public boolean hasNext() {
      return cur != null;
    }

    @Override
    public T next() {
      T old = cur;
      T temp = null;
      boolean isBreak = false;
      while (raw.hasNext()) {
        temp = raw.next();
        if (cmp.compare(temp, old) != 0) {
          isBreak = true;
          break;
        }
      }
      if (isBreak) {
        cur = temp;
      } else {
        cur = null;
      }
      return old;
    }
  }

  private static class ReduceToVertices implements Callable<Integer> {

    private final ArrayList<ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>>> items;
    private final ArrayList<AttributeVertex> vertices;
    private final Set<Integer> keys;

    private ReduceToVertices(
        ArrayList<ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>>> items,
        ArrayList<AttributeVertex> vertices, Set<Integer> keys) {
      this.items = items;
      this.vertices = vertices;
      this.keys = keys;
    }

    @Override
    public Integer call() {
      int count = 0;
      for (Integer key : keys) {
        ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>> edges = items.get(key);
        RangeAttributeVertex vertex = (RangeAttributeVertex)(vertices.get(key));
        for (List<TupleEdge<NumericValue, EventVertex, Object>> edgeList : edges) {
          for (TupleEdge<NumericValue, EventVertex, Object> edge : edgeList) {
            vertex.linkToEvent(edge.getTarget());
            count++;
          }
        }
      }
      return count;
    }
  }

  private static class PartitionByInt implements
      Callable<ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>>> {

    private final ArrayList<AttributeVertex> vertices;
    private final Iterator<TupleEdge<NumericValue, EventVertex, Object>> edges;

    private PartitionByInt(
        ArrayList<AttributeVertex> vertices,
        Iterator<TupleEdge<NumericValue, EventVertex, Object>> edges) {
      this.vertices = vertices;
      this.edges = edges;
    }

    @Override
    public ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>> call() {
      int i = 0;
      ArrayList<List<TupleEdge<NumericValue, EventVertex, Object>>> multimap
          = Lists.newArrayList();
      for (int j = 0; j < vertices.size(); j++) {
        multimap.add(new LinkedList<>());
      }
      while (edges.hasNext()) {
        TupleEdge<NumericValue, EventVertex, Object> edge = edges.next();
        while (!((RangeAttributeVertex)vertices.get(i)).getRange().contains(edge.getSource())) {
          i++;
        }
        multimap.get(i).add(edge);
      }
      return multimap;
    }
  }

  private static class CopyToEdge implements Callable<Integer> {

    private final Iterator<TupleEdge<EventVertex, NumericValue, Object>> iterator;
    private final ArrayList<AttributeVertex> vertices;
    private final Predicate predicate;

    private CopyToEdge(
        Iterator<TupleEdge<EventVertex, NumericValue, Object>> iterator,
        ArrayList<AttributeVertex> vertices, Predicate predicate) {
      this.iterator = iterator;
      this.vertices = vertices;
      this.predicate = predicate;
    }

    @Override
    public Integer call() {
      int i = 0;
      int count = 0;
      while (iterator.hasNext()) {
        TupleEdge<EventVertex, NumericValue, Object> edge = iterator.next();
        while (!((RangeAttributeVertex)vertices.get(i)).getRange().contains(edge.getTarget())) {
          i++;
        }
        switch (predicate.op) {
          case gt:
            for (int j = i + 1; j < vertices.size(); j++) {
              edge.getSource().linkToAttr(predicate.tag, vertices.get(j));
              count++;
            }
            break;
          case eq:
            edge.getSource().linkToAttr(predicate.tag, vertices.get(i));
            break;
          default:
            break;
        }
      }
      return count;
    }
  }
}
