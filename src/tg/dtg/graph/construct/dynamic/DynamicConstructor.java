package tg.dtg.graph.construct.dynamic;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.Stack;
import javax.annotation.Nonnull;

import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.events.Event;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Operator;
import tg.dtg.query.Predicate;
import tg.dtg.util.Tuple;

public class DynamicConstructor extends Constructor {

  protected final SkipListDynamicAttributeVertex head;
  protected final Index[] iheads;
  protected final int maxLevel;

  private final int[] randGaps;
  private final Random random;

  private final NumericValue step;

  protected int curLevel;

  /**
   * dynamic constructor by skip list.
   *
   * @param start    start of value range
   * @param end      end of value range
   * @param step     precision
   * @param maxLevel max level of the skip list
   */
  public DynamicConstructor(Predicate predicate,
                            NumericValue start, NumericValue end, NumericValue step,
                            int maxLevel) {
    super(predicate);
    this.step = step;
    SkipListDynamicAttributeVertex node = new SkipListDynamicAttributeVertex(start, end);
    head = new SkipListDynamicAttributeVertex(null, null);
    head.next = node;
    this.maxLevel = maxLevel;
    iheads = new Index[maxLevel];
    randGaps = new int[maxLevel];

    for (int i = 0; i < maxLevel; i++) {
      randGaps[maxLevel - i - 1] = 1 << i;
    }
    random = new Random();
    curLevel = 0;
  }

  public DynamicConstructor(Predicate predicate,
                            NumericValue start, NumericValue end, NumericValue step) {
    this(predicate, start, end, step, 12);
  }

  private Tuple<Stack<Index>, SkipListDynamicAttributeVertex> find(NumericValue value) {
    Stack<Index> path = new Stack<>();
    if (curLevel == 0) {
      // in the list
      return Tuple.of(path, findInList(head.next, null, value));
    } else {
      Index prev = iheads[curLevel - 1];
      Index index = prev.next;

      while (true) {
        if (index == null || index.node.start.compareTo(value) >= 0) {
          if (prev.down != null) {
            path.push(prev);
            prev = prev.down;
            index = prev.next;
          } else {
            SkipListDynamicAttributeVertex start = prev.node;
            SkipListDynamicAttributeVertex end = null;
            if (start == head) {
              start = start.next;
            }
            if (index != null) {
              end = index.node;
            }
            return Tuple.of(path, findInList(start, end, value));
          }
        } else {
          prev = index;
          index = index.next;
        }
      }
    }
  }

  private SkipListDynamicAttributeVertex findInList(SkipListDynamicAttributeVertex start,
                                                    SkipListDynamicAttributeVertex end,
                                                    NumericValue value) {
    Preconditions.checkArgument(start != head && start.start.compareTo(value) <= 0
            && (end == null || value.compareTo(end.start) < 0),
        "value must be in the range [%s, %s)", start, end);
    SkipListDynamicAttributeVertex prev = start;
    SkipListDynamicAttributeVertex node = prev.next;
    while (node != end) {
      if (node.start.compareTo(value) > 0) {
        return prev;
      }
      prev = node;
      node = prev.next;
    }
    return prev;
  }

  protected SkipListDynamicAttributeVertex splitNode(
      Tuple<Stack<Index>, SkipListDynamicAttributeVertex> pathAndNode,
      NumericValue gap, int flag) {
    // link the node into the list
    NumericValue tgap = (NumericValue) Value.numeric(gap.numericVal() + flag * step.numericVal());
    SkipListDynamicAttributeVertex node = pathAndNode.right;
    SkipListDynamicAttributeVertex nnode = node.split(tgap);
    nnode.next = node.next;
    node.next = nnode;

    int rnd = random.nextInt(randGaps[0]);
    Stack<Index> path = pathAndNode.left;
    int l = 1;
    Index dindex = null;
    while (rnd < randGaps[l]) {
      Index index = new Index();
      index.node = nnode;
      Index pindex;
      if (!path.empty()) {
        pindex = path.pop();
      } else {
        iheads[l - 1] = new Index();
        if (l > 1) {
          iheads[l - 1].down = iheads[l - 2];
        }
        pindex = iheads[l - 1];
      }
      index.next = pindex.next;
      pindex.next = index;
      index.down = dindex;
      dindex = index;
      l++;
    }
    return node;
  }

  @Override
  public void link(EventVertex eventVertex) {
    Event event = eventVertex.event;
    NumericValue tv = (NumericValue) predicate.func.apply(event.get(predicate.rightOperand));
    Iterator<SkipListDynamicAttributeVertex> tonodes;
    NumericValue fv = (NumericValue) event.get(predicate.leftOperand);
    SkipListDynamicAttributeVertex fromNode = find(fv).right;

    switch (predicate.op) {
      case eq:
        tonodes = Collections.singletonList(find(tv).right).iterator();
        break;
      case gt:
        Tuple<Stack<Index>, SkipListDynamicAttributeVertex> result = find(tv);
        SkipListDynamicAttributeVertex node = splitNode(result, tv, 1);
        tonodes = node.next.iterator();
        break;
      default:
        tonodes = Collections.emptyIterator();
        break;
    }

    fromNode.linkToEvent(fv, eventVertex);
//    tonodes.forEachRemainin;
  }

  @Override
  public void manage() {

  }

  @Override
  public Iterator<AttributeVertex> attributes() {
    return new Iterator<AttributeVertex>() {
      SkipListDynamicAttributeVertex tail = head.next;

      @Override
      public boolean hasNext() {
        return tail != null;
      }

      @Override
      public AttributeVertex next() {
        AttributeVertex node = tail;
        tail = tail.next;
        return node;
      }
    };
  }

  private static class SkipListDynamicAttributeVertex extends
      DynamicAttributeVertex implements
      Iterable<SkipListDynamicAttributeVertex> {

    SkipListDynamicAttributeVertex next;

    public SkipListDynamicAttributeVertex(NumericValue start, NumericValue end) {
      this(start, end, null);
    }

    protected SkipListDynamicAttributeVertex(NumericValue start, NumericValue end, OuterEdge edge) {
      super(start, end, edge);
    }


    @Override
    @Nonnull
    public Iterator<SkipListDynamicAttributeVertex> iterator() {
      return new DListIterator(this);
    }

    public SkipListDynamicAttributeVertex split(NumericValue gap) {
      OuterEdge edge = head.next;
      OuterEdge prev = head;
      while (edge != null && edge.value.compareTo(gap) < 0) {
        prev = edge;
        edge = edge.next;
      }
      prev.next = null;
      NumericValue oend = end;
      this.end = gap;
      return new SkipListDynamicAttributeVertex(gap, oend, edge);
    }

    private static class DListIterator implements Iterator<SkipListDynamicAttributeVertex> {

      private SkipListDynamicAttributeVertex cur;

      public DListIterator(SkipListDynamicAttributeVertex cur) {
        this.cur = cur;
      }

      @Override
      public boolean hasNext() {
        return cur != null;
      }

      @Override
      public SkipListDynamicAttributeVertex next() {
        SkipListDynamicAttributeVertex node = cur;
        cur = cur.next;
        return node;
      }
    }

    @Override
    public String toString() {
      return "DynamicAttributeNode"
          + "[" + start
          + ", " + end + ')';
    }
  }

  private static class Index {

    SkipListDynamicAttributeVertex node;
    Index next;
    Index down;
  }

  /**
   * for debug, show all attribute vertices in the constructor.
   *
   * @param constructor the constructor
   * @return the string of attribute vertices
   */
  public static String dynamicAttributeNode(DynamicConstructor constructor) {
    SkipListDynamicAttributeVertex head = constructor.head;
    StringBuilder builder = new StringBuilder();
    builder.append("O");
    head = head.next;
    while (head != null) {
      builder.append("->").append(head);
      head = head.next;
    }
    return builder.toString();
  }
}
