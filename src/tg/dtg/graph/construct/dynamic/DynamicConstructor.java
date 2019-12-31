package tg.dtg.graph.construct.dynamic;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.Stack;
import javax.annotation.Nonnull;
import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.graph.AttributeNode;
import tg.dtg.graph.EventNode;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Operator;
import tg.dtg.util.Tuple;

public class DynamicConstructor extends Constructor {

  protected final SkipListDynamicAttributeNode head;
  protected final Index[] iheads;
  protected final int MAX_LEVEL;

  private final int[] randGaps;
  private final Random random;

  private final NumericValue step;

  protected int maxLevel;

  public DynamicConstructor(NumericValue start, NumericValue end, NumericValue step,
      int MAX_LEVEL) {
    this.step = step;
    SkipListDynamicAttributeNode node = new SkipListDynamicAttributeNode(start, end);
    head = new SkipListDynamicAttributeNode(null, null);
    head.next = node;
    this.MAX_LEVEL = MAX_LEVEL;
    iheads = new Index[MAX_LEVEL];
    randGaps = new int[MAX_LEVEL];

    for (int i = 0; i < MAX_LEVEL; i++) {
      randGaps[MAX_LEVEL - i - 1] = 1 << i;
    }
    random = new Random();
    maxLevel = 0;
  }

  public DynamicConstructor(NumericValue start, NumericValue end, NumericValue step) {
    this(start, end, step, 12);
  }

  /*private DynamicAttributeNode find(NumericValue value) {
    if(maxLevel == 0) {
      // in the list
      return findInList(head.next,null,value);
    }else {
      Index prev = iheads[maxLevel - 1];
      Index index = prev.next;

      while (true) {
        if(index == null || index.node.start.compareTo(value) >= 0) {
          if(prev.down != null) {
            prev = prev.down;
            index = prev.next;
          } else {
            DynamicAttributeNode start = prev.node;
            DynamicAttributeNode end = null;
            if(start == head) start = start.next;
            if(index != null) end = index.node;
            return findInList(start, end, value);
          }
        }else {
          prev = index;
          index = index.next;
        }
      }
    }
  }*/

  private Tuple<Stack<Index>, SkipListDynamicAttributeNode> find(NumericValue value) {
    Stack<Index> path = new Stack<>();
    if (maxLevel == 0) {
      // in the list
      return Tuple.of(path, findInList(head.next, null, value));
    } else {
      Index prev = iheads[maxLevel - 1];
      Index index = prev.next;

      while (true) {
        if (index == null || index.node.start.compareTo(value) >= 0) {
          if (prev.down != null) {
            path.push(prev);
            prev = prev.down;
            index = prev.next;
          } else {
            SkipListDynamicAttributeNode start = prev.node;
            SkipListDynamicAttributeNode end = null;
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

  private SkipListDynamicAttributeNode findInList(SkipListDynamicAttributeNode start, SkipListDynamicAttributeNode end,
      NumericValue value) {
    Preconditions.checkArgument(start != head && start.start.compareTo(value) <= 0 &&
            (end == null || value.compareTo(end.start) < 0),
        "value must be in the range [%s, %s)", start, end);
    SkipListDynamicAttributeNode prev = start;
    SkipListDynamicAttributeNode node = prev.next;
    while (node != end) {
      if (node.start.compareTo(value) > 0) {
        return prev;
      }
      prev = node;
      node = prev.next;
    }
    return prev;
  }

  protected SkipListDynamicAttributeNode splitNode(Tuple<Stack<Index>, SkipListDynamicAttributeNode> pathAndNode,
      NumericValue gap, int flag) {
    // link the node into the list
    NumericValue tgap = (NumericValue) Value.numeric(gap.numericVal() + flag * step.numericVal());
    SkipListDynamicAttributeNode node = pathAndNode._2;
    SkipListDynamicAttributeNode nnode = node.split(tgap);
    nnode.next = node.next;
    node.next = nnode;

    int rnd = random.nextInt(randGaps[0]);
    Stack<Index> path = pathAndNode._1;
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
  public void link(Value from, Value to, EventNode eventNode, Operator operator) {
    Preconditions.checkArgument(from instanceof NumericValue && to instanceof NumericValue,
        "dynamic constructor only support numeric attribute value");

    NumericValue tv = (NumericValue) to;
    Iterator<SkipListDynamicAttributeNode> tonodes;
    NumericValue fv = (NumericValue) from;
    SkipListDynamicAttributeNode fromNode = find(fv)._2;

    switch (operator) {
      case eq:
        tonodes = Collections.singletonList(find(tv)._2).iterator();
        break;
      case gt:
        Tuple<Stack<Index>, SkipListDynamicAttributeNode> result = find(tv);
        SkipListDynamicAttributeNode node = splitNode(result, tv, 1);
        tonodes = node.next.iterator();
        break;
      default:
        tonodes = Collections.emptyIterator();
        break;
    }

    fromNode.linkToEvent(fv, eventNode);
    tonodes.forEachRemaining(eventNode::linkAttribute);
  }

  @Override
  public Iterator<AttributeNode> attributes() {
    return new Iterator<AttributeNode>() {
      SkipListDynamicAttributeNode tail = head.next;
      @Override
      public boolean hasNext() {
        return tail != null;
      }

      @Override
      public AttributeNode next() {
        AttributeNode node =tail;
        tail = tail.next;
        return node;
      }
    };
  }

  private static class SkipListDynamicAttributeNode extends
      DynamicAttributeNode implements
      Iterable<SkipListDynamicAttributeNode> {

    SkipListDynamicAttributeNode next;

    public SkipListDynamicAttributeNode(NumericValue start, NumericValue end) {
      this(start, end, null);
    }

    protected SkipListDynamicAttributeNode(NumericValue start, NumericValue end, OuterEdge edge) {
      super(start,end,edge);
    }



      @Override
    @Nonnull
    public Iterator<SkipListDynamicAttributeNode> iterator() {
      return new DListIterator(this);
    }

    public SkipListDynamicAttributeNode split(NumericValue gap) {
      OuterEdge edge = head.next;
      OuterEdge prev = head;
      while (edge != null && edge.value.compareTo(gap) < 0) {
        prev = edge;
        edge = edge.next;
      }
      prev.next = null;
      NumericValue oend = end;
      this.end = gap;
      return new SkipListDynamicAttributeNode(gap, oend, edge);
    }

    private static class DListIterator implements Iterator<SkipListDynamicAttributeNode> {

      private SkipListDynamicAttributeNode cur;

      public DListIterator(SkipListDynamicAttributeNode cur) {
        this.cur = cur;
      }

      @Override
      public boolean hasNext() {
        return cur != null;
      }

      @Override
      public SkipListDynamicAttributeNode next() {
        SkipListDynamicAttributeNode node = cur;
        cur = cur.next;
        return node;
      }
    }

    @Override
    public String toString() {
      return "DynamicAttributeNode" +
          "[" + start +
          ", " + end + ')';
    }
  }

  private static class Index {

    SkipListDynamicAttributeNode node;
    Index next;
    Index down;
  }



  public static String dynamicAttributeNode(DynamicConstructor constructor){
    SkipListDynamicAttributeNode head = constructor.head;
    StringBuilder builder = new StringBuilder();
    builder.append("O");
    head=head.next;
    while (head!=null) {
      builder.append("->").append(head);
      head=head.next;
    }
    return builder.toString();
  }
}
