package tg.dtg.graph.construct.dynamic;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Iterator;
import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.graph.AttributeNode;
import tg.dtg.graph.EventNode;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Operator;

public class DynamicConstructor2 extends Constructor {

  protected final TreeDynamicAttributeNode root;
  protected final TreeDynamicAttributeNode head;
  private final NumericValue step;

  public DynamicConstructor2(NumericValue start, NumericValue end, NumericValue step) {
    root = new TreeDynamicAttributeNode(start, end);
    head = new TreeDynamicAttributeNode(null, null);
    head.next = root;
    this.step = step;
  }

  @Override
  public void link(Value from, Value to, EventNode eventNode, Operator operator) {
    Preconditions.checkArgument(from instanceof NumericValue && to instanceof NumericValue,
        "dynamic constructor only support numeric attribute value");

    NumericValue tv = (NumericValue) to;
    Iterator<TreeDynamicAttributeNode> tonodes;
    NumericValue fv = (NumericValue) from;
    TreeDynamicAttributeNode fromNode = find(fv);

    switch (operator) {
      case eq:
        tonodes = Collections.singletonList(find(tv)).iterator();
        break;
      case gt:
        TreeDynamicAttributeNode result = find(tv);
        NumericValue value = (NumericValue) Value.numeric(tv.numericVal() + step.numericVal());
        TreeDynamicAttributeNode node = result.split(value);
        tonodes = node.next.upper();
        break;
      default:
        tonodes = Collections.emptyIterator();
        break;
    }

    fromNode.linkToEvent(fv, eventNode);
    tonodes.forEachRemaining(eventNode::linkAttribute);
  }

  private TreeDynamicAttributeNode find(NumericValue value) {
    return find(root, value);
  }

  private TreeDynamicAttributeNode find(TreeDynamicAttributeNode node, NumericValue value) {
    if (node.split == null) {
      return node;
    } else {
      if (value.compareTo(node.split) < 0) {
        return find(node.left, value);
      } else {
        return find(node.right, value);
      }
    }
  }

  @Override
  public Iterator<AttributeNode> attributes() {
    return new Iterator<AttributeNode>() {
      TreeDynamicAttributeNode node = head.next;

      @Override
      public boolean hasNext() {
        return node != null;
      }

      @Override
      public AttributeNode next() {
        TreeDynamicAttributeNode nn = node;
        node = node.next;
        return nn;
      }
    };
  }

  private static class TreeDynamicAttributeNode extends DynamicAttributeNode {

    NumericValue start, end;
    TreeDynamicAttributeNode left, right;
    TreeDynamicAttributeNode next, prev;
    NumericValue split;

    TreeDynamicAttributeNode(NumericValue start, NumericValue end) {
      super(start, end, null);
      this.start = start;
      this.end = end;
      left = right = null;
      next = prev = null;
    }

    private TreeDynamicAttributeNode split(NumericValue value) {
      left = new TreeDynamicAttributeNode(start, value);
      right = new TreeDynamicAttributeNode(value, end);
      split = value;
      left.next = right;
      right.next = next;
      prev.next = left;
      return left;
    }

    Iterator<TreeDynamicAttributeNode> upper() {
      return new ListIterator();
    }

    static class ListIterator implements
        Iterator<TreeDynamicAttributeNode> {

      private TreeDynamicAttributeNode cur;

      @Override
      public boolean hasNext() {
        return cur != null;
      }

      @Override
      public TreeDynamicAttributeNode next() {
        TreeDynamicAttributeNode node = cur;
        cur = cur.next;
        return node;
      }
    }
  }
}
