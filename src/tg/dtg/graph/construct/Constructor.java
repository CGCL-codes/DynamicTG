package tg.dtg.graph.construct;

import java.util.Iterator;
import tg.dtg.common.values.Value;
import tg.dtg.graph.AttributeNode;
import tg.dtg.graph.EventNode;
import tg.dtg.query.Operator;

public abstract class Constructor {
  public abstract void link(Value from, Value to, EventNode eventNode, Operator operator);

  public abstract Iterator<AttributeNode> attributes();
}
