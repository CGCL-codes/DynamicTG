package tg.dtg.graph.construct;

import java.util.Iterator;
import tg.dtg.common.values.Value;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.Edge;
import tg.dtg.graph.EventVertex;
import tg.dtg.query.Operator;
import tg.dtg.query.Predicate;

public abstract class Constructor {
  protected final Predicate predicate;

  protected Constructor(Predicate predicate) {
    this.predicate = predicate;
  }

  public abstract void link(EventVertex eventVertex);

  public abstract void manage();

  public abstract Iterator<AttributeVertex> attributes();
}
