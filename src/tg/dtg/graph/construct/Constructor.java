package tg.dtg.graph.construct;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.query.Predicate;

public abstract class Constructor {
  protected final Predicate predicate;

  protected Constructor(Predicate predicate) {
    this.predicate = predicate;
  }

  public abstract void link(EventVertex eventVertex);

  public void invokeEventsEnd(){}

  public abstract void manage();

  public abstract Iterator<AttributeVertex> attributes();

  public void close() { }
}
