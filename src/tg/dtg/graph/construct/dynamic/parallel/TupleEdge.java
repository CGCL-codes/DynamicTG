package tg.dtg.graph.construct.dynamic.parallel;

import tg.dtg.graph.AbstractEdge;

public class TupleEdge<S, T, V> extends AbstractEdge<S, T, V> {
  public TupleEdge(S src, T dest, V value) {
    super(src, dest, value);
  }

  public TupleEdge(S src, T dest) {
    super(src, dest);
  }
}
