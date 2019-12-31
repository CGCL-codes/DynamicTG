package tg.dtg.graph;

public abstract class AbstractEdge<S, T, V> implements Edge<S, T, V> {
  protected final S src;
  protected final T dest;
  protected final V value;

  public AbstractEdge(S src, T dest, V value) {
    this.src = src;
    this.dest = dest;
    this.value = value;
  }

  public AbstractEdge(S src, T dest) {
    this(src, dest, null);
  }

  @Override
  public S getSource() {
    return src;
  }

  @Override
  public T getTarget() {
    return dest;
  }

  @Override
  public V getValue() {
    return value;
  }
}
