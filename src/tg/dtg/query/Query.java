package tg.dtg.query;

public class Query {
  public final String types;
  public final Expression condition;
  public final long wl;
  public final long sl;

  public Query(String types, Expression condition, long wl, long sl) {
    this.types = types;
    this.condition = condition;
    this.wl = wl;
    this.sl = sl;
  }
}
