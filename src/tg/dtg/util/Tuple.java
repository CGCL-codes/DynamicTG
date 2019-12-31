package tg.dtg.util;

public class Tuple<L,R> {
  public final L _1;
  public final R _2;

  private Tuple(L _1, R _2) {
    this._1 = _1;
    this._2 = _2;
  }

  public static <L,R> Tuple<L,R> of(L _1, R _2) {
    return new Tuple<>(_1,_2);
  }
}
