package tg.dtg.graph.construct.dynamic;

import java.util.Comparator;
import tg.dtg.common.values.NumericValue;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Predicate;
import tg.dtg.util.Global;

public abstract class DynamicConstructor extends Constructor {

  protected final NumericValue start;
  protected final NumericValue end;
  protected final NumericValue step;
  protected final Comparator<NumericValue> cmp;

  protected DynamicConstructor(Predicate predicate, NumericValue start,
      NumericValue end, NumericValue step) {
    super(predicate);
    this.start = start;
    this.end = end;
    this.step = step;
    this.cmp = Global.numericValueComparator();
  }
}
