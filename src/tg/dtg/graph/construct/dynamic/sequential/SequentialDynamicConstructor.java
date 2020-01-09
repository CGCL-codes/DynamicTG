package tg.dtg.graph.construct.dynamic.sequential;

import java.util.ArrayList;
import java.util.Iterator;
import tg.dtg.common.values.NumericValue;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.dynamic.DynamicConstructor;
import tg.dtg.query.Predicate;

public abstract class SequentialDynamicConstructor extends DynamicConstructor {

  protected SequentialDynamicConstructor(Predicate predicate,
      NumericValue start, NumericValue end,
      NumericValue step) {
    super(predicate, start, end, step);
  }

  @Override
  public void parallelLink(ArrayList<Iterator<EventVertex>> iterators) {
    throw new UnsupportedOperationException("sequential constructor do not use this api");
  }
}
