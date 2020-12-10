package tg.dtg.graph.construct.dynamic.parallel;

import tg.dtg.common.values.NumericValue;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.dynamic.DynamicConstructor;
import tg.dtg.query.Predicate;

public abstract class ParallelDynamicConstructor extends DynamicConstructor {

  protected ParallelDynamicConstructor(Predicate predicate, NumericValue start,
      NumericValue end, NumericValue step) {
    super(predicate, start, end, step);
  }

  @Override
  public void link(EventVertex eventVertex) {
    throw new UnsupportedOperationException("parallel constructor do not use this api");
  }
}
