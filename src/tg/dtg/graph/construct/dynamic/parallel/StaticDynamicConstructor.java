package tg.dtg.graph.construct.dynamic.parallel;

import tg.dtg.common.values.NumericValue;
import tg.dtg.graph.AttributeVertex;
import tg.dtg.graph.EventVertex;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Predicate;
import tg.dtg.util.Parallel;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

public class StaticDynamicConstructor extends Constructor {
  private final ExecutorService executor;
  private final EventProcessor[] processors;
  private final BlockingQueue<EventVertex> queue;

  public StaticDynamicConstructor(int parallism, Predicate predicate,
                                  NumericValue start, NumericValue end,
                                  NumericValue step) {
    super(predicate);
    executor = Parallel.getInstance().getExecutor();
    queue = new LinkedBlockingQueue<>();
    processors = new EventProcessor[parallism];
    for (int i = 0; i < processors.length; i++) {
      processors[i] = new EventProcessor(queue, predicate);
    }
    for (EventProcessor processor : processors) {
      executor.execute(processor);
    }
  }

  @Override
  public void link(EventVertex eventVertex) {
    queue.offer(eventVertex);
  }

  @Override
  public void manage() {

  }

  @Override
  public Iterator<AttributeVertex> attributes() {
    return null;
  }
}
