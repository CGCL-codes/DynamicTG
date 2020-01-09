package tg.dtg.main.examples;

import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.Iterator;
import javax.annotation.Nonnull;
import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.graph.construct.dynamic.sequential.SeqDynamicConstructor;
import tg.dtg.graph.construct.dynamic.parallel.ParallelStaticDynamicConstructor;
import tg.dtg.graph.construct.dynamic.sequential.SeqStaticDynamicConstructor;
import tg.dtg.query.LogicalExpression;
import tg.dtg.query.LogicalExpression.LogicalOperater;
import tg.dtg.query.Operator;
import tg.dtg.query.Predicate;
import tg.dtg.query.Query;

public class Stock extends Example {

  private final EventTemplate template;
  private final Predicate idPredicate;
  private final Predicate pricePredicate;
  private final boolean isSimple;
  private final NumericValue start;
  private final NumericValue end;
  private final NumericValue step;

  public Stock(Argument args) {
    super(args);
    template = new EventTemplate.Builder()
        .addStr("id")
        .addNumeric("price")
        .build();
    idPredicate = new Predicate(Operator.eq, template.indexOf("id"), -1);
    pricePredicate = new Predicate(Operator.gt, template.indexOf("price"),
        template.indexOf("price"));
    isSimple = args.isSimple;

    String range = args.range.trim();
    range = range.substring(1, range.length() - 1).trim();
    String[] sps = range.split(",");
    start = (NumericValue) Value.numeric(Double.parseDouble(sps[0]));
    end = (NumericValue) Value.numeric(Double.parseDouble(sps[1]));
    step = (NumericValue) Value.numeric(Double.parseDouble(sps[2]));
    setPrecision(step.numericVal());
  }

  static Config getArgument() {
    return new Argument();
  }

  @Override
  public String getName() {
    return isSimple ? "simple stock" : "stock";
  }

  private Query simpleStock() {
    return new Query("S", pricePredicate, wl, sl);
  }

  private Query stock() {
    LogicalExpression expression = new LogicalExpression(idPredicate, pricePredicate,
        LogicalOperater.and);
    return new Query("S", expression, wl, sl);
  }

  @Override
  public Query getQuery() {
    return isSimple ? simpleStock() : stock();
  }

  @Override
  @Nonnull
  public Iterator<Event> readInput() {
    return readInputFromFile(path);
  }

  @Override
  public EventTemplate getTemplate() {
    return template;
  }

  @Override
  protected String parameters() {
    return "simple: " + isSimple + "\n"
        + "range: " + "[" + start + ", " + end + ", " + step + ")";
  }

  @Override
  public ArrayList<Constructor> getConstructors() {
    Constructor pc;
    if (parallism > 0) {
      pc = new ParallelStaticDynamicConstructor(parallism, pricePredicate, start, end,
          step);
    } else {
      if(isStatic) pc = new SeqStaticDynamicConstructor(pricePredicate, start, end, step);
      else pc = new SeqDynamicConstructor(pricePredicate,start,end,step);
    }
    ArrayList<Constructor> constructors = new ArrayList<>(2);
    if (!isSimple) {
      constructors.add(null);
    } else {
      constructors.add(pc);
    }
    return constructors;
  }

  static class Argument extends Config {

    @Parameter(names = "-simple", description = "simple query or not")
    boolean isSimple;

    @Parameter(names = {"-r",
        "--range"}, description = "range for numeric, in the form of [start,end,step)")
    String range = "[0,50,1)";
  }
}
