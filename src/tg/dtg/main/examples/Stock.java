package tg.dtg.main.examples;

import java.util.ArrayList;
import java.util.Iterator;

import tg.dtg.common.values.NumericValue;
import tg.dtg.common.values.Value;
import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.graph.construct.dynamic.DynamicConstructor;
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
  private final long wl;
  private final long sl;
  private final String path;
  private final boolean isInteger;

  public Stock(String[] args) {
    super(args);
    template = new EventTemplate.Builder()
        .addStr("id")
        .addNumeric("price")
        .build();
    idPredicate = new Predicate(Operator.eq, template.indexOf("id"), -1);
    pricePredicate = new Predicate(Operator.gt, template.indexOf("price"),
        template.indexOf("price"));
    path = args[0];
    isSimple = args[1].equals("simple");
    wl = Long.parseLong(args[2]);
    sl = Long.parseLong(args[3]);
    if (args.length > 4) {
      isInteger = args[4].equals("int");
    } else {
      isInteger = false;
    }
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
  public Iterator<Event> readInput() {
    return readInputFromFile(path);
  }

  @Override
  public EventTemplate getTemplate() {
    return template;
  }

  @Override
  public ArrayList<Constructor> getConstructors() {
    NumericValue start;
    NumericValue end;
    NumericValue step;
    if (isInteger) {
      start = (NumericValue) Value.numeric(0);
      end = (NumericValue) Value.numeric(50);
      step = (NumericValue) Value.numeric(1);
    } else {
      start = (NumericValue) Value.numeric(0);
      end = (NumericValue) Value.numeric(50);
      step = (NumericValue) Value.numeric(0.01);
    }
    DynamicConstructor pc = new DynamicConstructor(start, end, step);
    ArrayList<Constructor> constructors = new ArrayList<>(2);
    if (!isSimple) {
      constructors.add(null);
    } else {
      constructors.add(pc);
    }
    return constructors;
  }
}
