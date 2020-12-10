package tg.dtg.main.examples;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import tg.dtg.common.values.Value;
import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.graph.construct.dynamic.parallel.ParallelStaticDynamicEqConstructor;
import tg.dtg.graph.construct.dynamic.sequential.SeqDynamicEqConstructor;
import tg.dtg.query.Operator;
import tg.dtg.query.Predicate;
import tg.dtg.query.Query;

public class Kite extends Example {

  private final Predicate kitePredicate;
  private final EventTemplate template;
  private final int parallism;

  public Kite(Config args) {
    super(args);
    template = new EventTemplate.Builder()
        .addStr("src")
        .addStr("dest")
        .build();
    kitePredicate = new Predicate(Operator.eq, template.indexOf("src"), template.indexOf("dest"));
    this.parallism = args.parallism;
  }

  @Override
  protected String parameters() {
    return null;
  }

  @Override
  public String getName() {
    return "kite";
  }

  @Override
  public Query getQuery() {
    return new Query("C", kitePredicate, wl, sl);
  }

  @Nonnull
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
    ArrayList<Constructor> constructors = new ArrayList<>();
    if (parallism > 0) {
      constructors.add(new ParallelStaticDynamicEqConstructor(kitePredicate));
//      List<Value> attrs = Lists.newArrayList();
//      File file = new File(path);
//      File attrFile = new File(file.getParentFile(),"attr");
//      try(BufferedReader br = new BufferedReader(new FileReader(attrFile))){
//        attrs = br.lines().map(Value::str).collect(Collectors.toList());
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//      constructors.add(new StaticConstructor(kitePredicate,attrs));
    } else {
      constructors.add(new SeqDynamicEqConstructor(kitePredicate));
    }
    return constructors;
  }
}
