package tg.dtg.main.examples;

import com.beust.jcommander.JCommander;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.Graph;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Query;
import tg.dtg.util.Global;

public abstract class Example {

  protected final String path;
  protected final long wl;
  protected final long sl;
  protected final int parallism;
  protected final int numWindow;

  public Example(Config args) {
    this.path = args.path;
    this.wl = args.wl;
    this.sl = args.sl;
    this.parallism = args.parallism;
    if (parallism > 0) {
      Global.initParallel(parallism);
    }
    numWindow = 1;
  }

  public static Example getExample(String[] args) {
    Preconditions.checkArgument(args.length > 0, "must specify example name");
    Config config;
    String[] nargs = Arrays.copyOfRange(args, 1, args.length);
    if ("stock".equals(args[0])) {
      config = Stock.getArgument();
      JCommander.newBuilder()
          .addObject(config)
          .build()
          .parse(nargs);
      return new Stock((Stock.Argument) config);
    } else {
      config = new Config();
      return new EmptyExample(config);
    }
  }

  public void start() {
    String parameters = "************************************\n"
        + "name: " + getName() + "\n"
        + "input events: " + path + "\n"
        + "window: " + wl + ", " + sl + "\n"
        + "parallism: " + parallism + "\n"
        + parameters() + "\n"
        + "************************************";
    System.out.println(parameters);

    ArrayList<Window> windowedEvents = windowEvents();
    Graph graph = new Graph(windowedEvents.get(0).events, getTemplate(),
        getQuery(), getConstructors());
    graph.construct();

    if (parallism > 0) {
      Global.close(100L * (wl + sl * (numWindow - 1)), TimeUnit.MILLISECONDS);
    }
  }

  private ArrayList<Window> windowEvents() {
    ArrayList<Window> windowedEvents = new ArrayList<>();
    Iterator<Event> it = readInput();
    Query query = getQuery();
    long nextW = 0;
    while (it.hasNext()) {
      Event event = it.next();
      while (event.timestamp >= nextW) {
        Window window = new Window(nextW);
        windowedEvents.add(window);
        nextW = nextW + query.sl;
      }
      for (Window window : windowedEvents) {
        if (window.start <= event.timestamp && event.timestamp < window.start + query.wl) {
          window.events.add(event);
        }
      }
    }
    return windowedEvents;
  }

  protected abstract String parameters();

  public abstract String getName();

  public abstract Query getQuery();

  protected void setPrecision(double... precisions) {
    Preconditions.checkArgument(precisions.length > 0);
    Global.initValue(Arrays.stream(precisions).min().getAsDouble());
  }

  @Nonnull
  public abstract Iterator<Event> readInput();

  public abstract EventTemplate getTemplate();

  public abstract ArrayList<Constructor> getConstructors();

  protected Iterator<Event> readInputFromFile(String path) {
    Iterator<String> it = new FileIterator(path);
    EventTemplate template = getTemplate();
    return Iterators.transform(it, template::str2event);
  }

  protected static class FileIterator implements Iterator<String> {

    private final BufferedReader br;
    private String nextLine = null;

    public FileIterator(String path) {
      BufferedReader br = null;
      try {
        br = new BufferedReader(new FileReader(path));
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      this.br = br;
    }

    @Override
    public boolean hasNext() {
      if (nextLine != null) {
        return true;
      } else {
        try {
          nextLine = br.readLine();
          return (nextLine != null);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    @Override
    public String next() {
      if (nextLine != null || hasNext()) {
        String line = nextLine;
        nextLine = null;
        return line;
      } else {
        throw new NoSuchElementException();
      }
    }
  }

  private static class EmptyExample extends Example {

    public EmptyExample(Config args) {
      super(args);
    }

    @Override
    protected String parameters() {
      return "";
    }

    @Override
    public String getName() {
      return "empty";
    }

    @Override
    public Query getQuery() {
      return null;
    }

    @Nonnull
    @Override
    public Iterator<Event> readInput() {
      return Collections.emptyIterator();
    }

    @Override
    public EventTemplate getTemplate() {
      return null;
    }

    @Override
    public ArrayList<Constructor> getConstructors() {
      return null;
    }
  }
}
