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

import javax.annotation.Nonnull;
import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Query;

public abstract class Example {
  protected final String path;
  protected final long wl;
  protected final long sl;
  protected final int parallism;

  public Example(Argument args) {
    this.path = args.path;
    this.wl = args.wl;
    this.sl = args.sl;
    this.parallism = args.parallism;
  }

  public abstract String getName();

  public abstract Query getQuery();

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

  public static Example getExample(String[] args) {
    Preconditions.checkArgument(args.length > 1, "must specify example name");
    Argument argument;
    String[] nargs = Arrays.copyOfRange(args,1,args.length);
    if ("stock".equals(args[0])) {
      argument = Stock.getArgument();
      JCommander.newBuilder()
          .addObject(argument)
          .build()
          .parse(nargs);
      return new Stock((Stock.Argument) argument);
    }else {
      argument = new Argument();
      return new EmptyExample(argument);
    }
  }

  private static class EmptyExample extends Example {

    public EmptyExample(Argument args) {
      super(args);
    }

    @Override
    public String getName() {
      return null;
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
