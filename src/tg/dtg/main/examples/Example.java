package tg.dtg.main.examples;

import com.google.common.collect.Iterators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import tg.dtg.events.Event;
import tg.dtg.events.EventTemplate;
import tg.dtg.graph.construct.Constructor;
import tg.dtg.query.Query;

public abstract class Example {

  public Example(String[] args) {
  }

  public abstract String getName();

  public abstract Query getQuery();

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
}
