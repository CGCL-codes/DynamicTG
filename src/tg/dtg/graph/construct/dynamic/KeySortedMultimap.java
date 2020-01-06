package tg.dtg.graph.construct.dynamic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;

public class KeySortedMultimap<K, V> {

  private final TreeMap<K, ArrayList<V>> map;

  public KeySortedMultimap(Comparator<K> comparator) {
    this.map = new TreeMap<>(comparator);
  }

  public boolean put(K key, V value) {
    ArrayList<V> list;
    if (map.containsKey(key)) {
      list = map.get(key);
    } else {
      list = new ArrayList<>();
      map.put(key, list);
    }
    return list.add(value);
  }

  public ArrayList<V> getMultiple(K key) {
    return map.get(key);
  }

  public void putMultiple(K key, ArrayList<V> values) {
    if (map.containsKey(key)) {
      map.get(key).addAll(values);
    } else {
      map.put(key, values);
    }
  }

  public Iterator<V> valueIterator() {
    return new SortedIterator<>(map.values().iterator());
  }


  private static class SortedIterator<V> implements Iterator<V> {

    private final Iterator<ArrayList<V>> iterator;
    private Iterator<V> subit;

    private SortedIterator(Iterator<ArrayList<V>> iterator) {
      this.iterator = iterator;

      if (iterator.hasNext()) {
        subit = iterator.next().iterator();
      } else {
        subit = Collections.emptyIterator();
      }
    }

    @Override
    public boolean hasNext() {
      return subit.hasNext() || iterator.hasNext();
    }

    @Override
    public V next() {
      if (subit.hasNext()) {
        return subit.next();
      } else {
        subit = iterator.next().iterator();
        return subit.next();
      }
    }
  }
}
