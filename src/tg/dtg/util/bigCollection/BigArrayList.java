package tg.dtg.util.bigCollection;

import java.util.ArrayList;

public class BigArrayList<T> {

  private static final int MAX_ARRAY_SIZE = 2 << 18;
  private static final int MASK = Integer.MAX_VALUE ^ (MAX_ARRAY_SIZE - 1);
  private ArrayList<Object[]> elements;
  private int index = 0;
  private int offset = 0;
  private int size = 0;

  public BigArrayList() {
    this.elements = new ArrayList<>();
    elements.add(new Object[MAX_ARRAY_SIZE]);
  }

  public void add(T element) {
    elements.get(index)[offset] = element;
    offset++;
    if (offset == MAX_ARRAY_SIZE) {
      offset = 0;
      elements.add(new Object[MAX_ARRAY_SIZE]);
      index ++;
    }
    size += 1;
  }

  public void get(int index) {
    if(index >= size) throw new IndexOutOfBoundsException(index+"");
    int a = index & (MAX_ARRAY_SIZE - 1);
  }
}
