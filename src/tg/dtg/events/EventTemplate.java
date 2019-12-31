package tg.dtg.events;

import java.util.ArrayList;
import tg.dtg.common.values.Value;
import tg.dtg.common.values.Value.ValType;

public class EventTemplate {

  private Attribute[] attributes;

  public EventTemplate(Attribute[] attributes) {
    this.attributes = attributes;
  }

  public int indexOf(String name) {
    for (int i = 0; i < attributes.length; i++) {
      if(name.equals(attributes[i].name)) return i;
    }
    return -1;
  }

  public Event str2event(String line) {
    String[] sps = line.split(" ");
    long timestamp = Long.parseLong(sps[0]);
    Value[] values = new Value[attributes.length];
    for (int i = 0; i < attributes.length; i++) {
      if (attributes[i].valType == ValType.str) {
        values[i] = Value.str(sps[i + 1]);
      } else {
        values[i] = Value.numeric(Double.parseDouble(sps[i + 1]));
      }
    }
    return new Event(timestamp,values);
  }

  public static class Builder {

    private ArrayList<Attribute> attributes;

    public Builder() {
      attributes=new ArrayList<>();
    }

    public Builder addStr(String name) {
      attributes.add(new Attribute(name, ValType.str));
      return this;
    }

    public Builder addNumeric(String name) {
      attributes.add(new Attribute(name, ValType.numeric));
      return this;
    }

    public EventTemplate build() {
      return new EventTemplate(attributes.toArray(new Attribute[]{}));
    }
  }
}
