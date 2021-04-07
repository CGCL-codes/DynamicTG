package tg.dtg.events;

import java.io.Serializable;
import java.util.ArrayList;

import tg.dtg.common.values.Value;
import tg.dtg.common.values.Value.ValType;

public class EventTemplate implements Serializable {

  private Attribute[] attributes;

  public EventTemplate(Attribute[] attributes) {
    this.attributes = attributes;
  }

  /**
   * the index of attribute in the template,
   * or -1 which means the attribute name is not in this template.
   *
   * @param name the name of attribute
   * @return the index of the given name
   */
  public int indexOf(String name) {
    for (int i = 0; i < attributes.length; i++) {
      if (name.equals(attributes[i].name)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * create event from the string.
   * @param line the string of event
   * @return event
   */
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
    return new Event(timestamp, values);
  }

  public static class Builder {

    private ArrayList<Attribute> attributes;

    public Builder() {
      attributes = new ArrayList<>();
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
