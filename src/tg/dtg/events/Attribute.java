package tg.dtg.events;

import tg.dtg.common.values.Value.ValType;

public class Attribute {
  public final String name;
  public final ValType valType;

  public Attribute(String name, ValType valType) {
    this.name = name;
    this.valType = valType;
  }
}
