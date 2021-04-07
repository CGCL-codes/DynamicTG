package tg.dtg.events;

import tg.dtg.common.values.Value.ValType;

import java.io.Serializable;

public class Attribute implements Serializable {
  public final String name;
  public final ValType valType;

  public Attribute(String name, ValType valType) {
    this.name = name;
    this.valType = valType;
  }
}
