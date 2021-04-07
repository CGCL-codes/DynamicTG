package tg.dtg.query;

import java.io.Serializable;
import java.util.Map;

public interface Expression extends Serializable {
  Map<Character,Predicate> predicates();

  default boolean isLogical() {
    return false;
  }

  default boolean isPredicate() {
    return false;
  }
}
