package tg.dtg.query;

import java.util.Map;

public interface Expression {
  Map<Character,Predicate> predicates();

  default boolean isLogical() {
    return false;
  }

  default boolean isPredicate() {
    return false;
  }
}
