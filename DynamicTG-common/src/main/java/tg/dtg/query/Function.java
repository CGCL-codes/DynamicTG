package tg.dtg.query;

import java.io.Serializable;

/**
 * Created by meihuiyao on 2020/12/19
 */

@FunctionalInterface
public interface Function<T,R> extends Serializable {
  R apply(T t);
}
