package tg.dtg.util;

import java.util.Optional;

public class Either<L,R> {
  private final L left;
  private final R right;

  private Either(L left, R right) {
    this.left = left;
    this.right = right;
  }

  public static <L,R> Either<L,R> left(L left) {
    return new Either<L,R>(left,null);
  }

  public static <L,R> Either<L,R> right(R right) {
    return new Either<L,R>(null,right);
  }

  public Optional<L> getLeft() {
    return Optional.ofNullable(left);
  }

  public Optional<R> getRight() {
    return Optional.ofNullable(right);
  }

  public boolean isLeft() {
    return left != null;
  }

  public boolean isRight() {
    return right != null;
  }
}
