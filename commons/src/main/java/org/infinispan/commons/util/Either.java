package org.infinispan.commons.util;

import java.util.NoSuchElementException;

public abstract class Either<A, B> {

   public abstract Type type();
   public abstract A left();
   public abstract B right();

   public static <A, B> Either<A, B> newLeft(A a) { return new Left<>(a); }
   public static <A, B> Either<A, B> newRight(B b) { return new Right<>(b); }

   public static enum Type {
      LEFT, RIGHT
   }

   private static class Left<A, B> extends Either<A, B> {
      private A leftValue;
      Left(A a) { leftValue = a; }
      @Override public Type type() { return Type.LEFT; }
      @Override public A left() { return leftValue; }
      @Override public B right() { throw new NoSuchElementException("Either.right() called on Left"); }
   }

   private static class Right<A, B> extends Either<A, B> {
      private B rightValue;
      Right(B b) { rightValue = b; }
      @Override public Type type() { return Type.RIGHT; }
      @Override public A left() { throw new NoSuchElementException("Either.left() called on Right"); }
      @Override public B right() {  return rightValue; }
   }

}
