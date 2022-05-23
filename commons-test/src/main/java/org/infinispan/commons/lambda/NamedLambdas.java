package org.infinispan.commons.lambda;

import java.util.function.BiConsumer;
import java.util.function.Predicate;

public class NamedLambdas {

   public static <T, U> BiConsumer<T, U> of(String description, BiConsumer<T, U> consumer) {
      return new NamedBiConsumer<>(description, consumer);
   }

   public static <T> Predicate<T> of(String description, Predicate<T> predicate) {
      return new NamedPredicate(description, predicate);
   }

   public static Runnable of(String description, Runnable runnable) {
      return new NamedRunnable(description, runnable);
   }

   private static class NamedPredicate<T> implements Predicate<T> {

      private String description;
      private Predicate<T> predicate;

      public NamedPredicate(String description, Predicate<T> predicate) {
         this.description = description;
         this.predicate = predicate;
      }

      @Override
      public boolean test(T t) {
         return predicate.test(t);
      }

      @Override
      public Predicate<T> and(Predicate<? super T> other) {
         return predicate.and(other);
      }

      @Override
      public Predicate<T> negate() {
         return predicate.negate();
      }

      @Override
      public Predicate<T> or(Predicate<? super T> other) {
         return predicate.or(other);
      }

      @Override
      public String toString() {
         return this.description;
      }
   }

   private static class NamedBiConsumer<T, U> implements BiConsumer<T, U> {

      private String description;
      private BiConsumer<T, U> biConsumer;

      public NamedBiConsumer(String description, BiConsumer<T, U> biConsumer) {
         this.description = description;
         this.biConsumer = biConsumer;
      }

      @Override
      public void accept(T t, U u) {
         this.biConsumer.accept(t, u);
      }

      @Override
      public BiConsumer<T, U> andThen(BiConsumer<? super T, ? super U> after) {
         return this.biConsumer.andThen(after);
      }

      @Override
      public String toString() {
         return this.description;
      }
   }

   private static class NamedRunnable implements Runnable {

      private final String description;
      private final Runnable runnable;

      private NamedRunnable(String description, Runnable runnable) {
         this.description = description;
         this.runnable = runnable;
      }

      @Override
      public void run() {
         runnable.run();
      }

      @Override
      public String toString() {
         return this.description;
      }
   }
}
