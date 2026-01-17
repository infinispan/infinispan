package org.infinispan.testing.lambda;

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

   private record NamedPredicate<T>(String description, Predicate<T> predicate) implements Predicate<T> {

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

   private record NamedBiConsumer<T, U>(String description, BiConsumer<T, U> biConsumer) implements BiConsumer<T, U> {

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

   private record NamedRunnable(String description, Runnable runnable) implements Runnable {

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
