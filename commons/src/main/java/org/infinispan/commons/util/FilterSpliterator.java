package org.infinispan.commons.util;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Spliterator that only returns entries that pass the given predicate. This spliterator will inherit all of the
 * characteristics of the underlying spliterator, except that it won't return {@link Spliterator#SIZED} or
 * {@link Spliterator#SUBSIZED}.
 * <p>
 * The {@link #forEachRemaining(Consumer)} method should provide better performance than calling
 * {@link #tryAdvance(Consumer)} until it returns false. This is due to having to capture the argument before testing
 * it and finally invoking the provided {@link Consumer}.
 * @author wburns
 * @since 9.3
 */
public class FilterSpliterator<T> implements CloseableSpliterator<T> {
   private final Spliterator<T> spliterator;
   private final Predicate<? super T> predicate;

   // We assume that spliterator is not used concurrently - normally it is split so we can use these variables safely
   private final Consumer<? super T> consumer = t -> current = t;

   private T current;

   public FilterSpliterator(Spliterator<T> spliterator, Predicate<? super T> predicate) {
      this.spliterator = spliterator;
      this.predicate = predicate;
   }

   @Override
   public void close() {
      if (spliterator instanceof CloseableSpliterator) {
         ((CloseableSpliterator) spliterator).close();
      }
   }

   @Override
   public boolean tryAdvance(Consumer<? super T> action) {
      while (spliterator.tryAdvance(consumer)) {
         T objectToUse = current;
         // If object passes then accept it and return
         if (predicate.test(objectToUse)) {
            action.accept(objectToUse);
            return true;
         }
      }

      return false;
   }

   @Override
   public void forEachRemaining(Consumer<? super T> action) {
      spliterator.forEachRemaining(e -> {
         if (predicate.test(e)) {
            action.accept(e);
         }
      });
   }

   @Override
   public Spliterator<T> trySplit() {
      Spliterator<T> split = spliterator.trySplit();
      if (split != null) {
         return new FilterSpliterator<>(split, predicate);
      }
      return null;
   }

   @Override
   public long estimateSize() {
      return spliterator.estimateSize();
   }

   @Override
   public int characteristics() {
      // Unset the SIZED and SUBSIZED as we don't have an exact amount
      return spliterator.characteristics() & ~(Spliterator.SIZED | Spliterator.SUBSIZED);
   }
}
